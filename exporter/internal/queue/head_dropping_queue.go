// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/internal/queue"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/experimental/storage"
)

// headDroppingQueue provides a persistent queue implementation backed by file storage extension, and drops the oldest/unconsumed
// element when the queue is full.
type headDroppingQueue[T any] struct {
	set    PersistentQueueSettings[T]
	logger *zap.Logger
	client storage.Client

	mu         sync.Mutex
	cond       *sync.Cond
	readIndex  uint64
	writeIndex uint64
	refClient  int64
	stopped    bool
}

var _ Queue[any] = (*headDroppingQueue[any])(nil)

// NewHeadDroppingQueue creates a new queue backed by file storage.
func NewHeadDroppingQueue[T any](set PersistentQueueSettings[T]) Queue[T] {
	pq := &headDroppingQueue[T]{
		set:    set,
		logger: set.ExporterSettings.Logger,
	}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// Start starts the persistentQueue with the given number of consumers.
func (pq *headDroppingQueue[T]) Start(ctx context.Context, host component.Host) error {
	storageClient, err := toStorageClient(ctx, pq.set.StorageID, host, pq.set.ExporterSettings.ID, pq.set.Signal)
	if err != nil {
		return err
	}
	pq.initClient(ctx, storageClient)
	return nil
}

func (pq *headDroppingQueue[T]) initClient(ctx context.Context, client storage.Client) {
	pq.client = client
	pq.refClient = 1 // Initial reference count for producer goroutines and initialization
	pq.initPersistentContiguousStorage(ctx)
}

func (pq *headDroppingQueue[T]) initPersistentContiguousStorage(ctx context.Context) {
	riOp := storage.GetOperation(readIndexKey)
	wiOp := storage.GetOperation(writeIndexKey)

	err := pq.client.Batch(ctx, riOp, wiOp)
	if err == nil {
		pq.readIndex, err = bytesToItemIndexDefaultTo0(riOp.Value)
	}

	if err == nil {
		pq.writeIndex, err = bytesToItemIndexDefaultTo0(wiOp.Value)
	}

	if err != nil {
		if errors.Is(err, errValueNotSet) {
			pq.logger.Info("Initializing new persistent queue")
		} else {
			pq.logger.Error("Failed getting read/write index, starting with new ones", zap.Error(err))
		}
		pq.readIndex = 0
		pq.writeIndex = 0
	}
}

// Consume applies the provided function on the head of queue.
// The call blocks until there is an item available or the queue is stopped.
// The function returns true when an item is consumed or false if the queue is stopped.
func (pq *headDroppingQueue[T]) Read(ctx context.Context) (uint64, context.Context, T, bool) {
	for {
		var (
			req      T
			consumed bool
		)

		pq.mu.Lock()
		for pq.readIndex == pq.writeIndex && !pq.stopped {
			pq.cond.Wait()
		}

		if pq.stopped {
			pq.mu.Unlock()
			return 0, nil, req, false
		}

		req, index, consumed := pq.getNextItem(context.Background())
		pq.mu.Unlock()

		if consumed {
			return index, ctx, req, true
		}
	}
}

func (pq *headDroppingQueue[T]) Shutdown(ctx context.Context) error {
	// If the queue is not initialized, there is nothing to shut down.
	if pq.client == nil {
		return nil
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.stopped = true
	pq.cond.Broadcast()
	return pq.unrefClient(ctx)
}

// unrefClient unrefs the client, and closes if no more references. Callers MUST hold the mutex.
// This is needed because consumers of the queue may still process the requests while the queue is shutting down or immediately after.
func (pq *headDroppingQueue[T]) unrefClient(ctx context.Context) error {
	pq.refClient--
	if pq.refClient == 0 {
		return pq.client.Close(ctx)
	}
	return nil
}

// Offer inserts the specified element into this queue if it is possible to do so immediately
// without violating capacity restrictions. If the queue is full, it drops the oldest element and adds the new element.
func (pq *headDroppingQueue[T]) Offer(ctx context.Context, req T) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	queueSize, err := pq.getQueueSize(ctx)
	if err != nil {
		return err
	}

	if queueSize >= pq.set.Capacity {
		// Drop the oldest element
		oldestKey := getItemKey(pq.readIndex)
		newReadIndex := pq.readIndex + 1
		ops := []storage.Operation{
			storage.SetOperation(readIndexKey, itemIndexToBytes(newReadIndex)),
			storage.DeleteOperation(oldestKey),
		}
		if err := pq.client.Batch(ctx, ops...); err != nil {
			return fmt.Errorf("failed to delete oldest item from queue: %w", err)
		}
		pq.readIndex = newReadIndex
	}

	err = pq.putInternal(ctx, req)
	if err != nil {
		return err
	}
	pq.cond.Signal()
	return nil
}

func (pq *headDroppingQueue[T]) getQueueSize(ctx context.Context) (int64, error) {
	riOp := storage.GetOperation(readIndexKey)
	wiOp := storage.GetOperation(writeIndexKey)

	err := pq.client.Batch(ctx, riOp, wiOp)
	if err != nil {
		return 0, err
	}

	readIndex, err := bytesToItemIndexDefaultTo0(riOp.Value)
	if err != nil {
		return 0, err
	}

	writeIndex, err := bytesToItemIndexDefaultTo0(wiOp.Value)
	if err != nil {
		return 0, err
	}

	return int64(writeIndex - readIndex), nil
}

func bytesToItemIndexDefaultTo0(buf []byte) (uint64, error) {
	if buf == nil {
		return uint64(0), nil
	}
	// The sizeof uint64 in binary is 8.
	if len(buf) < 8 {
		return 0, errInvalidValue
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// putInternal is the internal version that requires caller to hold the mutex lock.
func (pq *headDroppingQueue[T]) putInternal(ctx context.Context, req T) error {
	itemKey := getItemKey(pq.writeIndex)
	newIndex := pq.writeIndex + 1

	reqBuf, err := pq.set.Marshaler(req)
	if err != nil {
		return err
	}

	// Carry out a transaction where we both add the item and update the write index
	ops := []storage.Operation{
		storage.SetOperation(writeIndexKey, itemIndexToBytes(newIndex)),
		storage.SetOperation(itemKey, reqBuf),
	}
	if storageErr := pq.client.Batch(ctx, ops...); storageErr != nil {
		return storageErr
	}

	pq.writeIndex = newIndex
	return nil
}

// getNextItem pulls the next available item from the persistent storage along with a callback function that should be
// called after the item is processed to clean up the storage. If no new item is available, returns false.
func (pq *headDroppingQueue[T]) getNextItem(ctx context.Context) (T, uint64, bool) {
	var request T

	if pq.stopped {
		return request, 0, false
	}

	if pq.readIndex == pq.writeIndex {
		return request, 0, false
	}

	index := pq.readIndex
	// Increase here, so even if errors happen below, it always iterates
	pq.readIndex++
	getOp := storage.GetOperation(getItemKey(index))
	err := pq.client.Batch(ctx,
		storage.SetOperation(readIndexKey, itemIndexToBytes(pq.readIndex)),
		getOp)

	if err == nil {
		request, err = pq.set.Unmarshaler(getOp.Value)
	}

	if err != nil {
		pq.logger.Debug("Failed to dispatch item", zap.Error(err))
		return request, 0, false
	}

	pq.refClient++ // Increase the reference count
	return request, index, true
}

func (pq *headDroppingQueue[T]) Size() int {
	ctx := context.Background()
	queueSize, err := pq.getQueueSize(ctx)
	if err != nil {
		pq.logger.Error("Error getting queue size", zap.Error(err))
		return 0
	}
	return int(queueSize)
}

func (pq *headDroppingQueue[T]) Capacity() int {
	return int(pq.set.Capacity)
}

func (pq *headDroppingQueue[T]) OnProcessingFinished(index uint64, consumeErr error) {
	pq.mu.Lock()
	defer func() {
		if err := pq.unrefClient(context.Background()); err != nil {
			pq.logger.Error("Error closing the storage client", zap.Error(err))
		}
		pq.mu.Unlock()
	}()

	// Delete the item from the persistent storage after it was processed.
	if err := pq.client.Batch(context.Background(), storage.DeleteOperation(getItemKey(index))); err != nil {
		pq.logger.Error("Error deleting item from queue", zap.Error(err))
	}
}
