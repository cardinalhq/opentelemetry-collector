package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	"go.opentelemetry.io/collector/pipeline"
)

func createTestHeadDroppingQueueWithClient(client storage.Client, capacity int64) *headDroppingQueue[tracesRequest] {
	pq := NewHeadDroppingQueue[tracesRequest](PersistentQueueSettings[tracesRequest]{
		Sizer:            &RequestSizer[tracesRequest]{},
		Capacity:         capacity,
		Signal:           pipeline.SignalTraces,
		StorageID:        component.ID{},
		Marshaler:        marshalTracesRequest,
		Unmarshaler:      unmarshalTracesRequest,
		ExporterSettings: exportertest.NewNopSettings(),
	}).(*headDroppingQueue[tracesRequest])
	pq.initClient(context.Background(), client)
	return pq
}

// test if basic operations work as expected
func TestBasicOperations(t *testing.T) {
	// Create a queue with capacity 1.
	pq := createTestHeadDroppingQueueWithClient(newFakeBoundedStorageClient(5000), 3)
	defer func() {
		require.NoError(t, pq.Shutdown(context.Background()))
	}()

	// Add 3 elements to the queue, and consume immediately.
	for i := 3; i > 0; i-- {
		require.NoError(t, pq.Offer(context.Background(), newTracesRequest(1, 1)))
		require.True(t, pq.Consume(func(context.Context, tracesRequest) error { return nil }))
	}
	require.Equal(t, uint64(3), pq.readIndex)
	require.Equal(t, uint64(3), pq.writeIndex)
	require.Equal(t, 0, pq.Size())
}

// test if head dropping queue drops head element when the queue is full.
func TestHeadDroppingQueue_Eviction(t *testing.T) {
	// Create a queue with capacity 1.
	pq := createTestHeadDroppingQueueWithClient(newFakeBoundedStorageClient(5000), 3)
	defer func() {
		require.NoError(t, pq.Shutdown(context.Background()))
	}()

	// Add 4 elements to the queue.
	require.NoError(t, pq.Offer(context.Background(), newTracesRequest(1, 1)))
	require.NoError(t, pq.Offer(context.Background(), newTracesRequest(1, 1)))
	require.NoError(t, pq.Offer(context.Background(), newTracesRequest(1, 1)))
	require.NoError(t, pq.Offer(context.Background(), newTracesRequest(1, 1)))

	// The first element should be dropped.
	require.Equal(t, 3, pq.Size())
	for i := 3; i > 0; i-- {
		require.True(t, pq.Consume(func(context.Context, tracesRequest) error { return nil }))
	}
}
