package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestRun9GCExactObjectsAcceptsEmptyBatch(t *testing.T) {
	out, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  t.TempDir(),
		},
		Threads: 1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Zero(t, out.DeletedObjects)
	require.Zero(t, out.DeletedBytes)
}

func TestRun9GCExactObjectsDeletesExactFileObject(t *testing.T) {
	bucket := t.TempDir()
	key := "chunks/0/0/1_0_3"
	objectPath := filepath.Join(bucket, "fmtroot", key)
	require.NoError(t, os.MkdirAll(filepath.Dir(objectPath), 0o755))
	require.NoError(t, os.WriteFile(objectPath, []byte("abc"), 0o644))

	out, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Objects: []run9GCExactObject{{Key: key, Size: 3}},
		Threads: 1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.DeletedObjects)
	require.Equal(t, uint64(3), out.DeletedBytes)
	require.NoFileExists(t, objectPath)
}

func TestRun9GCExactObjectsRejectsRemovedSecret(t *testing.T) {
	_, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage:   "s3",
			Bucket:    "s3://bucket",
			SecretKey: "removed",
		},
		Objects: []run9GCExactObject{{Key: "chunks/0/0/1_0_1", Size: 1}},
		Threads: 1,
	})

	require.ErrorContains(t, err, "removed secret")
}

func TestRun9GCExactObjectsRejectsEscapingKey(t *testing.T) {
	bucket := t.TempDir()
	outsidePath := filepath.Join(bucket, "other")
	require.NoError(t, os.WriteFile(outsidePath, []byte("keep"), 0o644))

	_, err := run9GCExactObjects(context.Background(), run9GCExactObjectsRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Objects: []run9GCExactObject{{Key: "chunks/../../other", Size: 4}},
		Threads: 1,
	})

	require.ErrorContains(t, err, "normalized relative path")
	require.FileExists(t, outsidePath)
}

type fakeRun9GCExactObjectsBulkDeleteStore struct {
	object.ObjectStorage
	mu          sync.Mutex
	bulkCalls   [][]string
	deleteCalls []string
}

func (f *fakeRun9GCExactObjectsBulkDeleteStore) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, key)
	return nil
}

func (f *fakeRun9GCExactObjectsBulkDeleteStore) DeleteObjects(ctx context.Context, keys []string, getters ...object.AttrGetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bulkCalls = append(f.bulkCalls, append([]string(nil), keys...))
	return nil
}

func TestDeleteRun9ExactObjectsUsesBulkDeleteWhenSupported(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	store := &fakeRun9GCExactObjectsBulkDeleteStore{ObjectStorage: base}
	wrapped := object.WithPrefix(store, "chunks/")
	objects := make([]run9GCExactObject, 1001)
	for i := range objects {
		objects[i] = run9GCExactObject{Key: fmt.Sprintf("0/0/%04d_0_1", i), Size: 1}
	}

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(context.Background(), wrapped, objects, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1001), deletedObjects)
	require.Equal(t, uint64(1001), deletedBytes)
	require.Empty(t, store.deleteCalls)
	require.Len(t, store.bulkCalls, 2)
	require.Len(t, store.bulkCalls[0], run9GCExactObjectsMaxBulkDeleteBatchSize)
	require.Len(t, store.bulkCalls[1], 1)
	require.Equal(t, "chunks/0/0/0000_0_1", store.bulkCalls[0][0])
}

type fakeRun9GCExactObjectsRetryBulkDeleteStore struct {
	object.ObjectStorage
	mu        sync.Mutex
	callCount int
	firstErr  error
}

func (f *fakeRun9GCExactObjectsRetryBulkDeleteStore) DeleteObjects(ctx context.Context, keys []string, getters ...object.AttrGetter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	if f.callCount == 1 {
		return f.firstErr
	}
	return nil
}

func TestDeleteRun9ExactObjectsRetriesBulkDeleteSlowDown(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	store := &fakeRun9GCExactObjectsRetryBulkDeleteStore{
		ObjectStorage: base,
		firstErr:      fmt.Errorf("api error SlowDown: Please reduce your request rate"),
	}

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(context.Background(), store, []run9GCExactObject{{Key: "chunks/0/0/1_0_1", Size: 1}}, 1)

	require.NoError(t, err)
	require.Equal(t, uint64(1), deletedObjects)
	require.Equal(t, uint64(1), deletedBytes)
	require.Equal(t, 2, store.callCount)
}

func TestDeleteRun9ExactObjectsRetriesBulkDeleteInternalError(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	store := &fakeRun9GCExactObjectsRetryBulkDeleteStore{
		ObjectStorage: base,
		firstErr:      fmt.Errorf("bulk delete exact objects failed for \"chunks/0/0/1_0_1\": InternalError: We encountered an internal error. Please try again."),
	}

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(context.Background(), store, []run9GCExactObject{{Key: "chunks/0/0/1_0_1", Size: 1}}, 1)

	require.NoError(t, err)
	require.Equal(t, uint64(1), deletedObjects)
	require.Equal(t, uint64(1), deletedBytes)
	require.Equal(t, 2, store.callCount)
}

type fakeRun9GCExactObjectsConcurrentBulkDeleteStore struct {
	object.ObjectStorage
	mu          sync.Mutex
	active      int
	maxActive   int
	overlapped  chan struct{}
	overlapOnce sync.Once
}

func (f *fakeRun9GCExactObjectsConcurrentBulkDeleteStore) DeleteObjects(ctx context.Context, keys []string, getters ...object.AttrGetter) error {
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	if f.maxActive == 2 {
		f.overlapOnce.Do(func() { close(f.overlapped) })
	}
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.overlapped:
	case <-time.After(500 * time.Millisecond):
		return fmt.Errorf("bulk delete batches did not run concurrently")
	}

	f.mu.Lock()
	f.active--
	f.mu.Unlock()
	return nil
}

func TestDeleteRun9ExactObjectsRunsBulkBatchesConcurrently(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	store := &fakeRun9GCExactObjectsConcurrentBulkDeleteStore{
		ObjectStorage: base,
		overlapped:    make(chan struct{}),
	}
	objects := make([]run9GCExactObject, 2000)
	for i := range objects {
		objects[i] = run9GCExactObject{Key: fmt.Sprintf("chunks/0/0/%04d_0_1", i), Size: 1}
	}

	deletedObjects, deletedBytes, err := deleteRun9ExactObjects(context.Background(), store, objects, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), deletedObjects)
	require.Equal(t, uint64(2000), deletedBytes)
	require.Equal(t, 2, store.maxActive)
}

func TestBulkDeleteBatchSizeUsesConfiguredThreadsForSmallBatches(t *testing.T) {
	require.Equal(t, 63, run9GCExactObjectsBulkDeleteBatchSize(1000, 16))
	require.Equal(t, 1000, run9GCExactObjectsBulkDeleteBatchSize(100_000, 16))
	require.Equal(t, 1000, run9GCExactObjectsBulkDeleteBatchSize(1000, 1))
}
