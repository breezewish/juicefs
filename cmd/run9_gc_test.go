package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestRun9DescribeFormatPersistsNextChunkOverride(t *testing.T) {
	metaDir := filepath.Join(t.TempDir(), "meta")
	store := meta.NewClient("badger://"+metaDir, meta.DefaultConf())
	require.NoError(t, store.Init(&meta.Format{
		Name:      "fmtroot",
		Storage:   "file",
		Bucket:    t.TempDir(),
		BlockSize: 4,
		TrashDays: 0,
	}, true))
	require.NoError(t, store.Shutdown())

	override := int64(7 << 32)
	out, err := run9DescribeFormat(context.Background(), fmt.Sprintf("badger://%s?nextchunk=%d", metaDir, override))
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, "fmtroot", out.JuiceFSFormatName)

	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	readCounter := func(key string) int64 {
		var raw []byte
		require.NoError(t, db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				raw = append([]byte(nil), val...)
				return nil
			})
		}))
		require.Len(t, raw, 8)
		return int64(binary.LittleEndian.Uint64(raw))
	}

	require.Equal(t, override, readCounter("CnextChunk"))
	require.Equal(t, override+(1<<32), readCounter("Crun9NextChunkLimit"))
}

func TestRun9PrepareWritableEpochPersistsNextChunkOverride(t *testing.T) {
	metaDir := filepath.Join(t.TempDir(), "meta")
	store := meta.NewClient("badger://"+metaDir, meta.DefaultConf())
	require.NoError(t, store.Init(&meta.Format{
		Name:      "fmtroot",
		Storage:   "file",
		Bucket:    t.TempDir(),
		BlockSize: 4,
		TrashDays: 0,
	}, true))
	require.NoError(t, store.Shutdown())

	out, err := run9PrepareWritableEpoch(context.Background(), "badger://"+metaDir, 7)
	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, "fmtroot", out.JuiceFSFormatName)

	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	readCounter := func(key string) int64 {
		var raw []byte
		require.NoError(t, db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				raw = append([]byte(nil), val...)
				return nil
			})
		}))
		require.Len(t, raw, 8)
		return int64(binary.LittleEndian.Uint64(raw))
	}

	override := int64(7 << 32)
	require.Equal(t, override, readCounter("CnextChunk"))
	require.Equal(t, override+(1<<32), readCounter("Crun9NextChunkLimit"))
}

func TestRun9GCSliceRangesDeletesOnlyMatchingObjects(t *testing.T) {
	bucket := t.TempDir()
	matchingKey := "chunks/0/0/11_0_3"
	otherKey := "chunks/0/0/12_0_3"
	matchingPath := filepath.Join(bucket, "fmtroot", matchingKey)
	otherPath := filepath.Join(bucket, "fmtroot", otherKey)
	require.NoError(t, os.MkdirAll(filepath.Dir(matchingPath), 0o755))
	require.NoError(t, os.WriteFile(matchingPath, []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(otherPath, []byte("def"), 0o644))

	out, err := run9GCSliceRanges(context.Background(), run9GCSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges:           []run9GCSliceRange{{Start: 11, EndInclusive: 11}},
		MaxDeleteObjects: 10,
		Threads:          1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.DeletedObjects)
	require.Equal(t, uint64(3), out.DeletedBytes)
	require.False(t, out.HasMore)
	require.NoFileExists(t, matchingPath)
	require.FileExists(t, otherPath)
}

func TestRun9GCSliceRangesStopsAtBudget(t *testing.T) {
	bucket := t.TempDir()
	firstPath := filepath.Join(bucket, "fmtroot", "chunks/0/0/21_0_3")
	secondPath := filepath.Join(bucket, "fmtroot", "chunks/0/0/22_0_3")
	require.NoError(t, os.MkdirAll(filepath.Dir(firstPath), 0o755))
	require.NoError(t, os.WriteFile(firstPath, []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(secondPath, []byte("def"), 0o644))

	out, err := run9GCSliceRanges(context.Background(), run9GCSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges:           []run9GCSliceRange{{Start: 21, EndInclusive: 22}},
		MaxDeleteObjects: 1,
		Threads:          1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.DeletedObjects)
	require.True(t, out.HasMore)
}

func TestRun9GCSliceRangesDoesNotReportMoreWhenBudgetEqualsEOF(t *testing.T) {
	bucket := t.TempDir()
	matchingPath := filepath.Join(bucket, "fmtroot", "chunks/0/0/31_0_3")
	require.NoError(t, os.MkdirAll(filepath.Dir(matchingPath), 0o755))
	require.NoError(t, os.WriteFile(matchingPath, []byte("abc"), 0o644))

	out, err := run9GCSliceRanges(context.Background(), run9GCSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges:           []run9GCSliceRange{{Start: 31, EndInclusive: 31}},
		MaxDeleteObjects: 1,
		Threads:          1,
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.DeletedObjects)
	require.False(t, out.HasMore)
}

func TestRun9CountSliceRangesCountsOnlyMatchingObjects(t *testing.T) {
	bucket := t.TempDir()
	matchingKey := "chunks/0/0/51_0_3"
	otherKey := "chunks/0/0/52_0_5"
	matchingPath := filepath.Join(bucket, "fmtroot", matchingKey)
	otherPath := filepath.Join(bucket, "fmtroot", otherKey)
	require.NoError(t, os.MkdirAll(filepath.Dir(matchingPath), 0o755))
	require.NoError(t, os.WriteFile(matchingPath, []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(otherPath, []byte("abcde"), 0o644))

	out, err := run9CountSliceRanges(context.Background(), run9CountSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges: []run9GCSliceRange{{Start: 51, EndInclusive: 51}},
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Greater(t, out.ScanListRequests, uint64(0))
	require.GreaterOrEqual(t, out.ScanListedObjects, uint64(2))
	require.Equal(t, uint64(1), out.Objects)
	require.Equal(t, uint64(3), out.Bytes)
	require.Equal(t, []run9CountSliceRangeAccount{{
		Start:        51,
		EndInclusive: 51,
		Objects:      1,
		Bytes:        3,
	}}, out.Ranges)
	require.FileExists(t, matchingPath)
	require.FileExists(t, otherPath)
}

func TestRun9CountSliceRangesUsesHashPrefixLayout(t *testing.T) {
	bucket := t.TempDir()
	matchingKey := chunk.FormatObjectBlockKey(61, 0, 3, true)
	otherKey := chunk.FormatObjectBlockKey(62, 0, 5, true)
	matchingPath := filepath.Join(bucket, "fmtroot", matchingKey)
	otherPath := filepath.Join(bucket, "fmtroot", otherKey)
	require.NoError(t, os.MkdirAll(filepath.Dir(matchingPath), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(otherPath), 0o755))
	require.NoError(t, os.WriteFile(matchingPath, []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(otherPath, []byte("abcde"), 0o644))

	out, err := run9CountSliceRanges(context.Background(), run9CountSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096, HashPrefix: true},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges: []run9GCSliceRange{{Start: 61, EndInclusive: 61}},
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(1), out.Objects)
	require.Equal(t, uint64(3), out.Bytes)
	require.Equal(t, []run9CountSliceRangeAccount{{
		Start:        61,
		EndInclusive: 61,
		Objects:      1,
		Bytes:        3,
	}}, out.Ranges)
}

func TestRun9CountSliceRangesReturnsPerRangeAccountsInRequestOrder(t *testing.T) {
	bucket := t.TempDir()
	firstPath := filepath.Join(bucket, "fmtroot", "chunks/0/0/71_0_3")
	secondPath := filepath.Join(bucket, "fmtroot", "chunks/0/0/72_0_5")
	require.NoError(t, os.MkdirAll(filepath.Dir(firstPath), 0o755))
	require.NoError(t, os.WriteFile(firstPath, []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(secondPath, []byte("abcde"), 0o644))

	out, err := run9CountSliceRanges(context.Background(), run9CountSliceRangesRequest{
		JuiceFSFormatName: "fmtroot",
		ObjectLayout:      run9ObjectLayout{BlockSizeBytes: 4096},
		ObjectStorage: run9ObjectStorageDescriptor{
			Storage: "file",
			Bucket:  bucket + string(os.PathSeparator),
		},
		Ranges: []run9GCSliceRange{
			{SnapID: "snap-b", Start: 72, EndInclusive: 72},
			{SnapID: "snap-a", Start: 71, EndInclusive: 71},
		},
	})

	require.NoError(t, err)
	require.True(t, out.OK)
	require.Equal(t, uint64(2), out.Objects)
	require.Equal(t, uint64(8), out.Bytes)
	require.Equal(t, []run9CountSliceRangeAccount{
		{SnapID: "snap-b", Start: 72, EndInclusive: 72, Objects: 1, Bytes: 5},
		{SnapID: "snap-a", Start: 71, EndInclusive: 71, Objects: 1, Bytes: 3},
	}, out.Ranges)
}

func TestRun9CountSliceRangesRejectsOverlappingRanges(t *testing.T) {
	_, _, _, err := countRun9SliceRangeMatches(context.Background(), nil, false, []run9GCSliceRange{
		{Start: 81, EndInclusive: 83},
		{Start: 83, EndInclusive: 84},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "overlapping ranges")
}

type fakeRun9GCSliceRangesStreamingStore struct {
	object.ObjectStorage
	firstDelete chan struct{}
	firstOnce   sync.Once
}

func (f *fakeRun9GCSliceRangesStreamingStore) DeleteObjects(ctx context.Context, keys []string, getters ...object.AttrGetter) error {
	f.firstOnce.Do(func() { close(f.firstDelete) })
	return nil
}

func TestDeleteRun9SliceRangeMatchesStreamsDeletesBeforeScanFinishes(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	store := &fakeRun9GCSliceRangesStreamingStore{
		ObjectStorage: base,
		firstDelete:   make(chan struct{}),
	}

	objs := make(chan object.Object, 4)
	_, stopScan := context.WithCancel(context.Background())
	defer stopScan()

	type result struct {
		deletedObjects uint64
		deletedBytes   uint64
		hasMore        bool
		err            error
	}
	done := make(chan result, 1)
	go func() {
		deletedObjects, deletedBytes, hasMore, err := deleteRun9SliceRangeMatches(
			context.Background(),
			stopScan,
			store,
			objs,
			false,
			[]run9GCSliceRange{{Start: 41, EndInclusive: 44}},
			4,
			2,
		)
		done <- result{
			deletedObjects: deletedObjects,
			deletedBytes:   deletedBytes,
			hasMore:        hasMore,
			err:            err,
		}
	}()

	objs <- testRun9GCObject(chunk.FormatObjectBlockKey(41, 0, 1, false))
	objs <- testRun9GCObject(chunk.FormatObjectBlockKey(42, 0, 1, false))
	select {
	case <-store.firstDelete:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first delete batch before scan reached EOF")
	}
	objs <- testRun9GCObject(chunk.FormatObjectBlockKey(43, 0, 1, false))
	objs <- testRun9GCObject(chunk.FormatObjectBlockKey(44, 0, 1, false))
	close(objs)

	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.Equal(t, uint64(4), out.deletedObjects)
		require.Equal(t, uint64(4), out.deletedBytes)
		require.False(t, out.hasMore)
	case <-time.After(2 * time.Second):
		t.Fatal("deleteRun9SliceRangeMatches did not finish")
	}
}

type fakeRun9GCSliceRangeListStore struct {
	object.ObjectStorage
	pages [][]object.Object
	call  int
}

func (f *fakeRun9GCSliceRangeListStore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	if f.call >= len(f.pages) {
		return nil, false, "", nil
	}
	page := f.pages[f.call]
	f.call++
	hasMore := f.call < len(f.pages)
	return page, hasMore, fmt.Sprintf("token-%d", f.call), nil
}

func TestListRun9GCSliceRangeObjectsCountsListRequests(t *testing.T) {
	base, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)

	objs, scanStats, err := listRun9GCSliceRangeObjects(context.Background(), &fakeRun9GCSliceRangeListStore{
		ObjectStorage: base,
		pages: [][]object.Object{
			{
				testRun9GCObject(chunk.FormatObjectBlockKey(51, 0, 1, false)),
				testRun9GCObject(chunk.FormatObjectBlockKey(52, 0, 1, false)),
			},
			{
				testRun9GCObject(chunk.FormatObjectBlockKey(53, 0, 1, false)),
			},
		},
	}, "chunks/", "", true)
	require.NoError(t, err)

	var listed []object.Object
	for obj := range objs {
		listed = append(listed, obj)
	}
	<-scanStats.done

	require.Len(t, listed, 3)
	listRequests, listedObjects := scanStats.snapshot()
	require.Equal(t, uint64(2), listRequests)
	require.Equal(t, uint64(3), listedObjects)
}

func testRun9GCObject(key string) object.Object {
	return object.UnmarshalObject(map[string]interface{}{
		"key":   key,
		"size":  float64(1),
		"mtime": "0",
		"isdir": false,
	})
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
