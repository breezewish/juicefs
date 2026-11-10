package chunk

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestPrefetchBlockFetchesOnlyAnUncachedBlock(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.BlockSize = 4
	conf.CacheFullBlock = true // as in production mounts
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), filepath.Join(t.TempDir(), "volume")
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	// Slice 301 holds 10 bytes in blocks of 4, 4 and 2 bytes.
	for indx, data := range []string{"abcd", "efgh", "ij"} {
		key := FormatObjectBlockKey(301, uint64(indx), uint64(len(data)), false)
		require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader([]byte(data))))
	}

	var admitted []int
	var downloads []bool
	admit := func(_ context.Context, size int) (func(bool), error) {
		admitted = append(admitted, size)
		return func(downloaded bool) { downloads = append(downloads, downloaded) }, nil
	}
	fetched, err := store.PrefetchBlock(context.Background(), 301, 10, 2, admit)
	require.NoError(t, err)
	require.Equal(t, 2, fetched)
	require.Equal(t, []int{2}, admitted)
	require.Equal(t, []bool{true}, downloads)
	require.Equal(t, int64(1), storage.gets.Load())
	require.NoFileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, FormatObjectBlockKey(301, 1, 4, false)))
	require.FileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, FormatObjectBlockKey(301, 2, 2, false)))

	// A block a local cache holds is neither admitted, fetched nor read again.
	fetched, err = store.PrefetchBlock(context.Background(), 301, 10, 2, admit)
	require.NoError(t, err)
	require.Zero(t, fetched)
	require.Equal(t, []int{2}, admitted)
	require.Equal(t, int64(1), storage.gets.Load())

	// A block admit refuses is not fetched.
	refused := errors.New("refused")
	fetched, err = store.PrefetchBlock(context.Background(), 301, 10, 0, func(context.Context, int) (func(bool), error) { return nil, refused })
	require.ErrorIs(t, err, refused)
	require.Zero(t, fetched)
	require.Equal(t, int64(1), storage.gets.Load())

	// Prefetches are not the workload's: no hit or miss is counted.
	require.Zero(t, toFloat64(store.cacheHits))
	require.Zero(t, toFloat64(store.cacheMiss))
	require.Zero(t, toFloat64(store.cleanCacheHits))
}

func TestSettlePrefetchCountsBlocksNoWorkloadRead(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	conf := defaultConf
	conf.BlockSize = 4
	conf.CacheFullBlock = true
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), filepath.Join(t.TempDir(), "volume")
	store := NewCachedStore(mem, conf, nil).(*cachedStore)
	for indx, data := range []string{"abcd", "efgh", "ij"} {
		key := FormatObjectBlockKey(301, uint64(indx), uint64(len(data)), false)
		require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader([]byte(data))))
	}
	admit := func(context.Context, int) (func(bool), error) { return func(bool) {}, nil }
	for indx := range 3 {
		_, err := store.PrefetchBlock(context.Background(), 301, 10, indx, admit)
		require.NoError(t, err)
	}
	require.Equal(t, float64(3), toFloat64(store.prefetchBlocks))
	require.Equal(t, float64(10), toFloat64(store.prefetchBytes))

	// A workload reads the second block; a prefetch read of the third does not count.
	page := NewPage(make([]byte, 4))
	_, err = store.NewReader(301, 10).ReadAt(context.Background(), page, 4)
	require.NoError(t, err)
	page = NewPage(make([]byte, 2))
	_, err = store.NewReader(301, 10).ReadAt(WithPrefetch(context.Background()), page, 8)
	require.NoError(t, err)

	blocks, unread := store.SettlePrefetch()
	require.Equal(t, 2, blocks)
	require.Equal(t, int64(6), unread)
	require.Equal(t, float64(2), toFloat64(store.prefetchUnreadBlocks))
	require.Equal(t, float64(6), toFloat64(store.prefetchUnreadBytes))

	require.Equal(t, float64(1), toFloat64(store.prefetchSettled))

	// Settled once: a later prefetch is no longer tracked.
	_, err = store.PrefetchBlock(context.Background(), 302, 10, 0, admit)
	require.Error(t, err) // slice 302 does not exist, but its download was spent
	blocks, unread = store.SettlePrefetch()
	require.Zero(t, blocks)
	require.Zero(t, unread)
}

func TestPrefetchBlockLoadedByAWorkloadDuringAdmissionIsNotAPrefetch(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.BlockSize = 4
	conf.CacheFullBlock = true
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), filepath.Join(t.TempDir(), "volume")
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	require.NoError(t, mem.Put(context.Background(), FormatObjectBlockKey(301, 0, 4, false), bytes.NewReader([]byte("abcd"))))

	// The guest reads the block while the prefetch waits for admission.
	var downloads []bool
	admit := func(context.Context, int) (func(bool), error) {
		page := NewPage(make([]byte, 4))
		_, err := store.NewReader(301, 4).ReadAt(context.Background(), page, 0)
		require.NoError(t, err)
		return func(downloaded bool) { downloads = append(downloads, downloaded) }, nil
	}
	fetched, err := store.PrefetchBlock(context.Background(), 301, 4, 0, admit)
	require.NoError(t, err)
	require.Zero(t, fetched)
	require.Equal(t, []bool{false}, downloads)
	require.Equal(t, int64(1), storage.gets.Load(), "only the guest downloaded it")
	require.Zero(t, toFloat64(store.prefetchBlocks))
	blocks, unread := store.SettlePrefetch()
	require.Zero(t, blocks)
	require.Zero(t, unread)
}
