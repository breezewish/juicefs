package chunk

import (
	"bytes"
	"context"
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

	fetched, err := store.PrefetchBlock(context.Background(), 301, 10, 2)
	require.NoError(t, err)
	require.True(t, fetched)
	require.Equal(t, int64(1), storage.gets.Load())
	require.NoFileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, FormatObjectBlockKey(301, 1, 4, false)))
	require.FileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, FormatObjectBlockKey(301, 2, 2, false)))

	// A block a local cache holds is neither fetched nor read again.
	fetched, err = store.PrefetchBlock(context.Background(), 301, 10, 2)
	require.NoError(t, err)
	require.False(t, fetched)
	require.Equal(t, int64(1), storage.gets.Load())

	// Prefetches are not the workload's: no hit or miss is counted.
	require.Zero(t, toFloat64(store.cacheHits))
	require.Zero(t, toFloat64(store.cacheMiss))
	require.Zero(t, toFloat64(store.cleanCacheHits))
}
