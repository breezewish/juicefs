package chunk

import (
	"bytes"
	"context"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestCacheOnlyReadServesSharedCacheUncountedAndNeverGetsObjects(t *testing.T) {
	sharedDir := t.TempDir()
	cached := []byte("cached")
	writeSharedCacheBlock(t, sharedDir, FormatObjectBlockKey(201, 0, uint64(len(cached)), false), cached)
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	uncached := []byte("remote")
	require.NoError(t, mem.Put(context.Background(), FormatObjectBlockKey(202, 0, uint64(len(uncached)), false), bytes.NewReader(uncached)))
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = t.TempDir()
	conf.SharedCacheDir = sharedDir
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	ctx := WithCacheOnly(context.Background())

	page := NewPage(make([]byte, len(cached)))
	defer page.Release()
	n, err := store.NewReader(201, len(cached)).ReadAt(ctx, page, 0)
	require.NoError(t, err)
	require.Equal(t, cached, page.Data[:n])

	missing := NewPage(make([]byte, len(uncached)))
	defer missing.Release()
	n, err = store.NewReader(202, len(uncached)).ReadAt(ctx, missing, 0)
	require.ErrorIs(t, err, ErrNotCached)
	require.Zero(t, n)
	require.Zero(t, storage.gets.Load())
	require.Zero(t, toFloat64(store.sharedCacheHits))
	require.Zero(t, toFloat64(store.sharedCacheMisses))
	require.Zero(t, toFloat64(store.cacheHits))
}
