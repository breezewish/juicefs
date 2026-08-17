package chunk

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

type sharedCacheCountingStorage struct {
	object.ObjectStorage
	gets atomic.Int64
}

func (s *sharedCacheCountingStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	s.gets.Add(1)
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

func writeSharedCacheBlock(t *testing.T, root, key string, data []byte) string {
	t.Helper()
	path := filepath.Join(root, cacheDir, filepath.FromSlash(key))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o444))
	return path
}

func readSharedCacheTestSlice(t *testing.T, store ChunkStore, id uint64, size int) []byte {
	t.Helper()
	page := NewPage(make([]byte, size))
	defer page.Release()
	n, err := store.NewReader(id, size).ReadAt(context.Background(), page, 0)
	require.NoError(t, err)
	require.Equal(t, size, n)
	return append([]byte(nil), page.Data...)
}

func TestSharedCacheHitReadsWithoutObjectGet(t *testing.T) {
	sharedDir := t.TempDir()
	data := []byte("shared")
	key := FormatObjectBlockKey(101, 0, uint64(len(data)), false)
	sharedPath := writeSharedCacheBlock(t, sharedDir, key, data)
	before, err := os.Stat(sharedPath)
	require.NoError(t, err)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = "memory"
	conf.SharedCacheDir = sharedDir
	store := NewCachedStore(storage, conf, nil).(*cachedStore)

	require.Equal(t, data, readSharedCacheTestSlice(t, store, 101, len(data)))
	require.Zero(t, storage.gets.Load())
	require.Equal(t, float64(1), toFloat64(store.sharedCacheHits))
	require.Equal(t, float64(len(data)), toFloat64(store.sharedCacheHitBytes))

	after, err := os.Stat(sharedPath)
	require.NoError(t, err)
	require.Equal(t, before.Mode(), after.Mode())
	require.Equal(t, before.Size(), after.Size())
	require.Equal(t, data, requireFileContents(t, sharedPath))
}

func TestSharedCacheMissFetchesObjectIntoPrivateCacheOnly(t *testing.T) {
	privateDir := t.TempDir()
	sharedDir := t.TempDir()
	data := []byte("object")
	key := FormatObjectBlockKey(102, 0, uint64(len(data)), false)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = privateDir
	conf.SharedCacheDir = sharedDir
	store := NewCachedStore(storage, conf, nil).(*cachedStore)

	require.Equal(t, data, readSharedCacheTestSlice(t, store, 102, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	require.Equal(t, float64(1), toFloat64(store.sharedCacheMisses))
	require.NoFileExists(t, filepath.Join(sharedDir, cacheDir, filepath.FromSlash(key)))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(privateDir, cacheDir, filepath.FromSlash(key)))
		return statErr == nil
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, data, readSharedCacheTestSlice(t, store, 102, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
}

func TestPrivateCacheTakesPrecedenceOverSharedCache(t *testing.T) {
	privateDir := t.TempDir()
	sharedDir := t.TempDir()
	privateData := []byte("private")
	sharedData := []byte("shared!")
	key := FormatObjectBlockKey(103, 0, uint64(len(privateData)), false)
	writeSharedCacheBlock(t, privateDir, key, privateData)
	writeSharedCacheBlock(t, sharedDir, key, sharedData)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = privateDir
	conf.SharedCacheDir = sharedDir
	store := NewCachedStore(storage, conf, nil).(*cachedStore)

	require.Equal(t, privateData, readSharedCacheTestSlice(t, store, 103, len(privateData)))
	require.Zero(t, storage.gets.Load())
	require.Zero(t, toFloat64(store.sharedCacheHits))
}

func TestCorruptSharedCacheBlockFallsThroughWithoutMutation(t *testing.T) {
	privateDir := t.TempDir()
	sharedDir := t.TempDir()
	data := []byte("valid")
	key := FormatObjectBlockKey(104, 0, uint64(len(data)), false)
	sharedPath := writeSharedCacheBlock(t, sharedDir, key, []byte("bad"))

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	conf := defaultConf
	conf.CacheDir = privateDir
	conf.SharedCacheDir = sharedDir
	store := NewCachedStore(storage, conf, nil).(*cachedStore)

	require.Equal(t, data, readSharedCacheTestSlice(t, store, 104, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	require.Equal(t, float64(1), toFloat64(store.sharedCacheErrors))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(privateDir, cacheDir, filepath.FromSlash(key)))
		return statErr == nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []byte("bad"), requireFileContents(t, sharedPath))
}

func TestConcurrentStoresReadSameSharedCacheWithoutObjectGets(t *testing.T) {
	sharedDir := t.TempDir()
	data := []byte("shared-concurrent")
	key := FormatObjectBlockKey(105, 0, uint64(len(data)), false)
	writeSharedCacheBlock(t, sharedDir, key, data)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	firstConf := defaultConf
	firstConf.CacheDir = "memory"
	firstConf.SharedCacheDir = sharedDir
	secondConf := defaultConf
	secondConf.CacheDir = "memory"
	secondConf.SharedCacheDir = sharedDir
	first := NewCachedStore(storage, firstConf, nil)
	second := NewCachedStore(storage, secondConf, nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			require.Equal(t, data, readSharedCacheTestSlice(t, first, 105, len(data)))
		}()
		go func() {
			defer wg.Done()
			require.Equal(t, data, readSharedCacheTestSlice(t, second, 105, len(data)))
		}()
	}
	wg.Wait()
	require.Zero(t, storage.gets.Load())
}

func TestWritesNeverPopulateSharedCache(t *testing.T) {
	privateDir := t.TempDir()
	sharedDir := t.TempDir()
	data := []byte("private-write")
	key := FormatObjectBlockKey(106, 0, uint64(len(data)), false)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	conf := defaultConf
	conf.CacheDir = privateDir
	conf.SharedCacheDir = sharedDir
	conf.Writeback = true
	conf.WritebackThresholdSize = conf.BlockSize + 1
	conf.UploadDelay = time.Hour
	conf.FreeSpace = 0.000001
	store := NewCachedStore(mem, conf, nil)

	writer := store.NewWriter(106)
	n, err := writer.WriteAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.NoError(t, writer.Finish(len(data)))
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(filepath.Join(privateDir, stagingDir, filepath.FromSlash(key)))
		return statErr == nil
	}, time.Second, 10*time.Millisecond)
	require.NoFileExists(t, filepath.Join(sharedDir, cacheDir, filepath.FromSlash(key)))
	require.NoFileExists(t, filepath.Join(sharedDir, stagingDir, filepath.FromSlash(key)))
}

func TestConfigSelfCheckScopesSharedCacheByVolumeUUID(t *testing.T) {
	conf := defaultConf
	conf.CacheDir = "/private"
	conf.SharedCacheDir = "/shared"

	conf.SelfCheck("volume-id")

	require.Equal(t, filepath.Join("/private", "volume-id"), conf.CacheDir)
	require.Equal(t, filepath.Join("/shared", "volume-id"), conf.SharedCacheDir)
}

func TestConfigSelfCheckScopesSharedCacheWhenPrivateCacheIsMemory(t *testing.T) {
	conf := defaultConf
	conf.CacheDir = "memory"
	conf.SharedCacheDir = "/shared"

	conf.SelfCheck("volume-id")

	require.Equal(t, "memory", conf.CacheDir)
	require.Equal(t, filepath.Join("/shared", "volume-id"), conf.SharedCacheDir)
}

func requireFileContents(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
