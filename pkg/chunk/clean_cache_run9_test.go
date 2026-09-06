package chunk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestCleanCacheSharesOneCopyAcrossIndependentStores(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	data := []byte("a block inherited by parent, child and sibling")
	key := FormatObjectBlockKey(101, 0, uint64(len(data)), false)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
	confA := defaultConf
	confA.CacheDir, confA.CleanCacheDir = t.TempDir(), filepath.Join(t.TempDir(), "volume")
	confB, confC := confA, confA
	confB.CacheDir, confC.CacheDir = t.TempDir(), t.TempDir()
	a := NewCachedStore(storage, confA, nil).(*cachedStore)
	b := NewCachedStore(storage, confB, nil).(*cachedStore)
	c := NewCachedStore(storage, confC, nil).(*cachedStore)

	// All readers already exist before a different process fills the directory.
	require.Equal(t, data, readSharedCacheTestSlice(t, c, 101, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	require.Equal(t, data, readSharedCacheTestSlice(t, a, 101, len(data)))
	require.Equal(t, data, readSharedCacheTestSlice(t, b, 101, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	require.Equal(t, float64(1), toFloat64(a.cleanCacheHits))
	require.Equal(t, float64(1), toFloat64(b.cleanCacheHits))
	require.NoFileExists(t, filepath.Join(confA.CacheDir, cacheDir, key))
	require.NoFileExists(t, filepath.Join(confB.CacheDir, cacheDir, key))
	require.NoFileExists(t, filepath.Join(confC.CacheDir, cacheDir, key))
	usage, err := PruneCleanCache(context.Background(), filepath.Dir(confA.CleanCacheDir), 1<<30, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.Blocks)
	require.Equal(t, uint64(len(data)+len(checksum(data))), usage.Bytes)
}

func TestCleanCacheConcurrentPublicationAndCorruptRead(t *testing.T) {
	cache := &cleanDiskCache{dir: filepath.Join(t.TempDir(), "volume"), mode: 0600, metrics: newCacheManagerMetrics(nil)}
	data := bytes.Repeat([]byte("payload"), 10000)
	key := FormatObjectBlockKey(201, 0, uint64(len(data)), false)
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- cache.publish(key, data, false) }()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.ErrorContains(t, cache.publish(key, bytes.Repeat([]byte("changed"), 10000), false), "conflict")
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(cache.dir, cacheDir, key)))
	require.NoError(t, err)
	require.Len(t, entries, 1)

	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
	conf := defaultConf
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), cache.dir
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	// A same-sized file with no checksum must never be accepted as a hit.
	require.NoError(t, os.WriteFile(filepath.Join(cache.dir, cacheDir, key), bytes.Repeat([]byte("corrupt"), 10000), 0600))
	require.Equal(t, data, readSharedCacheTestSlice(t, store, 201, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	require.Equal(t, float64(1), toFloat64(store.cleanCacheErrors))
	require.Equal(t, data, readSharedCacheTestSlice(t, store, 201, len(data)))
	require.Equal(t, int64(1), storage.gets.Load())
	// Valid length and trailer are insufficient if the payload itself changed.
	f, err := os.OpenFile(filepath.Join(cache.dir, cacheDir, key), os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("X"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, data, readSharedCacheTestSlice(t, store, 201, len(data)))
	require.Equal(t, int64(2), storage.gets.Load())
	require.Equal(t, float64(2), toFloat64(store.cleanCacheErrors))
}

type cleanCacheUploadStorage struct {
	object.ObjectStorage
	entered chan struct{}
	release chan struct{}
	fail    bool
}

func (s *cleanCacheUploadStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.fail {
		return errors.New("test PUT failure")
	}
	return s.ObjectStorage.Put(ctx, key, in, getters...)
}

func TestCleanCachePublishesOnlyAfterUploadAndKeepsDrainPrivate(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &cleanCacheUploadStorage{ObjectStorage: mem, entered: make(chan struct{}, 1), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(storage.release) })
	defer release()
	conf := defaultConf
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), filepath.Join(t.TempDir(), "volume")
	conf.Writeback, conf.WritebackThresholdSize = true, conf.BlockSize
	conf.Compress = "lz4"
	conf.PutTimeout = 10 * time.Second
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	otherConf := conf
	otherConf.CacheDir = t.TempDir()
	other := NewCachedStore(mem, otherConf, nil).(*cachedStore)
	data := []byte("new child data")
	key := FormatObjectBlockKey(301, 0, uint64(len(data)), false)
	writer := store.NewWriter(301)
	_, err = writer.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Finish(len(data)))
	select {
	case <-storage.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
	require.FileExists(t, filepath.Join(conf.CacheDir, stagingDir, key))
	require.NoFileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, key))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, other.WaitForUploadDrain(ctx))
	_, err = PruneCleanCache(ctx, filepath.Dir(conf.CleanCacheDir), 0, 0)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(conf.CacheDir, stagingDir, key))
	release()
	require.NoError(t, store.WaitForUploadDrain(ctx))
	require.FileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, key))
	require.NoFileExists(t, filepath.Join(conf.CacheDir, stagingDir, key))
	require.NoFileExists(t, filepath.Join(conf.CacheDir, cacheDir, key))
	require.Equal(t, data, readSharedCacheTestSlice(t, other, 301, len(data)))
	require.Equal(t, float64(1), toFloat64(other.cleanCacheHits))
}

func TestCleanCacheFailedPutNeverPublishes(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &cleanCacheUploadStorage{ObjectStorage: mem, fail: true}
	conf := defaultConf
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), t.TempDir()
	conf.MaxRetries = 1
	store := NewCachedStore(storage, conf, nil)
	writer := store.NewWriter(401)
	_, err = writer.WriteAt([]byte("failed"), 0)
	require.NoError(t, err)
	require.ErrorContains(t, writer.Finish(6), "test PUT failure")
	require.NoFileExists(t, filepath.Join(conf.CleanCacheDir, cacheDir, FormatObjectBlockKey(401, 0, 6, false)))
}

func TestCleanCacheScopesVolumeAndPreservesPrewarmPrecedence(t *testing.T) {
	conf := defaultConf
	conf.CacheDir, conf.CleanCacheDir = t.TempDir(), t.TempDir()
	otherConf := conf
	conf.SelfCheck("volume-a")
	otherConf.SelfCheck("volume-b")
	require.NotEqual(t, conf.CleanCacheDir, otherConf.CleanCacheDir)
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	data := []byte("base")
	key := FormatObjectBlockKey(501, 0, 4, false)
	require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
	storage := &sharedCacheCountingStorage{ObjectStorage: mem}
	a := NewCachedStore(storage, conf, nil).(*cachedStore)
	b := NewCachedStore(storage, otherConf, nil).(*cachedStore)
	require.Equal(t, data, readSharedCacheTestSlice(t, a, 501, len(data)))
	require.Equal(t, data, readSharedCacheTestSlice(t, b, 501, len(data)))
	require.Equal(t, int64(2), storage.gets.Load())

	prewarm := t.TempDir()
	writeSharedCacheBlock(t, prewarm, key, data)
	conf.SharedCacheDir = prewarm
	withPrewarm := NewCachedStore(storage, conf, nil).(*cachedStore)
	require.Equal(t, data, readSharedCacheTestSlice(t, withPrewarm, 501, len(data)))
	require.Equal(t, float64(1), toFloat64(withPrewarm.sharedCacheHits))
	require.Zero(t, toFloat64(withPrewarm.cleanCacheHits))
}

func TestCleanCachePruneEvictsColdBlocksAcrossVolumes(t *testing.T) {
	root := t.TempDir()
	a := &cleanDiskCache{dir: filepath.Join(root, "a"), mode: 0600, metrics: newCacheManagerMetrics(nil)}
	b := &cleanDiskCache{dir: filepath.Join(root, "b"), mode: 0600, metrics: newCacheManagerMetrics(nil)}
	key := FormatObjectBlockKey(601, 0, 4, false)
	require.NoError(t, a.publish(key, []byte("cold"), false))
	require.NoError(t, b.publish(key, []byte("warm"), false))
	coldPath := filepath.Join(a.dir, cacheDir, key)
	warmPath := filepath.Join(b.dir, cacheDir, key)
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(coldPath, old, old))
	// A reader with an open fd remains valid after cache eviction.
	r, err := openCacheFile(coldPath, 4, CsExtend)
	require.NoError(t, err)
	defer r.Close()
	usage, err := PruneCleanCache(context.Background(), root, 10, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.RemovedBlocks)
	require.Equal(t, uint64(1), usage.Blocks)
	require.NoFileExists(t, coldPath)
	require.FileExists(t, warmPath)
	data := make([]byte, 4)
	_, err = r.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, "cold", string(data))
}

func TestCleanCacheAdmissionFailureDoesNotFailUpload(t *testing.T) {
	mem, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	conf := defaultConf
	conf.CacheDir = t.TempDir()
	conf.CleanCacheDir = filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(conf.CleanCacheDir, []byte("unavailable cache"), 0600))
	store := NewCachedStore(mem, conf, nil).(*cachedStore)
	writer := store.NewWriter(701)
	_, err = writer.WriteAt([]byte("safe remotely"), 0)
	require.NoError(t, err)
	require.NoError(t, writer.Finish(13))
	require.Equal(t, float64(1), toFloat64(store.bcache.getMetrics().cacheDrops))
	key := FormatObjectBlockKey(701, 0, 13, false)
	r, err := mem.Get(context.Background(), key, 0, -1)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "safe remotely", string(data))
}

func TestCleanCachePruneRejectsSymlinksWithoutTouchingTarget(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	target := filepath.Join(outside, "private-data")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "volume")))
	_, err := PruneCleanCache(context.Background(), root, 0, 0)
	require.ErrorContains(t, err, "unexpected clean cache entry")
	require.FileExists(t, target)
}

func TestCleanCachePruneSerializesCleanersAndRemovesAbandonedFills(t *testing.T) {
	root := t.TempDir()
	lock := flock.New(filepath.Join(root, ".prune.lock"))
	require.NoError(t, lock.Lock())
	usage, err := PruneCleanCache(context.Background(), root, 0, 0)
	require.NoError(t, err)
	require.True(t, usage.Busy)
	require.NoError(t, lock.Close())
	cache := &cleanDiskCache{dir: filepath.Join(root, "volume"), mode: 0600, metrics: newCacheManagerMetrics(nil)}
	key := FormatObjectBlockKey(801, 0, 4, true)
	require.NoError(t, cache.publish(key, []byte("data"), false))
	dir := filepath.Dir(filepath.Join(cache.dir, cacheDir, key))
	abandoned, active := filepath.Join(dir, ".fill-crashed.tmp"), filepath.Join(dir, ".fill-active.tmp")
	require.NoError(t, os.WriteFile(abandoned, []byte("partial"), 0600))
	require.NoError(t, os.WriteFile(active, []byte("partial"), 0600))
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(abandoned, old, old))
	usage, err = PruneCleanCache(context.Background(), root, 1<<30, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), usage.Blocks)
	require.NoFileExists(t, abandoned)
	require.FileExists(t, active)
	require.FileExists(t, filepath.Join(cache.dir, cacheDir, key))
}
