package chunk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type finalizeTestCacheManager struct {
	root     string
	checksum string
}

func newFinalizeTestCacheManager(t *testing.T, checksum string) *finalizeTestCacheManager {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, stagingDir), 0o755); err != nil {
		t.Fatalf("mkdir rawstaging: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, cacheDir), 0o755); err != nil {
		t.Fatalf("mkdir raw: %v", err)
	}
	return &finalizeTestCacheManager{root: root, checksum: checksum}
}

func (m *finalizeTestCacheManager) cache(string, *Page, bool, bool) {}

func (m *finalizeTestCacheManager) remove(string, bool) {}

func (m *finalizeTestCacheManager) load(key string) (ReadCloser, error) {
	return openCacheFile(filepath.Join(m.root, cacheDir, key), parseObjOrigSize(key), m.checksum)
}

func (m *finalizeTestCacheManager) exist(key string) (string, bool) {
	_, err := os.Stat(filepath.Join(m.root, cacheDir, key))
	return m.root, err == nil
}

func (m *finalizeTestCacheManager) uploaded(string, int) {}

func (m *finalizeTestCacheManager) stage(key string, data []byte) (string, error) {
	stagingPath := filepath.Join(m.root, stagingDir, key)
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0o755); err != nil {
		return stagingPath, err
	}
	if err := os.WriteFile(stagingPath, data, 0o644); err != nil {
		return stagingPath, err
	}
	if m.checksum != CsNone {
		f, err := os.OpenFile(stagingPath, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			return stagingPath, err
		}
		if _, err := f.Write(checksum(data)); err != nil {
			_ = f.Close()
			return stagingPath, err
		}
		if err := f.Close(); err != nil {
			return stagingPath, err
		}
	}

	cachePath := filepath.Join(m.root, cacheDir, key)
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return stagingPath, err
	}
	if err := os.Link(stagingPath, cachePath); err != nil {
		return stagingPath, err
	}
	return stagingPath, nil
}

func (m *finalizeTestCacheManager) removeStage(key string) error {
	err := os.Remove(filepath.Join(m.root, stagingDir, key))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (m *finalizeTestCacheManager) stats() (int64, int64) { return 0, 0 }

func (m *finalizeTestCacheManager) usedMemory() int64 { return 0 }

func (m *finalizeTestCacheManager) isEmpty() bool { return false }

func (m *finalizeTestCacheManager) getMetrics() *cacheManagerMetrics { return nil }

func TestWaitForUploadDrain_AlreadyDrained(t *testing.T) {
	tmpDir := t.TempDir()
	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys:   make(map[string]*pendingItem),
		bcache: &cacheManager{
			stores: []*cacheStore{{dir: tmpDir}},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WaitForUploadDrain(ctx); err != nil {
		t.Fatalf("WaitForUploadDrain should succeed, got %v", err)
	}
}

func TestWaitForUploadDrain_ContextCanceled(t *testing.T) {
	stagingPath := filepath.Join(t.TempDir(), "staging")
	if err := os.WriteFile(stagingPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile staging block: %v", err)
	}

	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			"k": {key: "k", fpath: stagingPath},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := store.WaitForUploadDrain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForUploadDrain should return context deadline, got %v", err)
	}
}

func TestWaitForUploadDrain_FailsWhenPendingStageMissing(t *testing.T) {
	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			"k": {key: "k", fpath: filepath.Join(t.TempDir(), "missing")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := store.WaitForUploadDrain(ctx)
	if !errors.Is(err, errPendingStagingMissing) {
		t.Fatalf("WaitForUploadDrain should return missing staging error, got %v", err)
	}
}

func TestWaitForUploadDrain_IgnoresMissingStageWhileUploading(t *testing.T) {
	item := &pendingItem{key: "k", fpath: filepath.Join(t.TempDir(), "missing")}
	item.uploading.Store(true)
	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			"k": item,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := store.WaitForUploadDrain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForUploadDrain should keep waiting while upload is in-flight, got %v", err)
	}
}

func TestWaitForUploadDrain_IgnoresMissingStageWhenCacheCopyStillExists(t *testing.T) {
	bcache := newFinalizeTestCacheManager(t, CsNone)
	store := &cachedStore{
		conf:          Config{Writeback: true, CacheChecksum: CsNone},
		currentUpload: make(chan struct{}, 1),
		pendingKeys:   make(map[string]*pendingItem),
		bcache:        bcache,
	}

	key := "chunks/0/0/123_0_4"
	stagingPath, err := store.bcache.stage(key, []byte("good"))
	if err != nil {
		t.Fatalf("stage block: %v", err)
	}
	store.pendingKeys[key] = &pendingItem{key: key, fpath: stagingPath}
	if err := os.Remove(stagingPath); err != nil {
		t.Fatalf("remove staging path: %v", err)
	}
	if _, ok := store.bcache.exist(key); !ok {
		t.Fatalf("cache copy should still exist for %s", key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = store.WaitForUploadDrain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForUploadDrain should keep waiting when cache copy still exists, got %v", err)
	}
}

func TestWaitForUploadDrain_FailsWhenCacheCopyExistsButCannotLoad(t *testing.T) {
	bcache := newFinalizeTestCacheManager(t, CsNone)
	store := &cachedStore{
		conf:          Config{Writeback: true, CacheChecksum: CsNone},
		currentUpload: make(chan struct{}, 1),
		pendingKeys:   make(map[string]*pendingItem),
		bcache:        bcache,
	}

	key := "chunks/0/0/123_0_4"
	stagingPath, err := store.bcache.stage(key, []byte("good"))
	if err != nil {
		t.Fatalf("stage block: %v", err)
	}
	store.pendingKeys[key] = &pendingItem{key: key, fpath: stagingPath}
	if err := os.Remove(stagingPath); err != nil {
		t.Fatalf("remove staging path: %v", err)
	}

	cacheDirPath, ok := store.bcache.exist(key)
	if !ok {
		t.Fatalf("cache copy should still exist for %s", key)
	}
	cachePath := filepath.Join(cacheDirPath, cacheDir, key)
	if err := os.WriteFile(cachePath, []byte("bad"), 0o644); err != nil {
		t.Fatalf("corrupt cache copy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = store.WaitForUploadDrain(ctx)
	if !errors.Is(err, errPendingStagingMissing) {
		t.Fatalf("WaitForUploadDrain should fail when cache copy cannot load, got %v", err)
	}
}

func TestWaitForUploadDrain_FailsWhenCacheCopyChecksumReadFails(t *testing.T) {
	bcache := newFinalizeTestCacheManager(t, CsFull)
	store := &cachedStore{
		conf:          Config{Writeback: true, CacheChecksum: CsFull},
		currentUpload: make(chan struct{}, 1),
		pendingKeys:   make(map[string]*pendingItem),
		bcache:        bcache,
	}

	key := "chunks/0/0/123_0_4"
	stagingPath, err := store.bcache.stage(key, []byte("good"))
	if err != nil {
		t.Fatalf("stage block: %v", err)
	}
	store.pendingKeys[key] = &pendingItem{key: key, fpath: stagingPath}
	if err := os.Remove(stagingPath); err != nil {
		t.Fatalf("remove staging path: %v", err)
	}

	cacheDirPath, ok := store.bcache.exist(key)
	if !ok {
		t.Fatalf("cache copy should still exist for %s", key)
	}
	cachePath := filepath.Join(cacheDirPath, cacheDir, key)
	f, err := os.OpenFile(cachePath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open cache copy: %v", err)
	}
	if _, err := f.WriteAt([]byte("x"), 0); err != nil {
		_ = f.Close()
		t.Fatalf("corrupt cache checksum: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close cache copy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = store.WaitForUploadDrain(ctx)
	if !errors.Is(err, errPendingStagingMissing) {
		t.Fatalf("WaitForUploadDrain should fail when cache copy checksum read fails, got %v", err)
	}
	if !strings.Contains(err.Error(), "data checksum") {
		t.Fatalf("WaitForUploadDrain should surface checksum failure, got %v", err)
	}
}

func TestWaitForUploadDrain_IgnoresStaleMissingStageSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	stagingPath := filepath.Join(tmpDir, "missing")
	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			"k": {key: "k", fpath: stagingPath},
		},
		bcache: &cacheManager{
			stores: []*cacheStore{{dir: tmpDir}},
		},
	}

	statStarted := make(chan struct{}, 1)
	allowStatReturn := make(chan struct{})
	origStat := statPathForUploadDrain
	statPathForUploadDrain = func(name string) (os.FileInfo, error) {
		if name == stagingPath {
			statStarted <- struct{}{}
			<-allowStatReturn
			return nil, os.ErrNotExist
		}
		return origStat(name)
	}
	t.Cleanup(func() {
		statPathForUploadDrain = origStat
		defer func() { _ = recover() }()
		close(allowStatReturn)
	})

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		errCh <- store.WaitForUploadDrain(ctx)
	}()

	<-statStarted
	store.removePending("k")
	close(allowStatReturn)

	if err := <-errCh; err != nil {
		t.Fatalf("WaitForUploadDrain should ignore stale missing-stage snapshot, got %v", err)
	}
}

func TestWaitForUploadDrain_WaitsForRawstagingEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	rawstaging := filepath.Join(tmpDir, stagingDir)
	if err := os.MkdirAll(rawstaging, 0o755); err != nil {
		t.Fatalf("MkdirAll rawstaging: %v", err)
	}
	blockPath := filepath.Join(rawstaging, "dummy")
	if err := os.WriteFile(blockPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile staging block: %v", err)
	}

	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys:   make(map[string]*pendingItem),
		bcache: &cacheManager{
			stores: []*cacheStore{{dir: tmpDir}},
		},
	}

	time.AfterFunc(300*time.Millisecond, func() {
		_ = os.Remove(blockPath)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.WaitForUploadDrain(ctx); err != nil {
		t.Fatalf("WaitForUploadDrain should succeed, got %v", err)
	}
}
