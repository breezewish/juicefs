package chunk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
