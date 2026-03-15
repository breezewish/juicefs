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
	store := &cachedStore{
		conf:          Config{Writeback: true},
		currentUpload: make(chan struct{}, 1),
		pendingKeys: map[string]*pendingItem{
			"k": {},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := store.WaitForUploadDrain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForUploadDrain should return context deadline, got %v", err)
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
