package chunk

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestPendingUploadsRequireSuccessfulPUT(t *testing.T) {
	backend, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	storage := &cleanCacheUploadStorage{ObjectStorage: backend, entered: make(chan struct{}, 1), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(storage.release) })
	defer release()
	conf := defaultConf
	conf.CacheDir, conf.Writeback = t.TempDir(), true
	conf.WritebackThresholdSize = conf.BlockSize + 1
	store := NewCachedStore(storage, conf, nil).(*cachedStore)
	payload := bytes.Repeat([]byte("captured"), 512)
	writer := store.NewWriter(999)
	_, err = writer.WriteAt(payload, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Finish(len(payload)))
	<-storage.entered
	fence := store.CaptureUploads()
	require.Equal(t, 1, fence.Blocks)
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, fence.Wait(deadline), context.DeadlineExceeded)
	key := FormatObjectBlockKey(999, 0, uint64(len(payload)), false)
	_, err = backend.Get(context.Background(), key, 0, -1)
	require.Error(t, err, "local staging is not remote availability")
	release()
	deadline, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, fence.Wait(deadline))
	reader, err := backend.Get(context.Background(), key, 0, -1)
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, body)
}

func TestPendingUploadsExcludeLaterWrites(t *testing.T) {
	store := &cachedStore{}
	store.uploads.start("captured", 1024)
	fence := store.CaptureUploads()
	require.Equal(t, 1, fence.Blocks)
	require.Equal(t, int64(1024), fence.Bytes)
	store.uploads.start("parent-later", 2048)
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, fence.Wait(deadline), context.DeadlineExceeded)
	store.uploads.complete("captured")
	require.NoError(t, fence.Wait(context.Background()))
	later := store.CaptureUploads()
	require.Equal(t, int64(2048), later.Bytes, "a completed snapshot must not wait for later parent writes")
	store.uploads.abandon("parent-later")
	require.ErrorContains(t, later.Wait(context.Background()), "abandoned")
	require.Zero(t, store.CaptureUploads().Blocks)
}

func TestPendingUploadsCompleteAfterRemoteRecovery(t *testing.T) {
	backend, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	conf := defaultConf
	conf.CacheDir, conf.Writeback, conf.UploadDelay = t.TempDir(), true, time.Hour
	store := NewCachedStore(backend, conf, nil).(*cachedStore)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	missing := filepath.Join(t.TempDir(), "missing")

	// Finalize can prove that a missing staged object was already uploaded.
	first := "chunks/0/0/123_0_4"
	require.NoError(t, backend.Put(ctx, first, bytes.NewReader([]byte("good"))))
	store.pendingMutex.Lock()
	store.pendingKeys[first] = &pendingItem{key: first, fpath: missing, ts: time.Now()}
	store.pendingMutex.Unlock()
	store.uploads.start(first, 4)
	captured := store.CaptureUploads()
	require.NoError(t, store.pendingBlockRecoveryError(ctx, first, missing))
	require.NoError(t, captured.Wait(ctx))
	require.Zero(t, store.CaptureUploads().Blocks)

	// The ordinary background uploader has the same recovery path.
	second := "chunks/0/0/124_0_4"
	require.NoError(t, backend.Put(ctx, second, bytes.NewReader([]byte("good"))))
	store.pendingMutex.Lock()
	store.pendingKeys[second] = &pendingItem{key: second, fpath: missing, ts: time.Now()}
	store.pendingMutex.Unlock()
	store.uploads.start(second, 4)
	captured = store.CaptureUploads()
	store.uploadStagingFile(second, missing)
	require.NoError(t, captured.Wait(ctx))
	require.Zero(t, store.CaptureUploads().Blocks)
}
