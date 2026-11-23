//go:build linux && !nobadger

package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// A capture waits for the capture or flush in progress instead of failing:
// the coordinator resumes a VM before its source has reopened.
func TestRun9CaptureWaitsForTheCaptureInProgress(t *testing.T) {
	forkMountPublish <- struct{}{}
	defer func() { <-forkMountPublish }()
	server, client := net.Pipe()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	finished := make(chan error, 1)
	go func() {
		finished <- serveRun9Capture(context.Background(), server, json.NewEncoder(server), nil, false)
	}()
	require.NoError(t, json.NewEncoder(client).Encode(run9CaptureRequest{
		Directory: "/unused", DeadlineUnixMS: time.Now().Add(5 * time.Second).UnixMilli(),
	}))
	select {
	case err := <-finished:
		t.Fatalf("capture did not wait: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// The client going away cancels the wait.
	require.NoError(t, client.Close())
	require.ErrorIs(t, <-finished, context.Canceled)
}

// Each socket serves exactly its own protocol.
func TestRun9CaptureProtocolsRejectEachOthersRequests(t *testing.T) {
	serve := func(current bool, request run9CaptureRequest) string {
		server, client := net.Pipe()
		defer client.Close()
		require.NoError(t, client.SetDeadline(time.Now().Add(time.Second)))
		go func() { _ = serveRun9Capture(context.Background(), server, json.NewEncoder(server), nil, current) }()
		require.NoError(t, json.NewEncoder(client).Encode(request))
		var response run9CaptureResponse
		require.NoError(t, json.NewDecoder(client).Decode(&response))
		require.Equal(t, "error", response.Phase)
		return response.Error
	}
	deadline := time.Now().Add(time.Second).UnixMilli()
	require.Equal(t, "capture requires copies and takes no directory",
		serve(true, run9CaptureRequest{Directory: "/copy", DeadlineUnixMS: deadline}))
	require.Equal(t, "capture copy requires a directory and a checkpoint directory",
		serve(true, run9CaptureRequest{Copies: []run9CaptureCopy{{Directory: "/copy"}}, DeadlineUnixMS: deadline}))
	require.Equal(t, "legacy capture takes one directory",
		serve(false, run9CaptureRequest{Copies: []run9CaptureCopy{{Directory: "/copy", CheckpointDirectory: "/checkpoint"}}, DeadlineUnixMS: deadline}))
}

// reopenFailingMeta corrupts the closed source database once its copies
// exist, so the reopen that follows fails, and holds that reopen until
// reopen is closed.
type reopenFailingMeta struct {
	meta.Meta
	source string
	reopen chan struct{}
}

func (m reopenFailingMeta) Run9WaitForkRecord(ctx context.Context) error {
	return m.Meta.(run9CaptureMeta).Run9WaitForkRecord(ctx)
}

func (m reopenFailingMeta) Run9CaptureMetadata(ctx context.Context, directory string, opts meta.Run9CaptureOptions) (meta.Run9MetadataCapture, *meta.Run9ForkRecord, error) {
	cloned := opts.Cloned
	opts.Cloned = func(capture meta.Run9MetadataCapture) {
		cloned(capture)
		if err := os.WriteFile(filepath.Join(m.source, "MANIFEST"), []byte("corrupt"), 0600); err != nil {
			panic(err)
		}
		<-m.reopen
	}
	return m.Meta.(run9CaptureMeta).Run9CaptureMetadata(ctx, directory, opts)
}

// gatedPuts holds every object upload until release is closed.
type gatedPuts struct {
	object.ObjectStorage
	release chan struct{}
}

func (s gatedPuts) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	<-s.release
	return s.ObjectStorage.Put(ctx, key, in, getters...)
}

// The current protocol reports every child of one capture ready from their
// copies and the fixed upload set alone, while the source still reopens. A
// source that then fails to reopen reports its own error after that, and
// keeps writes suspended, so that nothing is acknowledged that the lost
// mount would drop.
func TestRun9CaptureReportsReadyBeforeSourceReopenFailure(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	metaConf := meta.DefaultConf()
	metaConf.NoBGJob, metaConf.MaxDeletes = true, 0
	m := meta.NewClient("badger://"+source, metaConf)
	format := &meta.Format{Name: "capture", UUID: "capture-test", Storage: "mem", BlockSize: 4096}
	require.NoError(t, m.Init(format, false))
	require.NoError(t, m.NewSession(true))
	blob, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	uploads := make(chan struct{})
	chunkConf := chunk.Config{BlockSize: format.BlockSize << 10, MaxUpload: 1, BufferSize: 30 << 20, CacheDir: t.TempDir(), CacheMode: 0600, CacheSize: 10 << 20,
		Writeback: true, WritebackThresholdSize: format.BlockSize<<10 + 1}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(gatedPuts{ObjectStorage: blob, release: uploads}, chunkConf, registry)
	reopen := make(chan struct{})
	v := vfs.NewVFS(&vfs.Config{Meta: metaConf, Format: *format, Chunk: &chunkConf, FuseOpts: &vfs.FuseOptions{}},
		reopenFailingMeta{Meta: m, source: source, reopen: reopen}, store, registry, registry)
	ctx := vfs.NewLogContext(meta.Background())
	file, fh, st := v.Create(ctx, meta.RootInode, "pending", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	require.Zero(t, v.Write(ctx, file.Inode, []byte("pending upload"), 0, fh))

	server, client := net.Pipe()
	defer client.Close()
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	finished := make(chan error, 1)
	go func() { finished <- serveRun9Capture(context.Background(), server, json.NewEncoder(server), v, true) }()
	copies := []run9CaptureCopy{
		{Directory: filepath.Join(root, "child-a"), CheckpointDirectory: filepath.Join(root, "checkpoint-a")},
		{Directory: filepath.Join(root, "child-b"), CheckpointDirectory: filepath.Join(root, "checkpoint-b")},
	}
	require.NoError(t, json.NewEncoder(client).Encode(run9CaptureRequest{Copies: copies, DeadlineUnixMS: time.Now().Add(10 * time.Second).UnixMilli()}))
	decoder := json.NewDecoder(client)
	var captured, ready, failure run9CaptureResponse
	require.NoError(t, decoder.Decode(&captured))
	require.Equal(t, "captured", captured.Phase)
	require.NotNil(t, captured.Usage)
	require.Equal(t, 1, captured.Uploads.Blocks, "the written block is still uploading")
	for _, c := range copies {
		require.DirExists(t, c.Directory)
		require.DirExists(t, c.CheckpointDirectory)
	}
	// The source has not reopened: ready follows the uploads alone.
	close(uploads)
	require.NoError(t, decoder.Decode(&ready))
	require.Equal(t, "ready", ready.Phase)
	close(reopen)
	require.NoError(t, decoder.Decode(&failure))
	require.Equal(t, "error", failure.Phase)
	require.Equal(t, "source_unavailable", failure.Code)
	require.ErrorIs(t, <-finished, meta.ErrRun9MetadataUnavailable)
	written := make(chan syscall.Errno, 1)
	go func() { written <- v.Write(ctx, file.Inode, []byte("after loss"), 0, fh) }()
	select {
	case st := <-written:
		t.Fatalf("a write after the source was lost returned %v", st)
	case <-time.After(200 * time.Millisecond):
	}
}
