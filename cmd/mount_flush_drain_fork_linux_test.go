//go:build linux

package cmd

import (
	"context"
	"errors"
	"flag"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestFlushDrainConcurrentRequestsKeepSeparateResults(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	original := forkFlushDrainFlushAll
	forkFlushDrainFlushAll = func(*vfs.VFS) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return errors.New("first request flush failed")
		}
		return nil
	}
	defer func() { forkFlushDrainFlushAll = original }()
	store := &fakeFinalizeChunkStore{}
	stop, err := installForkFlushDrain(&vfs.VFS{
		Conf: &vfs.Config{Chunk: &chunk.Config{Writeback: true}}, Store: store,
	})
	require.NoError(t, err)
	defer stop()
	releaseFlush := sync.OnceFunc(func() { close(release) })
	defer releaseFlush()
	ticks, err := readProcStatStarttimeTicks(os.Getpid())
	require.NoError(t, err)
	socket := forkFlushDrainSocketPath(os.Getpid(), ticks)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := make(chan *forkFlushDrainResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := requestForkFlushDrain(ctx, socket)
		first <- result
		firstErr <- err
	}()
	<-started

	// A second connection must wait, not delete the first request's proof or
	// run another FlushAll concurrently. Expiry only cancels its own queue slot.
	queuedCtx, cancelQueued := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelQueued()
	queued, err := requestForkFlushDrain(queuedCtx, socket)
	// Either endpoint may observe the shared deadline first.
	if err != nil {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	} else {
		require.False(t, queued.Ok)
		require.Equal(t, "queue", queued.Ack.Phase)
		require.Equal(t, context.DeadlineExceeded.Error(), queued.Reason)
	}
	require.EqualValues(t, 1, calls.Load())

	second := make(chan *forkFlushDrainResult, 1)
	secondErr := make(chan error, 1)
	go func() {
		result, err := requestForkFlushDrain(ctx, socket)
		second <- result
		secondErr <- err
	}()
	releaseFlush()
	require.NoError(t, <-firstErr)
	failed := <-first
	require.False(t, failed.Ok)
	require.Equal(t, "first request flush failed", failed.Reason)
	require.Equal(t, "flush_all", failed.Ack.Phase)
	require.NoError(t, <-secondErr)
	success := <-second
	require.True(t, success.Ok)
	require.True(t, success.Drained)
	require.Equal(t, "ok", success.Ack.Status)
	require.NotEmpty(t, success.Ack.FinishedAt)
	require.EqualValues(t, 2, calls.Load())
	require.Equal(t, 1, store.waitCalls)
}

func TestFlushDrainCancellationDoesNotAbandonFlushAll(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	original := forkFlushDrainFlushAll
	forkFlushDrainFlushAll = func(*vfs.VFS) error {
		close(started)
		<-release
		return nil
	}
	defer func() { forkFlushDrainFlushAll = original }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *forkFlushDrainResult, 1)
	go func() { done <- runForkFlushDrain(ctx, &vfs.VFS{}) }()
	<-started
	cancel()
	require.False(t, forkMountLifecycle.TryLock(), "finalize must wait for FlushAll to actually finish")
	require.Len(t, forkMountPublish, 1, "cancellation must not release the publish fence early")
	select {
	case <-done:
		t.Fatal("abandoned FlushAll")
	default:
	}
	close(release)
	result := <-done
	require.False(t, result.Ok)
	require.Equal(t, "flush_all", result.Ack.Phase)
	require.Equal(t, context.Canceled.Error(), result.Reason)
	require.Empty(t, forkMountPublish)
	require.True(t, forkMountLifecycle.TryLock())
	forkMountLifecycle.Unlock()
}

func TestFlushDrainReportsUploadErrorAndRejectsFinalize(t *testing.T) {
	original := forkFlushDrainFlushAll
	forkFlushDrainFlushAll = func(*vfs.VFS) error { return nil }
	defer func() { forkFlushDrainFlushAll = original }()
	store := &fakeFinalizeChunkStore{waitErr: errors.New("object PUT failed")}
	result := runForkFlushDrain(context.Background(), &vfs.VFS{
		Conf: &vfs.Config{Chunk: &chunk.Config{Writeback: true}}, Store: store,
	})
	require.False(t, result.Ok)
	require.False(t, result.Drained)
	require.Equal(t, "upload_drain", result.Ack.Phase)
	require.Equal(t, "object PUT failed", result.Reason)

	forkMountLifecycle.Lock()
	result = runForkFlushDrain(context.Background(), nil)
	forkMountLifecycle.Unlock()
	require.False(t, result.Ok)
	require.Equal(t, "mount is finalizing", result.Reason)
}

func TestFlushDrainRejectsLegacyMountWithoutSignalling(t *testing.T) {
	original := readVFSConfigWithTimeout
	readVFSConfigWithTimeout = func(context.Context, string, time.Duration) (*vfs.Config, error) {
		return &vfs.Config{Pid: os.Getpid()}, nil
	}
	defer func() { readVFSConfigWithTimeout = original }()
	flags := flag.NewFlagSet("flush-drain", flag.ContinueOnError)
	flags.String("timeout", "1s", "")
	require.NoError(t, flags.Parse([]string{"/unused"}))
	result, exitCode := flushDrainRun(cli.NewContext(cli.NewApp(), flags, nil))
	require.Equal(t, 1, exitCode)
	require.False(t, result.Drained)
	require.Contains(t, result.Reason, "mount daemon upgrade required")
}
