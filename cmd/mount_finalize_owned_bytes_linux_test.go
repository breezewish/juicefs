//go:build linux

package cmd

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
)

type finalizeOwnedBytesCounter struct {
	fakeFinalizeSessionShutdowner
	wait     <-chan struct{}
	owned    *meta.Run9OwnedBytes
	closeErr error
}

func (c *finalizeOwnedBytesCounter) CloseSession() error {
	c.closeCalls++
	return c.closeErr
}

func (c *finalizeOwnedBytesCounter) Run9SettleOwnedBytes() *meta.Run9OwnedBytes {
	if c.wait != nil {
		<-c.wait
	}
	return c.owned
}

func TestFinalizeOwnedBytesPublishedOnlyAfterSuccessfulClose(t *testing.T) {
	start, err := readProcStatStarttimeTicks(os.Getpid())
	require.NoError(t, err)
	ackPath := forkFinalizeAckPath(os.Getpid(), start)
	t.Cleanup(func() { _ = os.Remove(ackPath) })
	origFlush, origMeasure := forkFinalizeFlushAll, forkFinalizeMeasureSize
	forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
	forkFinalizeMeasureSize = func(*vfs.VFS) (uint64, uint64, error) { return 0, 0, nil }
	t.Cleanup(func() { forkFinalizeFlushAll, forkFinalizeMeasureSize = origFlush, origMeasure })

	counter := &finalizeOwnedBytesCounter{}
	allocationStart := uint64(1 << 32)
	require.NoError(t, runForkFinalizeOnMain(counter, &vfs.VFS{Conf: &vfs.Config{}}, nil, &allocationStart))
	ack, err := readFinalizeAckFile(ackPath)
	require.NoError(t, err)
	require.Equal(t, "ok", ack.Status)
	require.Nil(t, ack.OwnedBytes)
	require.Equal(t, 1, counter.closeCalls)
	require.Equal(t, 1, counter.shutdownCalls)

	counter.owned = &meta.Run9OwnedBytes{VisibleBytes: 100, KeptForForksBytes: -50}
	require.NoError(t, runForkFinalizeOnMain(counter, &vfs.VFS{Conf: &vfs.Config{}}, nil, &allocationStart))
	ack, err = readFinalizeAckFile(ackPath)
	require.NoError(t, err)
	require.Equal(t, "ok", ack.Status)
	require.Equal(t, counter.owned, ack.OwnedBytes)
	require.Equal(t, 2, counter.closeCalls)
	require.Equal(t, 2, counter.shutdownCalls)

	counter.closeErr = errors.New("close session failed")
	require.ErrorIs(t, runForkFinalizeOnMain(counter, &vfs.VFS{Conf: &vfs.Config{}}, nil, &allocationStart), counter.closeErr)
	ack, err = readFinalizeAckFile(ackPath)
	require.NoError(t, err)
	require.Equal(t, "error", ack.Status)
	require.Equal(t, "close_session", ack.Phase)
	require.Nil(t, ack.OwnedBytes)
	require.Equal(t, 3, counter.closeCalls)
	require.Equal(t, 3, counter.shutdownCalls)
}

func TestFinalizeOwnedBytesTimeoutWritesAckWithoutClosingMetadata(t *testing.T) {
	// Diagnostics own stderr; isolate the test to verify the emitted evidence.
	if os.Getenv("JFS_TEST_FINALIZE_OWNED_TIMEOUT") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFinalizeOwnedBytesTimeoutWritesAckWithoutClosingMetadata$")
		child.Env = append(os.Environ(), "JFS_TEST_FINALIZE_OWNED_TIMEOUT=1")
		output, err := child.CombinedOutput()
		require.NoError(t, err, "%s", output)
		require.Contains(t, string(output), `"phase":"count_owned_bytes"`)
		require.Contains(t, string(output), `"error_reason":"deadline_exceeded"`)
		require.Contains(t, string(output), `"event":"juicefs_finalize_finished","status":"error","phase":"count_owned_bytes"`)
		return
	}

	synctest.Test(t, func(t *testing.T) {
		start, err := readProcStatStarttimeTicks(os.Getpid())
		require.NoError(t, err)
		ackPath := forkFinalizeAckPath(os.Getpid(), start)
		reqPath := forkFinalizeRequestPath(os.Getpid(), start)
		t.Cleanup(func() { _ = os.Remove(ackPath); _ = os.Remove(reqPath) })
		require.NoError(t, os.MkdirAll(filepath.Dir(ackPath), 0700))
		require.NoError(t, writeForkFinalizeRequest(reqPath, &forkFinalizeRequestV1{SchemaVersion: 1, FinalizeTimeout: "200ms"}))
		forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
		forkFinalizeMeasureSize = func(*vfs.VFS) (uint64, uint64, error) { return 0, 0, nil }
		release := make(chan struct{})
		counter := &finalizeOwnedBytesCounter{wait: release, owned: &meta.Run9OwnedBytes{VisibleBytes: 100}}
		allocationStart := uint64(1 << 32)
		began := time.Now()
		err = runForkFinalizeOnMain(counter, &vfs.VFS{Conf: &vfs.Config{}}, nil, &allocationStart)
		// Unblock and join even if an assertion fails, retaining race coverage.
		defer func() { close(release); synctest.Wait() }()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, forkFinalizeDaemonTimeout(200*time.Millisecond), time.Since(began))
		require.Zero(t, counter.closeCalls)
		require.Zero(t, counter.shutdownCalls)
		ack, err := readFinalizeAckFile(ackPath)
		require.NoError(t, err)
		require.Equal(t, "error", ack.Status)
		require.Equal(t, "count_owned_bytes", ack.Phase)
		require.Contains(t, ack.Error, "timeout after")
		require.NotEmpty(t, ack.FinishedAt)
		require.Nil(t, ack.OwnedBytes)
	})
}
