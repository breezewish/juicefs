package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// newRun9StartupMeta returns metadata whose file references one own slice of
// epoch 7, as a stopped Snap's metadata would.
func newRun9StartupMeta(t *testing.T) meta.Meta {
	m := meta.NewClient("badger://"+filepath.Join(t.TempDir(), "meta"), meta.DefaultConf())
	require.NoError(t, m.Init(&meta.Format{Name: "startup", Storage: "file", Bucket: t.TempDir() + "/", BlockSize: 4}, true))
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	var inode meta.Ino
	var attr meta.Attr
	require.Zero(t, m.Create(meta.Background(), meta.RootInode, "stopped", 0644, 0, 0, &inode, &attr))
	require.Zero(t, m.Write(meta.Background(), inode, 0, 0, meta.Slice{Id: 7<<32 + 1, Size: 100, Len: 100}, time.Now()))
	return m
}

// handRun9ForkProtectionPipe gives the mount the read end, as run9rt does.
func handRun9ForkProtectionPipe(t *testing.T) *os.File {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	fd, err := syscall.Dup(int(r.Fd()))
	require.NoError(t, err)
	require.NoError(t, r.Close())
	t.Setenv(run9ForkProtectionFDEnv, strconv.Itoa(fd))
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func requireStillWritable(t *testing.T, m meta.Meta, name string) {
	var inode meta.Ino
	var attr meta.Attr
	require.Zero(t, m.Create(meta.Background(), meta.RootInode, name, 0644, 0, 0, &inode, &attr))
	require.Zero(t, m.Close(meta.Background(), inode))
}

func TestOwnedBytesStartupDoesNotWaitForForkProtection(t *testing.T) {
	m := newRun9StartupMeta(t)
	w := handRun9ForkProtectionPipe(t)
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "slices"), encodeRun9ForkCheckpointSlices([]uint64{7<<32 + 1}), 0600))
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32+2))
	requireStillWritable(t, m, "before-protection")

	// run9rt delivers protection after the mount is already writable.
	require.NoError(t, json.NewEncoder(w).Encode(run9ForkProtectionRequest{ForkCheckpoints: []run9ForkCheckpoint{{Directory: directory, SourceAllocationEnd: 7<<32 + 2}}}))
	require.NoError(t, w.Close())
	counter := m.(interface {
		Run9WaitForkRecord(context.Context) error
		Run9SettleOwnedBytes(context.Context) *meta.Run9OwnedBytes
	})
	require.NoError(t, counter.Run9WaitForkRecord(t.Context()))
	// The stopped file's slice belongs to the Fork, not to the visible bytes.
	require.Equal(t, &meta.Run9OwnedBytes{}, counter.Run9SettleOwnedBytes(context.Background()))
}

// Without an inherited pipe, as from an older run9rt, the request's own
// protection applies; it is never treated as protecting nothing.
func TestOwnedBytesStartupWithoutPipeUsesTheRequestProtection(t *testing.T) {
	m := newRun9StartupMeta(t)
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "slices"), encodeRun9ForkCheckpointSlices([]uint64{7<<32 + 1}), 0600))
	request := filepath.Join(t.TempDir(), "request.json")
	require.NoError(t, os.WriteFile(request, []byte(`{"fork_checkpoints":[{"directory":"`+directory+`","source_allocation_end":30064771074}],"basis":3}`), 0600))
	t.Setenv("JFS_RUN9_OWNED_BYTES_REQUEST", request)
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32+2))
	counter := m.(interface {
		Run9WaitForkRecord(context.Context) error
		Run9SettleOwnedBytes(context.Context) *meta.Run9OwnedBytes
	})
	require.NoError(t, counter.Run9WaitForkRecord(t.Context()))
	require.Equal(t, &meta.Run9OwnedBytes{}, counter.Run9SettleOwnedBytes(context.Background()))
}

func TestOwnedBytesStartupWithoutProtectionLeavesTheCountUnavailable(t *testing.T) {
	m := newRun9StartupMeta(t)
	w := handRun9ForkProtectionPipe(t)
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32+2))
	// run9rt exited without a complete request: protection is not assumed empty.
	require.NoError(t, w.Close())
	counter := m.(interface {
		Run9WaitForkRecord(context.Context) error
		Run9SettleOwnedBytes(context.Context) *meta.Run9OwnedBytes
	})
	// Initialization has ended before finalize, so it was not cancelled.
	require.NoError(t, counter.Run9WaitForkRecord(t.Context()))
	requireStillWritable(t, m, "after-failure")
	require.Nil(t, counter.Run9SettleOwnedBytes(context.Background()))
}

func TestOwnedBytesStartupBudgetDoesNotFailTheWritableMount(t *testing.T) {
	old := meta.Run9OwnedInitBudget
	meta.Run9OwnedInitBudget = -time.Second
	t.Cleanup(func() { meta.Run9OwnedInitBudget = old }) // After the worker stops.
	m := newRun9StartupMeta(t)
	handRun9ForkProtectionPipe(t)

	// Expiry is deterministic, including on a machine with an empty page cache.
	// It must not turn a missing optional count into mount failure or valid zero.
	require.NoError(t, beginRun9OwnedBytes(m, 7, 7<<32+2))
	requireStillWritable(t, m, "still-writable")
	counter := m.(interface {
		Run9SettleOwnedBytes(context.Context) *meta.Run9OwnedBytes
	})
	require.Nil(t, counter.Run9SettleOwnedBytes(context.Background()))
}
