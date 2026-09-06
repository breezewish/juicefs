//go:build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
)

type retiredFinalizeMetadata struct {
	closed      bool
	shutdownErr error
	scanErr     error
}

func (m *retiredFinalizeMetadata) CloseSession() error { m.closed = true; return nil }
func (m *retiredFinalizeMetadata) Shutdown() error     { return m.shutdownErr }
func (m *retiredFinalizeMetadata) Run9SliceAllocationCounter() (uint64, error) {
	return 1<<32 + 4096, nil
}
func (m *retiredFinalizeMetadata) Run9RetiredSlices(context.Context, uint64, uint64, int) ([]meta.Slice, bool, error) {
	if !m.closed {
		return nil, false, errors.New("scan before close session")
	}
	return []meta.Slice{{Id: 1 << 32, Size: 4096}}, false, m.scanErr
}

func TestRun9RetiredSlicesFinalizePublishesOnlyAfterSuccess(t *testing.T) {
	flush, measure := forkFinalizeFlushAll, forkFinalizeMeasureSize
	forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
	forkFinalizeMeasureSize = func(*vfs.VFS) (uint64, uint64, error) { return 0, 0, nil }
	t.Cleanup(func() { forkFinalizeFlushAll = flush; forkFinalizeMeasureSize = measure })
	start, err := readProcStatStarttimeTicks(os.Getpid())
	require.NoError(t, err)
	ackPath := forkFinalizeAckPath(os.Getpid(), start)
	t.Cleanup(func() { _ = os.Remove(ackPath) })
	m := &retiredFinalizeMetadata{}
	gc := &run9RetiredSliceGC{metadata: m, dir: t.TempDir(), batch: run9RetiredSliceBatch{
		Version: 1, Format: "test", Storage: run9ObjectStorageDescriptor{Storage: "file", Bucket: t.TempDir() + "/", UUID: "uuid"},
		Layout: run9ObjectLayout{BlockSizeBytes: 4096}, OwnedEpoch: 1, Start: 1 << 32,
	}}
	require.NoError(t, runForkFinalizeOnMain(m, &vfs.VFS{Conf: &vfs.Config{}}, nil, gc))
	entries, err := os.ReadDir(gc.dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	raw, err := os.ReadFile(ackPath)
	require.NoError(t, err)
	var ack forkFinalizeAckV1
	require.NoError(t, json.Unmarshal(raw, &ack))
	require.Equal(t, "ok", ack.Status)
	require.Equal(t, 1, ack.RetiredSlices)

	gc.dir = t.TempDir()
	gc.batch.Slices = nil
	m.shutdownErr = errors.New("failed metadata persistence")
	require.ErrorContains(t, runForkFinalizeOnMain(m, &vfs.VFS{Conf: &vfs.Config{}}, nil, gc), "failed metadata persistence")
	entries, err = os.ReadDir(gc.dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	gc.batch.Slices = nil
	m.shutdownErr = nil
	m.scanErr = errors.New("invalid reference record")
	require.NoError(t, runForkFinalizeOnMain(m, &vfs.VFS{Conf: &vfs.Config{}}, nil, gc))
	entries, err = os.ReadDir(gc.dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	raw, err = os.ReadFile(ackPath)
	require.NoError(t, err)
	ack = forkFinalizeAckV1{}
	require.NoError(t, json.Unmarshal(raw, &ack))
	require.Equal(t, "ok", ack.Status)
	require.Contains(t, ack.SliceGCError, "invalid reference record")
}
