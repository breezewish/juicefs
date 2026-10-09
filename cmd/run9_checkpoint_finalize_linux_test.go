//go:build linux

package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
)

type checkpointFinalizeMetadata struct{ shutdownErr error }

func (m *checkpointFinalizeMetadata) CloseSession() error { return nil }
func (m *checkpointFinalizeMetadata) Shutdown() error     { return m.shutdownErr }

func (m *checkpointFinalizeMetadata) Run9SliceAllocationEnd(initial uint64) uint64 { return initial }

func TestRun9CheckpointFinalizeOnlyReturnsAllocationProof(t *testing.T) {
	flush, measure := forkFinalizeFlushAll, forkFinalizeMeasureSize
	forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
	forkFinalizeMeasureSize = func(*vfs.VFS) (uint64, uint64, error) { return 0, 0, nil }
	t.Cleanup(func() { forkFinalizeFlushAll = flush; forkFinalizeMeasureSize = measure })
	pidStart, err := readProcStatStarttimeTicks(os.Getpid())
	require.NoError(t, err)
	ackPath := forkFinalizeAckPath(os.Getpid(), pidStart)
	t.Cleanup(func() { _ = os.Remove(ackPath) })
	m := &checkpointFinalizeMetadata{}
	start := uint64(1 << 32)
	// This fixture has no scan/counter method: finalize must need neither.
	require.NoError(t, runForkFinalizeOnMain(m, &vfs.VFS{Conf: &vfs.Config{}}, nil, &start))
	raw, err := os.ReadFile(ackPath)
	require.NoError(t, err)
	var ack forkFinalizeAckV1
	require.NoError(t, json.Unmarshal(raw, &ack))
	require.Equal(t, "ok", ack.Status)
	require.Equal(t, &start, ack.SliceAllocationEnd)
	m.shutdownErr = errors.New("failed persistence")
	require.ErrorContains(t, runForkFinalizeOnMain(m, &vfs.VFS{Conf: &vfs.Config{}}, nil, &start), "failed persistence")
	raw, err = os.ReadFile(ackPath)
	require.NoError(t, err)
	ack = forkFinalizeAckV1{}
	require.NoError(t, json.Unmarshal(raw, &ack))
	require.Nil(t, ack.SliceAllocationEnd)
}
