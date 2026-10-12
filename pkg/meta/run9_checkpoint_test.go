//go:build !nobadger

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9CheckpointEffectiveReferencesAndCorruption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	client, err := newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	f := testFormat()
	f.TrashDays = 0
	require.NoError(t, m.Init(f, false))
	require.NoError(t, m.client.txn(ctx, func(tx *kvTxn) error {
		tx.set(m.inodeKey(2), (&Attr{Typ: TypeFile, Nlink: 1, Length: 200}).Marshal())
		// Full overwrite removes 10; partial overwrite retains both 11 and 12.
		ss := append(marshalSlice(0, 10, 200, 0, 200), marshalSlice(0, 11, 200, 0, 200)...)
		ss = append(ss, marshalSlice(50, 12, 50, 0, 50)...)
		tx.set(m.chunkKey(2, 0), ss)
		// Deleted but not yet cleaned up inode and unlinked-open inode are unreachable.
		tx.set(m.chunkKey(3, 0), marshalSlice(0, 13, 200, 0, 200))
		tx.set(m.inodeKey(4), (&Attr{Typ: TypeFile, Nlink: 0, Length: 200}).Marshal())
		tx.set(m.chunkKey(4, 0), marshalSlice(0, 14, 200, 0, 200))
		return nil
	}, 0))
	require.NoError(t, m.Shutdown())
	reader, err := OpenRun9Checkpoint(dir)
	require.NoError(t, err)
	ids, err := reader.Run9CheckpointSlices(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 12}, ids)
	require.NoError(t, reader.Shutdown())
	client, err = newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	m = client.(*kvMeta)
	require.NoError(t, m.client.txn(ctx, func(tx *kvTxn) error { tx.set(m.chunkKey(5, 0), []byte{1}); return nil }, 0))
	require.NoError(t, m.Shutdown())
	reader, err = OpenRun9Checkpoint(dir)
	require.NoError(t, err)
	defer reader.Shutdown()
	ids, err = reader.Run9CheckpointSlices(ctx)
	require.ErrorContains(t, err, "malformed")
	require.Nil(t, ids)
}

// A clone is a new name even when its source was already unlinked and only held
// open. The scan skips files without links, so the clone must have one.
func TestRun9CheckpointRetainsCloneOfUnlinkedOpenFile(t *testing.T) {
	ctx := Background()
	dir := t.TempDir()
	client, err := newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	m := client.(*kvMeta)
	f := testFormat()
	f.TrashDays = 0
	require.NoError(t, m.Init(f, false))
	require.NoError(t, m.client.txn(ctx, func(tx *kvTxn) error {
		tx.set(m.inodeKey(2), (&Attr{Typ: TypeFile, Mode: 0644, Nlink: 0, Length: 200}).Marshal())
		tx.set(m.chunkKey(2, 0), marshalSlice(0, 10, 200, 0, 200))
		return nil
	}, 0))
	var count, total uint64
	require.Zero(t, m.Clone(ctx, RootInode, 2, RootInode, "clone", 0, 0, 1, &count, &total))
	var clone Ino
	var attr Attr
	require.Zero(t, m.Lookup(ctx, RootInode, "clone", &clone, &attr, false))
	require.Equal(t, uint32(1), attr.Nlink)
	require.NoError(t, m.Shutdown())
	reader, err := OpenRun9Checkpoint(dir)
	require.NoError(t, err)
	ids, err := reader.Run9CheckpointSlices(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{10}, ids)
	require.NoError(t, reader.Shutdown())
}

func TestRun9CheckpointAllocationEndIncludesUnusedReservations(t *testing.T) {
	m := &baseMeta{}
	require.Equal(t, uint64(123), m.Run9SliceAllocationEnd(123))
	m.freeSlices = freeID{next: 124, maxid: 4096}
	require.Equal(t, uint64(4096), m.Run9SliceAllocationEnd(123))
}
