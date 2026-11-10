//go:build !nobadger

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun9PrepareForkCleansSessionsAndReportsUsage(t *testing.T) {
	directory := t.TempDir()
	conf := testConfig()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	source, err := newKVMeta("badger", directory, conf)
	require.NoError(t, err)
	require.NoError(t, source.Init(testFormat(), false))
	require.NoError(t, source.NewSession(true))
	ctx := Background()
	write := func(name string, size uint32) Ino {
		var inode Ino
		var attr Attr
		require.Zero(t, source.Create(ctx, RootInode, name, 0644, 022, 0, &inode, &attr))
		var id uint64
		require.Zero(t, source.NewSlice(ctx, &id))
		require.Zero(t, source.Write(ctx, inode, 0, 0, Slice{Id: id, Size: size, Len: size}, time.Now()))
		return inode
	}
	write("kept", 4096)
	held := write("held-open", 8192)
	var attr Attr
	require.Zero(t, source.Open(ctx, held, 2, &attr))
	require.Zero(t, source.Unlink(ctx, RootInode, "held-open"))
	// The copy is taken while the source session still holds the unlinked file.
	source.(*kvMeta).doFlushStats()
	require.NoError(t, source.(*kvMeta).client.(*badgerClient).client.Close())

	prepared, err := Run9PrepareFork(context.Background(), directory)
	require.NoError(t, err)
	require.Equal(t, Run9PreparedFork{UsedSpace: 4096, UsedInodes: 1}, prepared)

	child, err := newKVMeta("badger", directory+"?readonly=1", conf)
	require.NoError(t, err)
	defer func() { require.NoError(t, child.Shutdown()) }()
	sessions, err := child.ListSessions()
	require.NoError(t, err)
	require.Empty(t, sessions)
	_, err = child.Load(true)
	require.NoError(t, err)
	var total, available, usedInodes, availableInodes uint64
	require.Zero(t, child.StatFS(Background(), RootInode, &total, &available, &usedInodes, &availableInodes))
	require.Equal(t, prepared, Run9PreparedFork{UsedSpace: total - available, UsedInodes: usedInodes}, "the child keeps the usage that was reported")
}
