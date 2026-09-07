//go:build !nobadger

package meta

import (
	"math"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9EmptyFilesystemPersistsDirectoryStatsAndEpoch(t *testing.T) {
	dir := t.TempDir()
	m, err := newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	require.NoError(t, m.Init(testFormat(), false))
	require.NoError(t, InitRun9EmptyFilesystem(m, 7, 1001, 1002))
	require.ErrorContains(t, InitRun9EmptyFilesystem(m, 8, 1001, 1002), "fresh metadata")
	require.NoError(t, m.Shutdown())

	// Reopen after the SkipWAL close fence: checking only the original client's
	// in-memory state would miss lost directory and filesystem accounting.
	m, err = newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	_, err = m.Load(true)
	require.NoError(t, err)
	var inode Ino
	var attr Attr
	require.Zero(t, m.Lookup(Background(), RootInode, "rootfs", &inode, &attr, false))
	require.Equal(t, Ino(2), inode)
	require.Equal(t, uint8(TypeDirectory), attr.Typ)
	require.Equal(t, uint16(0755), attr.Mode)
	require.Equal(t, uint32(1001), attr.Uid)
	require.Equal(t, uint32(1002), attr.Gid)
	require.Equal(t, uint64(4096), attr.Length)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Equal(t, RootInode, attr.Parent)
	require.Zero(t, m.GetAttr(Background(), RootInode, &attr))
	require.Equal(t, uint32(3), attr.Nlink)
	var total, available, used, free uint64
	require.Zero(t, m.StatFS(Background(), RootInode, &total, &available, &used, &free))
	require.Equal(t, uint64(4096), total-available)
	require.Equal(t, uint64(1), used)
	kv := m.(*kvMeta)
	rootStats, st := kv.doGetDirStat(Background(), RootInode, false)
	require.Zero(t, st)
	require.Equal(t, &dirStat{space: 4096, inodes: 1}, rootStats)
	childStats, st := kv.doGetDirStat(Background(), inode, false)
	require.Zero(t, st)
	require.Equal(t, &dirStat{}, childStats)
	next, err := kv.getCounter("nextChunk")
	require.NoError(t, err)
	require.Equal(t, int64(7<<32), next)
	limit, err := kv.getCounter("run9NextChunkLimit")
	require.NoError(t, err)
	require.Equal(t, int64(8<<32), limit)
	var slice uint64
	require.Zero(t, m.NewSlice(Background(), &slice))
	require.GreaterOrEqual(t, slice, uint64(7<<32))
	require.Less(t, slice, uint64(8<<32))
	var child Ino
	require.Zero(t, m.Mkdir(Background(), inode, "child", 0755, 0, 0, &child, &attr))
	require.Greater(t, child, inode, "native allocation must not reuse rootfs inode")
	m.FlushSession()
}

func TestRun9EmptyFilesystemRejectsUsedMetadataAndInvalidEpoch(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	require.Error(t, InitRun9EmptyFilesystem(m, 0, 0, 0))
	require.Error(t, InitRun9EmptyFilesystem(m, uint64(math.MaxInt64>>32)+1, 0, 0))
	var inode Ino
	var attr Attr
	require.Zero(t, m.Mkdir(Background(), RootInode, "existing", 0755, 0, 0, &inode, &attr))
	require.ErrorContains(t, InitRun9EmptyFilesystem(m, 7, 0, 0), "fresh metadata")
	require.Equal(t, syscall.ENOENT, m.Lookup(Background(), RootInode, "rootfs", &inode, &attr, false))
	m.FlushSession()
}

func TestRun9EmptyFilesystemMatchesNativeMkdirAccounting(t *testing.T) {
	native, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, native.Shutdown()) })
	require.NoError(t, native.Init(testFormat(), false))
	var inode Ino
	var nativeAttr Attr
	require.Zero(t, native.Mkdir(Background(), RootInode, "rootfs", 0755, 0, 0, &inode, &nativeAttr))
	native.FlushSession()

	fast, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fast.Shutdown()) })
	require.NoError(t, fast.Init(testFormat(), false))
	require.NoError(t, InitRun9EmptyFilesystem(fast, 7, 0, 0))
	var fastAttr Attr
	require.Zero(t, fast.Lookup(Background(), RootInode, "rootfs", &inode, &fastAttr, false))
	require.Equal(t, nativeAttr.Typ, fastAttr.Typ)
	require.Equal(t, nativeAttr.Mode, fastAttr.Mode)
	require.Equal(t, nativeAttr.Uid, fastAttr.Uid)
	require.Equal(t, nativeAttr.Gid, fastAttr.Gid)
	require.Equal(t, nativeAttr.Parent, fastAttr.Parent)
	require.Equal(t, nativeAttr.Nlink, fastAttr.Nlink)
	require.Equal(t, nativeAttr.Length, fastAttr.Length)
	nativeKV, fastKV := native.(*kvMeta), fast.(*kvMeta)
	nativeStats, st := nativeKV.doGetDirStat(Background(), RootInode, false)
	require.Zero(t, st)
	fastStats, st := fastKV.doGetDirStat(Background(), RootInode, false)
	require.Zero(t, st)
	require.Equal(t, nativeStats, fastStats)
	nativeSpace, err := nativeKV.getCounter(usedSpace)
	require.NoError(t, err)
	fastSpace, err := fastKV.getCounter(usedSpace)
	require.NoError(t, err)
	require.Equal(t, nativeSpace, fastSpace)
	nativeInodes, err := nativeKV.getCounter(totalInodes)
	require.NoError(t, err)
	fastInodes, err := fastKV.getCounter(totalInodes)
	require.NoError(t, err)
	require.Equal(t, nativeInodes, fastInodes)
}
