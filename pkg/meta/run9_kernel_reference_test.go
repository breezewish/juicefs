//go:build !nobadger

package meta

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func newRun9KernelReferenceMeta(t *testing.T) Meta {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	require.NoError(t, m.NewSession(true))
	return m
}

func sustainedInodes(t *testing.T, m Meta) []Ino {
	s, err := m.GetSession(m.getBase().sid, true)
	require.NoError(t, err)
	return s.Sustained
}

func TestRun9KernelReferenceKeepsUnlinkedFileUntilForget(t *testing.T) {
	m := newRun9KernelReferenceMeta(t)
	ctx := Background()
	var inode Ino
	var attr Attr
	require.Zero(t, m.Create(ctx, RootInode, "status", 0644, 022, 0, &inode, &attr))
	require.Zero(t, m.Close(ctx, inode))
	m.Reference(inode)
	m.Reference(inode)

	require.Zero(t, m.Unlink(ctx, RootInode, "status"))
	require.Zero(t, m.GetAttr(ctx, inode, &attr))
	require.Equal(t, uint32(0), attr.Nlink)
	require.Equal(t, []Ino{inode}, sustainedInodes(t, m))
	require.Zero(t, m.Open(ctx, inode, syscall.O_RDONLY, &attr))
	require.Zero(t, m.Close(ctx, inode))

	m.Forget(inode, 1)
	require.Zero(t, m.GetAttr(ctx, inode, &attr))
	m.Forget(inode, 1)
	require.Equal(t, syscall.ENOENT, m.GetAttr(ctx, inode, &attr))
	require.Empty(t, sustainedInodes(t, m))
}

func TestRun9OpenHandleOutlivesForget(t *testing.T) {
	m := newRun9KernelReferenceMeta(t)
	ctx := Background()
	var inode Ino
	var attr Attr
	require.Zero(t, m.Create(ctx, RootInode, "status", 0644, 022, 0, &inode, &attr))
	m.Reference(inode)

	require.Zero(t, m.Unlink(ctx, RootInode, "status"))
	m.Forget(inode, 1)
	require.Zero(t, m.GetAttr(ctx, inode, &attr))
	require.Zero(t, m.Close(ctx, inode))
	require.Equal(t, syscall.ENOENT, m.GetAttr(ctx, inode, &attr))
	require.Empty(t, sustainedInodes(t, m))
}

func TestRun9RenameOverReferencedFileKeepsReplacedInode(t *testing.T) {
	m := newRun9KernelReferenceMeta(t)
	ctx := Background()
	var replaced, source Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "status", TypeFile, 0644, 022, 0, "", &replaced, &attr))
	require.Zero(t, m.Mknod(ctx, RootInode, "status-new", TypeFile, 0644, 022, 0, "", &source, &attr))
	m.Reference(replaced)

	require.Zero(t, m.Rename(ctx, RootInode, "status-new", RootInode, "status", 0, nil, nil))
	require.Zero(t, m.GetAttr(ctx, replaced, &attr))
	require.Equal(t, uint32(0), attr.Nlink)
	m.Forget(replaced, 1)
	require.Equal(t, syscall.ENOENT, m.GetAttr(ctx, replaced, &attr))
}

func TestRun9UnreferencedUnlinkDeletesImmediately(t *testing.T) {
	m := newRun9KernelReferenceMeta(t)
	ctx := Background()
	var inode Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "status", TypeFile, 0644, 022, 0, "", &inode, &attr))
	m.Reference(inode)
	m.Forget(inode, 1)

	require.Zero(t, m.Unlink(ctx, RootInode, "status"))
	require.Equal(t, syscall.ENOENT, m.GetAttr(ctx, inode, &attr))
	require.Empty(t, sustainedInodes(t, m))
}
