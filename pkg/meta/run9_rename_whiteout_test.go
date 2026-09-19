//go:build !nobadger

package meta

import (
	"runtime"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9RenameWhiteoutPreservesSourceInodeAndCreatesCharacterDevice(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	ctx := Background()
	var source, destination, whiteout Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "source", TypeFile, 0644, 0, 0, "", &source, &attr))
	require.Zero(t, m.Rename(ctx, RootInode, "source", RootInode, "destination", RenameWhiteout, nil, nil))
	require.Zero(t, m.Lookup(ctx, RootInode, "destination", &destination, &attr, false))
	require.Equal(t, source, destination)
	require.Equal(t, uint8(TypeFile), attr.Typ)
	require.Zero(t, m.Lookup(ctx, RootInode, "source", &whiteout, &attr, false))
	require.NotEqual(t, source, whiteout)
	require.Equal(t, uint8(TypeCharDev), attr.Typ)
	require.Zero(t, attr.Rdev)
	require.Zero(t, attr.Mode)
	require.Equal(t, uint32(1), attr.Nlink)
}

func TestRun9RenameWhiteoutAcrossDirectoriesInheritsGroupAndPreservesHardlinks(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux setgid inheritance")
	}
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	ctx := Background()
	var dir, source, got Ino
	var attr Attr
	require.Zero(t, m.Mkdir(ctx, RootInode, "group", 02777, 0, 0, &dir, &attr))
	require.Zero(t, m.SetAttr(ctx, dir, SetAttrGID|SetAttrMode, 0, &Attr{Gid: 2000, Mode: 02777}))
	require.Zero(t, m.Mknod(ctx, dir, "source", TypeFile, 0644, 0, 0, "", &source, &attr))
	require.Zero(t, m.Link(ctx, source, dir, "link", &attr))
	require.Zero(t, m.Rename(ctx, dir, "source", RootInode, "moved", RenameWhiteout, nil, nil))
	require.Zero(t, m.Lookup(ctx, dir, "source", &got, &attr, false))
	require.NotEqual(t, source, got)
	require.Equal(t, uint8(TypeCharDev), attr.Typ)
	require.Equal(t, uint32(2000), attr.Gid)
	require.Zero(t, attr.Mode)
	require.Zero(t, attr.Rdev)
	require.Zero(t, m.Lookup(ctx, RootInode, "moved", &got, &attr, false))
	require.Equal(t, source, got)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Zero(t, m.Lookup(ctx, dir, "link", &got, &attr, false))
	require.Equal(t, source, got)
	var sum Summary
	require.Zero(t, m.GetSummary(ctx, RootInode, &sum, true, true))
	require.Equal(t, uint64(3), sum.Files)
	require.Equal(t, uint64(2), sum.Dirs)
}

func TestRun9RenameWhiteoutQuotaFailureDoesNotMoveSource(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	format := testFormat()
	format.Inodes = 1
	require.NoError(t, m.Init(format, false))
	ctx := Background()
	var source, got Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "source", TypeFile, 0644, 0, 0, "", &source, &attr))
	m.getBase().doFlushStats()
	require.Equal(t, syscall.ENOSPC, m.Rename(ctx, RootInode, "source", RootInode, "destination", RenameWhiteout, nil, nil))
	require.Zero(t, m.Lookup(ctx, RootInode, "source", &got, &attr, false))
	require.Equal(t, source, got)
	require.Equal(t, uint8(TypeFile), attr.Typ)
	require.Equal(t, syscall.ENOENT, m.Lookup(ctx, RootInode, "destination", &got, &attr, false))
}

func TestRun9RenameWhiteoutReplacementSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	m, err := newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	require.NoError(t, m.Init(testFormat(), false))
	ctx := Background()
	var source, replaced, got Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "source", TypeFile, 0644, 0, 0, "", &source, &attr))
	require.Zero(t, m.Mknod(ctx, RootInode, "destination", TypeFile, 0644, 0, 0, "", &replaced, &attr))
	require.Zero(t, m.Rename(ctx, RootInode, "source", RootInode, "destination", RenameWhiteout, nil, nil))
	require.NoError(t, m.Shutdown())
	m, err = newKVMeta("badger", dir, testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.Zero(t, m.Lookup(ctx, RootInode, "destination", &got, &attr, false))
	require.Equal(t, source, got)
	require.Equal(t, uint8(TypeFile), attr.Typ)
	require.Zero(t, m.Lookup(ctx, RootInode, "source", &got, &attr, false))
	require.Equal(t, uint8(TypeCharDev), attr.Typ)
	require.NotEqual(t, source, got)
	require.NotEqual(t, replaced, got)
	require.Zero(t, attr.Rdev)
}

func TestRun9RenameWhiteoutNoReplaceLeavesBothEntriesUnchanged(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	ctx := Background()
	var source, destination, got Ino
	var attr Attr
	require.Zero(t, m.Mknod(ctx, RootInode, "source", TypeFile, 0644, 0, 0, "", &source, &attr))
	require.Zero(t, m.Mknod(ctx, RootInode, "destination", TypeFile, 0644, 0, 0, "", &destination, &attr))
	require.Equal(t, syscall.EEXIST, m.Rename(ctx, RootInode, "source", RootInode, "destination", RenameWhiteout|RenameNoReplace, nil, nil))
	require.Zero(t, m.Lookup(ctx, RootInode, "source", &got, &attr, false))
	require.Equal(t, source, got)
	require.Equal(t, uint8(TypeFile), attr.Typ)
	require.Zero(t, m.Lookup(ctx, RootInode, "destination", &got, &attr, false))
	require.Equal(t, destination, got)
}

func TestRun9RenameWhiteoutMovesDirectoryAndUpdatesParentLinks(t *testing.T) {
	m, err := newKVMeta("badger", t.TempDir(), testConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	require.NoError(t, m.Init(testFormat(), false))
	ctx := Background()
	var source, destination, file, got Ino
	var attr Attr
	require.Zero(t, m.Mkdir(ctx, RootInode, "source", 0755, 0, 0, &source, &attr))
	require.Zero(t, m.Mkdir(ctx, RootInode, "destination", 0755, 0, 0, &destination, &attr))
	require.Zero(t, m.Mknod(ctx, source, "file", TypeFile, 0644, 0, 0, "", &file, &attr))
	require.Zero(t, m.Rename(ctx, RootInode, "source", destination, "moved", RenameWhiteout, nil, nil))
	require.Zero(t, m.Lookup(ctx, RootInode, "source", &got, &attr, false))
	require.Equal(t, uint8(TypeCharDev), attr.Typ)
	require.Zero(t, m.Lookup(ctx, destination, "moved", &got, &attr, false))
	require.Equal(t, source, got)
	require.Equal(t, destination, attr.Parent)
	require.Zero(t, m.Lookup(ctx, source, "file", &got, &attr, false))
	require.Equal(t, file, got)
	require.Zero(t, m.GetAttr(ctx, RootInode, &attr))
	require.Equal(t, uint32(3), attr.Nlink)
	require.Zero(t, m.GetAttr(ctx, destination, &attr))
	require.Equal(t, uint32(3), attr.Nlink)
	var sum Summary
	require.Zero(t, m.GetSummary(ctx, RootInode, &sum, true, true))
	require.Equal(t, uint64(2), sum.Files)
	require.Equal(t, uint64(3), sum.Dirs)
}
