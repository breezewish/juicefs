//go:build linux

package cmd

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalReadViewUsesLinuxNamesAndStablePages(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "z"), []byte("payload"), 0600))
	require.NoError(t, os.Symlink("z", filepath.Join(dir, "a")))
	require.NoError(t, syscall.Mkfifo(filepath.Join(dir, "fifo"), 0600))
	view, closeView, err := openRun9LocalReader(dir)
	require.NoError(t, err)
	defer closeView()
	ctx := run9ReadViewContext(t.Context())
	first, cursor, errno := view.ReadDirPage(ctx, "/rootfs", 2, "")
	require.Zero(t, errno)
	require.Len(t, first, 2)
	require.Equal(t, "a", first[0].Name())
	require.Equal(t, "fifo", cursor)
	last, cursor, errno := view.ReadDirPage(ctx, "/rootfs", 2, cursor)
	require.Zero(t, errno)
	require.Len(t, last, 1)
	require.Equal(t, "z", last[0].Name())
	require.Empty(t, cursor)
	_, _, errno = view.ReadDirPage(ctx, "/rootfs/fifo", 1, "")
	require.Equal(t, syscall.ENOTDIR, errno)
	target, errno := view.Readlink(ctx, "/rootfs/a")
	require.Zero(t, errno)
	require.Equal(t, []byte("z"), target)
	f, errno := view.OpenRead(ctx, "/rootfs/z")
	require.Zero(t, errno)
	defer f.Close()
	_, err = f.Seek(2, io.SeekStart)
	require.NoError(t, err)
	buf, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, []byte("yload"), buf)
}
