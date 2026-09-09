package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9FormatEmptyCreatesNativeFilesystemWithoutSession(t *testing.T) {
	dir, bucket := t.TempDir(), t.TempDir()
	uri := "badger://" + dir
	args := []string{"juicefs", "format", "--storage", "file", "--bucket", bucket, "--trash-days", "0", "--run9-init-empty-epoch", "7", uri, "empty-test"}
	require.NoError(t, Main(args))
	require.FileExists(t, filepath.Join(bucket, "empty-test", "juicefs_uuid"))
	require.ErrorContains(t, Main(args), "refuses existing format")
	m := meta.NewClient(uri, meta.DefaultConf())
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	format, err := m.Load(true)
	require.NoError(t, err)
	require.Equal(t, "empty-test", format.Name)
	var ino meta.Ino
	var attr meta.Attr
	require.Zero(t, m.Lookup(meta.Background(), meta.RootInode, "rootfs", &ino, &attr, false))
	require.Equal(t, uint16(0755), attr.Mode)
	require.Equal(t, uint32(os.Geteuid()), attr.Uid)
	require.Equal(t, uint32(os.Getegid()), attr.Gid)
	sessions, err := m.ListSessions()
	require.NoError(t, err)
	require.Empty(t, sessions)
}

func TestRun9FormatEmptyInitialGuestPermissions(t *testing.T) {
	uri := "badger://" + t.TempDir()
	require.NoError(t, Main([]string{"juicefs", "format", "--storage", "file", "--bucket", t.TempDir(),
		"--run9-init-empty-epoch", "7", "--run9-rootfs-uid", "1000", "--run9-rootfs-gid", "2000", "--run9-rootfs-mode", "1528", uri, "empty-permissions"}))
	m := meta.NewClient(uri, meta.DefaultConf())
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })
	_, err := m.Load(true)
	require.NoError(t, err)
	var ino meta.Ino
	var attr meta.Attr
	require.Zero(t, m.Lookup(meta.Background(), meta.RootInode, "rootfs", &ino, &attr, false))
	require.Equal(t, uint32(os.Geteuid()), attr.Uid)
	require.Equal(t, uint32(os.Getegid()), attr.Gid)
	var value []byte
	require.Zero(t, m.GetXattr(meta.Background(), ino, "user.containers.override_stat", &value))
	require.Equal(t, "1000:2000:02770", string(value))
}
