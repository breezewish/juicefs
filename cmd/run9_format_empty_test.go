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
