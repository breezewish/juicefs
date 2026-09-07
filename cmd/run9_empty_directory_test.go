package cmd

import (
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9EmptyDirectoryBatchValidatesEveryPath(t *testing.T) {
	metaURL := "badger://" + filepath.Join(t.TempDir(), "meta")
	store := meta.NewClient(metaURL, meta.DefaultConf())
	require.NoError(t, store.Init(&meta.Format{Name: "empty-batch", Storage: "file", Bucket: t.TempDir(), BlockSize: 4}, true))
	ctx := meta.NewContext(1, 0, []uint32{0})
	var inode meta.Ino
	var attr meta.Attr
	require.Zero(t, store.Mkdir(ctx, meta.RootInode, "rootfs", 0755, 0, 0, &inode, &attr))
	var occupied meta.Ino
	require.Zero(t, store.Create(ctx, inode, "occupied", 0644, 0, 0, &occupied, &attr))
	require.NoError(t, store.Shutdown())
	require.NoError(t, Main([]string{"juicefs", "check-empty-dir", metaURL + "?readonly=1", "/state", "/cache"}))
	require.NoError(t, checkRun9EmptyDirectory(t.Context(), metaURL+"?readonly=1", "/state", "/cache"))
	require.ErrorContains(t, checkRun9EmptyDirectory(t.Context(), metaURL+"?readonly=1", "/state", "/occupied"), "non-directory")
	require.ErrorContains(t, checkRun9EmptyDirectory(t.Context(), metaURL+"?readonly=1", "/state", "/"), "canonical")
}

func TestRun9EmptyDirectoryChecksEveryAncestorWithoutMutatingRoot(t *testing.T) {
	root := newRun9ReadViewTestFS(t)
	ctx := meta.NewContext(1, 0, []uint32{0})
	require.Zero(t, root.Mkdir(ctx, "/rootfs", 0755, 0))
	require.NoError(t, checkRun9EmptyDirectoryMetadata(ctx, root.Meta(), "/missing/nested"))
	require.Zero(t, root.Mkdir(ctx, "/rootfs/state", 0755, 0))
	require.NoError(t, checkRun9EmptyDirectoryMetadata(ctx, root.Meta(), "/state"))
	writeRun9ReadViewTestFile(t, root, ctx, "/rootfs/state/value", "keep")
	require.ErrorContains(t, checkRun9EmptyDirectoryMetadata(ctx, root.Meta(), "/state"), "not empty")
	require.ErrorContains(t, checkRun9EmptyDirectoryMetadata(ctx, root.Meta(), "/state/value/nested"), "non-directory")
	require.Zero(t, root.Symlink(ctx, "state", "/rootfs/link"))
	require.ErrorContains(t, checkRun9EmptyDirectoryMetadata(ctx, root.Meta(), "/link/missing"), "symlink")
}
