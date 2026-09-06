package cmd

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

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
