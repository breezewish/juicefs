//go:build !noredis

package meta

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisBatchClonePublishesEntries(t *testing.T) {
	rawMeta, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	require.NoError(t, err)
	defer rawMeta.Shutdown()
	require.NoError(t, rawMeta.Reset())
	require.NoError(t, rawMeta.Init(testFormat(), false))

	ctx := Background()
	var srcDir, dstDir, srcFile Ino
	require.Zero(t, rawMeta.Mkdir(ctx, RootInode, "src", 0755, 022, 0, &srcDir, nil))
	require.Zero(t, rawMeta.Mkdir(ctx, RootInode, "dst", 0755, 022, 0, &dstDir, nil))
	require.Zero(t, rawMeta.Mknod(ctx, srcDir, "file", TypeFile, 0644, 022, 0, "", &srcFile, nil))

	var count uint64
	entries := []*Entry{{Inode: srcFile, Name: []byte("file")}}
	require.Equal(t, syscall.Errno(0), rawMeta.getBase().BatchClone(ctx, srcDir, dstDir, entries, 0, 022, &count))
	require.Equal(t, uint64(1), count)

	var cloned Ino
	var attr Attr
	require.Zero(t, rawMeta.Lookup(ctx, dstDir, "file", &cloned, &attr, false))
	require.NotEqual(t, srcFile, cloned)
	require.Equal(t, uint8(TypeFile), attr.Typ)
}
