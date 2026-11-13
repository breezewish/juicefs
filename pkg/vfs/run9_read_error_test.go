package vfs

import (
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

type failedRead struct{ FileReader }

func (r failedRead) Read(meta.Context, uint64, []byte) (int, syscall.Errno) {
	return 0, syscall.EIO
}

func TestReadFailureLogsCallerOutcomeAndCacheOnlyScope(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.NewContext(42, 0, []uint32{0}))
	entry, fh, errno := v.Create(ctx, 1, "data", 0600, 0, syscall.O_RDWR)
	require.Zero(t, errno)
	t.Cleanup(func() { v.Release(ctx, entry.Inode, fh) })
	require.Zero(t, v.Write(ctx, entry.Inode, []byte("contents"), 0, fh))
	require.Zero(t, v.Flush(ctx, entry.Inode, fh, 0))
	originalHooks := logger.ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() { logger.ReplaceHooks(originalHooks) })
	hook := logtest.NewLocal(&logger.Logger)
	n, errno := v.Read(ctx, entry.Inode, make([]byte, 4), 2, fh)
	require.Zero(t, errno)
	require.Equal(t, 4, n)
	require.Nil(t, hook.LastEntry())

	h := v.findHandle(entry.Inode, fh)
	h.reader = failedRead{h.reader}
	n, errno = v.Read(ctx, entry.Inode, make([]byte, 4), 2, fh)
	require.Equal(t, syscall.EIO, errno)
	require.Zero(t, n)
	require.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
	message := hook.LastEntry().Message
	require.Contains(t, message, "read_request_failed format=\"test\"")
	require.Contains(t, message, "pid=42 uid=0 offset=2 requested_bytes=4 returned_bytes=0 errno=5")

	hook.Reset()
	v.cacheOnlyThreads.Store(uint32(42), struct{}{})
	t.Cleanup(func() { v.cacheOnlyThreads.Delete(uint32(42)) })
	_, errno = v.Read(ctx, entry.Inode, make([]byte, 4), 2, fh)
	require.Equal(t, syscall.EIO, errno)
	require.Nil(t, hook.LastEntry())
}
