//go:build run9_checkpoint_research

package vfs

import (
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestResearchCheckpointBarrierSeparatesBufferedWrites(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	defer v.Meta.Shutdown()
	ctx := NewLogContext(meta.NewContext(10, 0, []uint32{0}))
	file, fh, status := v.Create(ctx, 1, "checkpoint", 0644, 0, syscall.O_RDWR)
	require.Zero(t, status)
	require.Zero(t, v.Write(ctx, file.Inode, []byte("before"), 0, fh))
	unlock := v.Run9CheckpointWriteBarrier()
	released := false
	defer func() {
		if !released {
			unlock()
		}
	}()
	written := make(chan syscall.Errno, 1)
	go func() { written <- v.Write(ctx, file.Inode, []byte("after!"), 0, fh) }()
	require.NoError(t, v.FlushAll(""))
	buf := make([]byte, 6)
	n, status := v.Read(ctx, file.Inode, buf, 0, fh)
	require.Zero(t, status)
	require.Equal(t, 6, n)
	require.Equal(t, "before", string(buf))
	select {
	case status := <-written:
		t.Fatalf("write crossed snapshot barrier: %v", status)
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	released = true
	select {
	case status := <-written:
		require.Zero(t, status)
	case <-time.After(time.Second):
		t.Fatal("write did not resume")
	}
	n, status = v.Read(ctx, file.Inode, buf, 0, fh)
	require.Zero(t, status)
	require.Equal(t, 6, n)
	require.Equal(t, "after!", string(buf))
	require.NoError(t, v.FlushAll(""))
}
