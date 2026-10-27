//go:build !noredis

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Run compaction after the copy has read its source slices, before it publishes
// destination references. A separate client models a different FUSE mount.
type compactAfterReadHook struct {
	key     string
	compact func()
}

func (h *compactAfterReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *compactAfterReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *compactAfterReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "lrange" && cmd.Args()[1] == h.key && h.compact != nil {
			compact := h.compact
			h.compact = nil
			compact()
		}
		return err
	}
}

func TestRedisCopyFileRangeRetriesCompactedSource(t *testing.T) {
	raw, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	require.NoError(t, err)
	m := raw.(*redisMeta)
	defer m.Shutdown()
	require.NoError(t, m.Reset())
	require.NoError(t, m.Init(testFormat(), false))
	other, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	require.NoError(t, err)
	compactor := other.(*redisMeta)
	defer compactor.Shutdown()
	var deleted uint64
	compactor.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted = args[0].(uint64); return nil })
	ctx := Background()
	var source, destination Ino
	require.Zero(t, m.Create(ctx, RootInode, "source", 0600, 0, 0, &source, &Attr{}))
	require.Zero(t, m.Create(ctx, RootInode, "destination", 0600, 0, 0, &destination, &Attr{}))
	require.Zero(t, m.Write(ctx, source, 0, 0, Slice{Id: 100, Size: 100, Len: 100}, time.Now()))
	origin := marshalSlice(0, 100, 100, 0, 100)
	hook := &compactAfterReadHook{key: m.chunkKey(source, 0), compact: func() {
		require.Zero(t, compactor.doCompactChunk(source, 0, origin, []*slice{{id: 100, size: 100, len: 100}}, 0, 0, 200, 100, nil))
		require.Equal(t, uint64(100), deleted)
	}}
	m.rdb.AddHook(hook)
	var copied uint64
	require.Zero(t, m.CopyFileRange(ctx, source, 0, destination, 0, 100, 0, &copied, nil))
	require.Nil(t, hook.compact)
	require.Equal(t, uint64(100), copied)
	var slices []Slice
	require.Zero(t, m.Read(ctx, destination, 0, &slices))
	require.Equal(t, []Slice{{Id: 200, Size: 100, Len: 100}}, slices)
}

func TestRedisCloneRetriesCompactedSource(t *testing.T) {
	raw, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	require.NoError(t, err)
	m := raw.(*redisMeta)
	defer m.Shutdown()
	require.NoError(t, m.Reset())
	require.NoError(t, m.Init(testFormat(), false))
	other, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	require.NoError(t, err)
	compactor := other.(*redisMeta)
	defer compactor.Shutdown()
	var deleted uint64
	compactor.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted = args[0].(uint64); return nil })
	ctx := Background()
	var source Ino
	require.Zero(t, m.Create(ctx, RootInode, "source", 0600, 0, 0, &source, &Attr{}))
	require.Zero(t, m.Write(ctx, source, 0, 0, Slice{Id: 100, Size: 100, Len: 100}, time.Now()))
	origin := marshalSlice(0, 100, 100, 0, 100)
	hook := &compactAfterReadHook{key: m.chunkKey(source, 0), compact: func() {
		require.Zero(t, compactor.doCompactChunk(source, 0, origin, []*slice{{id: 100, size: 100, len: 100}}, 0, 0, 200, 100, nil))
		require.Equal(t, uint64(100), deleted)
	}}
	m.rdb.AddHook(hook)
	// Exercise the ordinary single-entry clone as well as run9's separate
	// batch-clone path, which already watches source chunk lists.
	destination, err := m.nextInode()
	require.NoError(t, err)
	require.Zero(t, m.doCloneEntry(ctx, source, RootInode, "clone", destination, &Attr{}, 0, 0, true))
	require.Nil(t, hook.compact)
	var slices []Slice
	require.Zero(t, m.Read(ctx, destination, 0, &slices))
	require.Equal(t, []Slice{{Id: 200, Size: 100, Len: 100}}, slices)
}
