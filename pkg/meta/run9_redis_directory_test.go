//go:build !noredis

package meta

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
)

type checkpointDirectoryRedis struct {
	redis.UniversalClient
	values map[string]string
	calls  int
}

func (r *checkpointDirectoryRedis) HGetAll(context.Context, string) *redis.MapStringStringCmd {
	r.calls++
	copy := make(map[string]string, len(r.values))
	for k, v := range r.values {
		copy[k] = v
	}
	return redis.NewMapStringStringResult(copy, nil)
}
func TestRun9CheckpointDirectoryKeepsOneImageAcrossPages(t *testing.T) {
	r := &checkpointDirectoryRedis{values: map[string]string{}}
	m := &redisMeta{baseMeta: &baseMeta{conf: &Config{SortDir: true}}, rdb: r}
	for i := 0; i < 10001; i++ {
		r.values[fmt.Sprintf("member-%05d", i)] = string(m.packEntry(TypeDirectory, Ino(i+2)))
	}
	r.values["zz-child"] = string(m.packEntry(TypeDirectory, 20000))
	h := &redisDirHandler{en: m, inode: 1, batchNum: 10000}
	first, st := h.List(Background(), 0)
	require.Zero(t, st)
	require.Len(t, first, 10000)
	// Another host renames the final unread registration between FUSE pages.
	delete(r.values, "zz-child")
	r.values[".gc.zz-child.token"] = string(m.packEntry(TypeDirectory, 20000))
	rest, st := h.List(Background(), len(first))
	require.Zero(t, st)
	require.Equal(t, "zz-child", string(rest[len(rest)-1].Name))
	require.Equal(t, 1, r.calls)
}
