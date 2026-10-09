//go:build !nobadger

package meta

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun9SliceRefsReclaimOnlyAfterLastCloneReference(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 0
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	var deletes atomic.Int64
	m.OnMsg(DeleteSlice, func(...interface{}) error { deletes.Add(1); return nil })
	ctx := Background()
	var source, clone Ino
	require.Zero(t, m.Create(ctx, 1, "source", 0600, 0, 0, &source, &Attr{}))
	require.Zero(t, m.Create(ctx, 1, "clone", 0600, 0, 0, &clone, &Attr{}))
	var id uint64
	require.Zero(t, m.NewSlice(ctx, &id))
	require.Zero(t, m.Write(ctx, source, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
	require.Zero(t, m.CopyFileRange(ctx, source, 0, clone, 0, 100, 0, nil, nil))
	// K stores references minus one: 1 means two references, not one.
	raw, err := m.get(m.sliceKey(id, 100))
	require.NoError(t, err)
	require.Equal(t, int64(1), parseCounter(raw))
	require.NoError(t, m.deleteChunk(source, 0))
	raw, err = m.get(m.sliceKey(id, 100))
	require.NoError(t, err)
	require.Len(t, raw, 8)
	require.Zero(t, parseCounter(raw))
	// The hourly cleanup may remove a zero counter, because absence encodes one
	// reference. It must leave the cloned chunk intact and its later decrement valid.
	require.NoError(t, m.doCleanupSlices(ctx, nil))
	raw, err = m.get(m.chunkKey(clone, 0))
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	require.NoError(t, m.deleteChunk(clone, 0))
	raw, err = m.get(m.sliceKey(id, 100))
	require.NoError(t, err)
	require.Empty(t, raw)
	// Repeated write/delete cycles do not leave one K record per retired ID.
	for range 32 {
		require.Zero(t, m.NewSlice(ctx, &id))
		require.Zero(t, m.Write(ctx, source, 0, 0, Slice{Id: id, Size: 100, Len: 100}, time.Now()))
		require.NoError(t, m.deleteChunk(source, 0))
	}
	keys, err := m.scanKeys(ctx, []byte("K"))
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Zero(t, deletes.Load())
}

func TestRun9SliceRefsCleanLegacyRecordsAcrossScanPages(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 0
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	var deletes atomic.Int64
	m.OnMsg(DeleteSlice, func(...interface{}) error { deletes.Add(1); return nil })
	require.NoError(t, m.txn(Background(), func(tx *kvTxn) error {
		for id := uint64(1); id <= 2050; id++ {
			tx.set(m.sliceKey(id, 100), packCounter(-1))
		}
		tx.set(m.sliceKey(3000, 100), packCounter(1)) // Two live references.
		return nil
	}))
	var count uint64
	require.NoError(t, m.doCleanupSlices(Background(), &count))
	require.Equal(t, uint64(2050), count)
	keys, err := m.scanKeys(Background(), []byte("K"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{m.sliceKey(3000, 100)}, keys)
	require.Zero(t, deletes.Load())
	count = 0
	require.NoError(t, m.doCleanupSlices(Background(), &count))
	require.Zero(t, count)
}

func TestRun9SliceRefsPreserveOtherDeletionModes(t *testing.T) {
	for _, driver := range []string{"badger", "memkv"} {
		for _, maxDeletes := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", driver, maxDeletes), func(t *testing.T) {
				conf := testConfig()
				conf.MaxDeletes = maxDeletes
				client, err := newKVMeta(driver, t.TempDir(), conf)
				require.NoError(t, err)
				m := client.(*kvMeta)
				defer m.Shutdown()
				require.NoError(t, m.txn(Background(), func(tx *kvTxn) error { tx.set(m.sliceKey(10, 100), packCounter(-1)); return nil }))
				deletes := 0
				m.OnMsg(DeleteSlice, func(...interface{}) error { deletes++; return nil })
				m.deleteSlice(10, 100)
				raw, err := m.get(m.sliceKey(10, 100))
				require.NoError(t, err)
				if maxDeletes == 1 {
					require.Equal(t, 1, deletes)
					require.Empty(t, raw)
				} else {
					require.Zero(t, deletes)
					if driver == "badger" {
						require.Empty(t, raw)
					} else {
						require.Equal(t, int64(-1), parseCounter(raw))
					}
				}
			})
		}
	}
}
