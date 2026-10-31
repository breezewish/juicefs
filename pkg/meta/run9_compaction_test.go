//go:build !nobadger

package meta

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Checkpoint GC may delete a slice hidden by a write beyond the compaction
// prefix. Compaction must still succeed without reading that retired object.
func TestRun9CompactionAfterCheckpointCollectsOverwrittenPrefix(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 0
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer m.Shutdown()
	require.NoError(t, m.Init(testFormat(), false))
	raw := marshalSlice(0, 10, 4, 0, 4)
	for range maxCompactSlices - 1 {
		raw = append(raw, marshalSlice(4, 20, 4, 0, 4)...)
	}
	raw = append(raw, marshalSlice(0, 30, 4, 0, 4)...)
	require.NoError(t, m.txn(Background(), func(tx *kvTxn) error {
		tx.set(m.inodeKey(2), (&Attr{Typ: TypeFile, Nlink: 1, Length: 8}).Marshal())
		tx.set(m.chunkKey(2, 0), raw)
		tx.set(m.counterKey("nextChunk"), packCounter(100))
		return nil
	}))
	// Only the effective KEEP and TAIL objects remain after checkpoint GC.
	objects := map[uint64][]byte{20: []byte("KEEP"), 30: []byte("TAIL")}
	var compactedID uint64
	m.OnMsg(CompactChunk, func(args ...interface{}) error {
		var data []byte
		for _, s := range args[0].([]Slice) {
			if s.Id == 0 {
				data = append(data, make([]byte, s.Len)...)
				continue
			}
			b, found := objects[s.Id]
			if !found {
				return fmt.Errorf("object %d was already collected", s.Id)
			}
			data = append(data, b[s.Off:s.Off+s.Len]...)
		}
		compactedID = args[1].(uint64)
		objects[compactedID] = data
		return nil
	})
	m.compactChunk(2, 0, false, false)
	require.NotZero(t, compactedID, "compaction must not retry a collected prefix slice")
	remaining, st := m.doRead(Background(), 2, 0)
	require.Zero(t, st)
	require.Len(t, remaining, 2, "the bounded prefix was compacted and the tail retained")
	visible := buildSlice(remaining)
	require.Equal(t, []Slice{{Id: 30, Size: 4, Len: 4}, {Id: compactedID, Size: 8, Off: 4, Len: 4}}, visible)
	require.Equal(t, []byte("\x00\x00\x00\x00KEEP"), objects[compactedID])
}

func TestRun9CompactionTailMasksOnlyOverwrittenBytes(t *testing.T) {
	prefix := []*slice{{pos: 4, id: 10, size: 20, off: 2, len: 12}}
	tail := []*slice{{pos: 6, id: 20, size: 2, len: 2}, {pos: 14, id: 30, size: 8, len: 8}}
	require.Equal(t, []Slice{
		{Id: 10, Size: 20, Off: 2, Len: 2},
		{Len: 2},
		{Id: 10, Size: 20, Off: 6, Len: 6},
		{Len: 2},
	}, run9CompactionSlices(prefix, tail, 4, 12))
	// An overwrite covering the entire prefix must retain the original bounds,
	// rather than emitting a hole at offset zero over an earlier skipped slice.
	require.Equal(t, []Slice{{Len: 12}}, run9CompactionSlices(prefix,
		[]*slice{{pos: 0, id: 40, size: 32, len: 32}}, 4, 12))
	// Disjoint tail writes neither extend the compacted object nor change offsets.
	require.Equal(t, []Slice{{Id: 10, Size: 20, Off: 2, Len: 12}}, run9CompactionSlices(prefix,
		[]*slice{{pos: 0, id: 50, size: 4, len: 4}, {pos: 16, id: 60, size: 4, len: 4}}, 4, 12))
	// Native compaction trims trailing holes; masking must not restore them and
	// upload more bytes than the replacement slice's recorded object size.
	require.Equal(t, []Slice{{Id: 10, Size: 20, Off: 2, Len: 4}, {Len: 8}}, run9CompactionSlices(
		[]*slice{{pos: 4, id: 10, size: 20, off: 2, len: 12}, {pos: 16, len: 16}},
		[]*slice{{pos: 8, id: 20, size: 8, len: 8}}, 4, 12))
	// Native compaction represents an entirely sparse prefix with one byte.
	require.Equal(t, []Slice{{Size: 4, Len: 1}}, run9CompactionSlices(
		[]*slice{{pos: 4, len: 8}}, []*slice{{pos: 4, len: 8}}, 0, 1))
}
