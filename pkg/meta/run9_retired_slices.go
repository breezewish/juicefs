package meta

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/juicedata/juicefs/pkg/utils"
)

// OpenRun9RetiredSnapshot opens a finalized metadata clone without sessions,
// background mutation, or online deletion. Unlike NewClient, corrupt snapshots
// return an error so maintenance can report them and continue other tasks.
func OpenRun9RetiredSnapshot(dir string) (*kvMeta, error) {
	conf := DefaultConf()
	conf.ReadOnly, conf.NoBGJob, conf.MaxDeletes = true, true, 0
	conf.AtimeMode = NoAtime
	m, err := newKVMeta("badger", dir+"?readonly=1", conf)
	if err != nil {
		return nil, err
	}
	return m.(*kvMeta), nil
}

// Run9SliceAllocationCounter reads the next reserved slice ID without allocating
// or resetting anything. The caller captures it before NewSession starts jobs.
func (m *kvMeta) Run9SliceAllocationCounter() (uint64, error) {
	raw, err := m.get(m.counterKey("nextChunk"))
	if err != nil {
		return 0, err
	}
	// An absent counter is zero in a new volume; malformed persisted values
	// must fail this task, not panic through the generic counter decoder.
	if len(raw) != 0 && len(raw) != 8 {
		return 0, fmt.Errorf("invalid nextChunk counter encoding")
	}
	v := parseCounter(raw)
	if v < 0 {
		return 0, fmt.Errorf("negative nextChunk counter")
	}
	return uint64(v), nil
}

// Run9RetiredSlices selects a page of native negative-reference records in [start,end).
// K stores references minus one: zero (and a missing K) is NOT proof of garbage.
// after is the previous page's hex-encoded last examined key (exclusive). Empty
// next means EOF. Pagination counts all reference records, including live ones,
// so sparse garbage cannot cause an unbounded page or be skipped between pages.
// The caller must use an immutable, successfully finalized metadata clone and
// independently prove the interval was private to that writable lifecycle.
func (m *kvMeta) Run9RetiredSlices(ctx context.Context, start, end uint64, after string, limit int) ([]Slice, string, error) {
	if m.Name() != "badger" || m.conf.MaxDeletes != 0 || start > end || limit <= 0 {
		return nil, "", fmt.Errorf("retired slice scan requires Badger, disabled deletes and valid bounds")
	}
	begin, stop := m.fmtKey("K", start), m.fmtKey("K", end)
	if after != "" {
		key, err := hex.DecodeString(after)
		if err != nil || len(key) != 13 || bytes.Compare(key, begin) < 0 || bytes.Compare(key, stop) >= 0 {
			return nil, "", fmt.Errorf("invalid retired slice cursor")
		}
		begin = append(key, 0) // strictly after the complete (id,size) key
	}
	var slices []Slice
	var next string
	err := m.client.txn(ctx, func(tx *kvTxn) error {
		var scanErr error
		var scanned int
		var last string
		tx.scan(begin, stop, false, func(k, v []byte) bool {
			if scanErr = ctx.Err(); scanErr != nil {
				return false
			}
			if scanned >= limit {
				next = last
				return false
			}
			scanned++
			last = hex.EncodeToString(k)
			if len(k) != 13 || len(v) != 8 {
				scanErr = fmt.Errorf("malformed slice reference record")
				return false
			}
			if parseCounter(v) < 0 {
				b := utils.FromBuffer(k[1:])
				id, size := b.Get64(), b.Get32()
				if id < start || id >= end || size == 0 || size > ChunkSize {
					scanErr = fmt.Errorf("invalid retired slice %d size %d", id, size)
					return false
				}
				slices = append(slices, Slice{Id: id, Size: size})
			}
			return true
		})
		return scanErr
	}, 0)
	if err != nil {
		return nil, "", err
	}
	return slices, next, nil
}
