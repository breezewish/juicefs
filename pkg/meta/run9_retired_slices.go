package meta

import (
	"context"
	"fmt"

	"github.com/juicedata/juicefs/pkg/utils"
)

// Run9SliceAllocationCounter reads the next reserved slice ID without allocating
// or resetting anything. The caller captures it before NewSession starts jobs.
func (m *kvMeta) Run9SliceAllocationCounter() (uint64, error) {
	v, err := m.en.getCounter("nextChunk")
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("negative nextChunk counter")
	}
	return uint64(v), nil
}

// Run9RetiredSlices selects only native negative-reference records in [start,end).
// K stores references minus one: zero (and a missing K) is NOT proof of garbage.
// This is a bounded, read-only transaction, not ListSlices' best-effort live scan.
// The caller must independently prove the interval was private to this mount and
// successfully close metadata before authorizing any physical deletion.
func (m *kvMeta) Run9RetiredSlices(ctx context.Context, start, end uint64, limit int) ([]Slice, bool, error) {
	if m.Name() != "badger" || m.conf.MaxDeletes != 0 || start > end || limit <= 0 {
		return nil, false, fmt.Errorf("retired slice scan requires Badger, disabled deletes and valid bounds")
	}
	var slices []Slice
	var truncated bool
	err := m.client.txn(ctx, func(tx *kvTxn) error {
		var scanErr error
		var scanned int
		tx.scan(m.fmtKey("K", start), m.fmtKey("K", end), false, func(k, v []byte) bool {
			if scanErr = ctx.Err(); scanErr != nil {
				return false
			}
			if scanned >= 65536 || len(slices) >= limit {
				truncated = true
				return false
			}
			scanned++
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
		return nil, false, err
	}
	return slices, truncated, nil
}
