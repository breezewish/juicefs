package meta

import (
	"context"
	"fmt"
	"sort"
)

// Run9CheckpointSlices is a deletion proof, unlike best-effort ListSlices.
// The caller must hold an immutable closed checkpoint. Every read and decode
// must succeed before it can use the returned live IDs to delete objects.
// Deleted inodes cannot become reachable again; effective chunk overlays match
// Read and CopyFileRange, so overwritten references cannot become live again.
func (m *kvMeta) Run9CheckpointSlices(ctx context.Context) ([]uint64, error) {
	live := make(map[uint64]bool)
	if err := m.ScanRun9CheckpointSlices(ctx, func(id uint64) { live[id] = true }); err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// ScanRun9CheckpointSlices validates the complete checkpoint while allowing
// range GC to retain only relevant references. The visitor's output is usable
// as deletion evidence only after this method and Shutdown both succeed.
func (m *kvMeta) ScanRun9CheckpointSlices(ctx context.Context, visit func(uint64)) error {

	if m.Name() != "badger" || !m.conf.ReadOnly || !m.conf.NoBGJob {
		return fmt.Errorf("checkpoint scan requires immutable Badger")
	}
	format, err := m.Load(true)
	if err != nil {
		return err
	}
	if format.TrashDays != 0 {
		return fmt.Errorf("checkpoint scan requires disabled trash")
	}
	var scanErr error
	err = m.client.scan([]byte("A"), func(k, v []byte) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		if len(k) < 10 {
			scanErr = fmt.Errorf("malformed inode key")
			return false
		}
		if k[9] != 'C' {
			return true
		}
		if len(k) != 14 || len(v)%sliceBytes != 0 {
			scanErr = fmt.Errorf("malformed chunk record")
			return false
		}
		raw, err := m.get(m.inodeKey(m.decodeInode(k[1:9])))
		if err != nil {
			scanErr = err
			return false
		}
		// Even unreachable chunks must decode cleanly: corruption never authorizes GC.
		ss := readSliceBuf(v)
		for _, s := range ss {
			if s.len == 0 || uint64(s.pos)+uint64(s.len) > ChunkSize || (s.id != 0 && (s.size == 0 || s.size > ChunkSize || uint64(s.off)+uint64(s.len) > uint64(s.size))) {
				scanErr = fmt.Errorf("invalid chunk slice")
				return false
			}
		}
		if len(raw) == 0 {
			return true
		}
		if len(raw) != 64 && len(raw) != 72 && len(raw) != 80 {
			scanErr = fmt.Errorf("malformed inode attributes")
			return false
		}
		var attr Attr
		attr.Unmarshal(raw)
		if attr.Typ != TypeFile {
			scanErr = fmt.Errorf("chunk belongs to non-file inode")
			return false
		}
		if attr.Nlink == 0 {
			return true
		}
		for _, s := range buildSlice(ss) {
			if s.Id != 0 {
				visit(s.Id)
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	return scanErr
}
