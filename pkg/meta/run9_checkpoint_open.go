package meta

import (
	"fmt"
)

// OpenRun9Checkpoint opens a finalized metadata clone without sessions,
// background mutation, or online deletion. Unlike NewClient, corrupt snapshots
// return an error so maintenance can report them and continue other tasks.
func OpenRun9Checkpoint(dir string) (*kvMeta, error) {
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

// Run9SliceAllocationEnd returns the final exclusive reservation bound without
// I/O. Call only after successful session close and metadata shutdown.
func (m *baseMeta) Run9SliceAllocationEnd(initial uint64) uint64 {
	m.freeMu.Lock()
	defer m.freeMu.Unlock()
	return max(initial, m.freeSlices.maxid)
}
