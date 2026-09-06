package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Capture before NewSession can start allocating slices. The counter includes
// reserved IDs, so unused holes are harmless. Finalize only returns this value;
// run9rt snapshots the closed metadata before releasing its lifecycle lock.
func beginRun9SliceAllocation(m meta.Meta) (*uint64, error) {
	raw := os.Getenv("JFS_RUN9_OWNED_EPOCH")
	if raw == "" || raw == "0" {
		return nil, nil
	}
	epoch, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || epoch == 0 || epoch >= 1<<32-1 || m.Name() != "badger" {
		return nil, fmt.Errorf("invalid run9 slice allocation configuration")
	}
	reader, ok := m.(interface{ Run9SliceAllocationCounter() (uint64, error) })
	if !ok {
		return nil, fmt.Errorf("run9 slice allocation requires Badger")
	}
	start, err := reader.Run9SliceAllocationCounter()
	if err != nil {
		return nil, err
	}
	if start < epoch<<32 || start > (epoch+1)<<32 {
		return nil, fmt.Errorf("slice allocation counter %d outside owned epoch %d", start, epoch)
	}
	return &start, nil
}
