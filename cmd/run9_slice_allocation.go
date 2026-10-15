package cmd

import (
	"fmt"
	"os"
	"strconv"
	"time"

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
	if err := beginRun9OwnedBytes(m, epoch, start); err != nil {
		return nil, err
	}
	return &start, nil
}

// beginRun9OwnedBytes starts counting the bytes this writable epoch owns.
func beginRun9OwnedBytes(m meta.Meta, epoch uint64, start uint64) error {
	// Slices below the bound existed at a Fork of this filesystem and were
	// counted as kept when it was taken. Absent: never forked before this mount.
	var forkBound, keptForForks uint64
	var err error
	if raw := os.Getenv("JFS_RUN9_FORK_BOUND"); raw != "" {
		if forkBound, err = strconv.ParseUint(raw, 10, 64); err != nil {
			return fmt.Errorf("invalid JFS_RUN9_FORK_BOUND")
		}
	}
	if raw := os.Getenv("JFS_RUN9_KEPT_FOR_FORKS_BYTES"); raw != "" {
		if keptForForks, err = strconv.ParseUint(raw, 10, 64); err != nil {
			return fmt.Errorf("invalid JFS_RUN9_KEPT_FOR_FORKS_BYTES")
		}
	}
	// Tests shorten the rest between recounts.
	if raw := os.Getenv("JFS_RUN9_OWNED_RECOUNT_INTERVAL"); raw != "" {
		if meta.Run9OwnedRecountInterval, err = time.ParseDuration(raw); err != nil || meta.Run9OwnedRecountInterval <= 0 {
			return fmt.Errorf("invalid JFS_RUN9_OWNED_RECOUNT_INTERVAL")
		}
	}
	m.(interface {
		Run9TrackOwnedBytes(uint64, uint64, uint64, uint64)
	}).Run9TrackOwnedBytes(epoch, forkBound, keptForForks, start)
	return nil
}
