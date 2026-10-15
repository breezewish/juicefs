package cmd

import (
	"context"
	"encoding/json"
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

type run9OwnedBytesRequest struct {
	ForkCheckpoints []run9ForkCheckpoint       `json:"fork_checkpoints"`
	ForkFloor       uint64                     `json:"fork_floor"`
	LastClosedRaw   *int64                     `json:"last_closed_raw"`
	Basis           uint64                     `json:"basis"`
	OwnershipRanges []meta.Run9OwnedEpochRange `json:"ownership_ranges"`
}

// beginRun9OwnedBytes uses GC's child checkpoint proofs and ownership ranges.
// A per-mount request file keeps large checkpoint lists out of the process's
// argument/environment limit. Cached slice lists avoid rescanning children.
func beginRun9OwnedBytes(m meta.Meta, epoch uint64, start uint64) error {
	ctx := context.Background()
	var request run9OwnedBytesRequest
	if path := os.Getenv("JFS_RUN9_OWNED_BYTES_REQUEST"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read owned bytes request: %w", err)
		}
		// The launch owns this private one-use file; child startup has consumed it.
		_ = os.Remove(path)
		if err := json.Unmarshal(raw, &request); err != nil {
			return fmt.Errorf("invalid owned bytes request: %w", err)
		}
	}
	format, err := m.Load(true)
	if err != nil {
		return err
	}
	cfg := meta.Run9OwnedBytesConfig{
		Epoch: epoch, AllocationStart: start, ForkFloor: request.ForkFloor,
		LastClosedRaw: request.LastClosedRaw, Basis: request.Basis,
		OwnershipRanges: request.OwnershipRanges,
	}
	scan := run9ForkCheckpointScan{remaining: run9ForkCheckpointScanBudget, failed: make(map[string]string)}
	for _, checkpoint := range request.ForkCheckpoints {
		ids, err := run9ForkCheckpointSlices(ctx, checkpoint.Directory, format.Name, &scan)
		if err != nil {
			cfg.ForkFloor = max(cfg.ForkFloor, checkpoint.SourceAllocationEnd)
			logger.Warnf("owned bytes: keep fork checkpoint %s by allocation floor: %s", checkpoint.Directory, err)
			continue
		}
		cfg.ProtectedIDs = append(cfg.ProtectedIDs, ids...)
	}
	// Tests shorten the rest between recounts.
	if raw := os.Getenv("JFS_RUN9_OWNED_RECOUNT_INTERVAL"); raw != "" {
		if meta.Run9OwnedRecountInterval, err = time.ParseDuration(raw); err != nil || meta.Run9OwnedRecountInterval <= 0 {
			return fmt.Errorf("invalid JFS_RUN9_OWNED_RECOUNT_INTERVAL")
		}
	}
	if err := m.(interface {
		Run9TrackOwnedBytes(context.Context, meta.Run9OwnedBytesConfig) error
	}).Run9TrackOwnedBytes(ctx, cfg); err != nil {
		logger.Warnf("owned bytes unavailable for this mount: %s", err)
	}
	return nil
}
