package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

type run9GCCheckpointRequest struct {
	After         string             `json:"after,omitempty"`
	Directory     string             `json:"directory"`
	Format        string             `json:"format"`
	AllocationEnd uint64             `json:"allocation_end"`
	Ranges        []run9GCSliceRange `json:"ranges"`
	Deleted       bool               `json:"deleted"`
	DryRun        bool               `json:"dry_run"`
	// ForkCheckpoints hold the metadata that direct children received at Fork.
	// Every slice they reference is retained, also for a deleted owner whose own
	// checkpoint no longer retains anything.
	ForkCheckpoints        []run9ForkCheckpoint         `json:"fork_checkpoints,omitempty"`
	ForkCheckpointFailures []run9FallbackForkCheckpoint `json:"fork_checkpoint_failures,omitempty"`
}

type run9ForkCheckpoint struct {
	Directory string `json:"directory"`
	// SourceAllocationEnd is the child's fork bound. Everything below it stays
	// protected in a round that cannot use Directory.
	SourceAllocationEnd uint64 `json:"source_allocation_end"`
}

type run9GCCheckpointOutput struct {
	run9GCSliceRangesOutput
	// FallbackForkCheckpoints protected the owner by their bound in this round.
	FallbackForkCheckpoints []run9FallbackForkCheckpoint `json:"fallback_fork_checkpoints,omitempty"`
	ForkCheckpointFailures  []run9FallbackForkCheckpoint `json:"fork_checkpoint_failures,omitempty"`
}

type run9FallbackForkCheckpoint struct {
	Directory string `json:"directory"`
	Reason    string `json:"reason"`
}

// A round first scans fork checkpoints that have no list yet. Past this budget
// the remaining ones keep protecting by their bound and wait for the next round.
var run9ForkCheckpointScanBudget = 5 * time.Minute

func cmdRun9GCCheckpoint() *cli.Command {
	return &cli.Command{Name: "gc-checkpoint", Hidden: true, Usage: "strictly scan a finalized checkpoint before range GC",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "request", Required: true},
			// A caller that lowers its fork floor because of fork_checkpoints passes
			// this flag, so a helper that would ignore the field rejects the round.
			&cli.BoolFlag{Name: "fork-checkpoints"},
		},
		Action: func(c *cli.Context) error {
			raw, err := os.ReadFile(c.String("request"))
			if err != nil {
				return err
			}
			var req run9GCCheckpointRequest
			if err := json.Unmarshal(raw, &req); err != nil {
				return err
			}
			out, err := run9GCCheckpoint(c.Context, req)
			if err != nil {
				return err
			}
			return json.NewEncoder(c.App.Writer).Encode(out)
		}}
}

func run9GCCheckpoint(ctx context.Context, req run9GCCheckpointRequest) (run9GCCheckpointOutput, error) {
	var out run9GCCheckpointOutput
	m, err := meta.OpenRun9Checkpoint(req.Directory)
	if err != nil {
		return out, err
	}
	defer m.Shutdown()
	format, err := m.Load(true)
	if err != nil {
		return out, err
	}
	end, err := m.Run9SliceAllocationCounter()
	if err != nil {
		return out, err
	}
	if format.Name != req.Format || req.AllocationEnd > end {
		return out, fmt.Errorf("checkpoint identity or allocation bound mismatch")
	}
	for _, r := range req.Ranges {
		if r.EndInclusive >= req.AllocationEnd {
			return out, fmt.Errorf("range exceeds checkpoint allocation bound")
		}
	}
	retained := make(map[uint64]bool)
	if !req.Deleted && len(req.Ranges) != 0 {
		err = m.ScanRun9CheckpointSlices(ctx, func(id uint64) {
			if sliceIDInRanges(id, req.Ranges) {
				retained[id] = true
			}
		})
		if err != nil {
			return out, err
		}
	}
	// Even a close error must occur before the first deletion.
	if err := m.Shutdown(); err != nil {
		return out, err
	}
	// Every round stores each fork checkpoint as its list, so the caller can
	// drop the metadata copy even while nothing is deletable. A fork checkpoint
	// that cannot be used keeps protecting the owner the way its child did
	// before fork checkpoints: by everything below the child's fork bound.
	var fallbackFloor uint64
	scanPostponed := false
	scan := run9ForkCheckpointScan{remaining: run9ForkCheckpointScanBudget, failed: make(map[string]string)}
	for _, failure := range req.ForkCheckpointFailures {
		scan.failed[failure.Directory] = failure.Reason
	}
	// Every attempted scan becomes either a cached list or a remembered failure.
	// Both bypass the scan budget on retry, so no separate scan cursor is needed.
	for _, forkCheckpoint := range req.ForkCheckpoints {
		ids, err := run9ForkCheckpointSlices(ctx, forkCheckpoint.Directory, req.Format, &scan)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
		if err != nil {
			out.FallbackForkCheckpoints = append(out.FallbackForkCheckpoints, run9FallbackForkCheckpoint{forkCheckpoint.Directory, err.Error()})
			fallbackFloor = max(fallbackFloor, forkCheckpoint.SourceAllocationEnd)
			scanPostponed = scanPostponed || errors.Is(err, errRun9ForkCheckpointScanPostponed)
			if !errors.Is(err, errRun9ForkCheckpointScanPostponed) {
				scan.failed[forkCheckpoint.Directory] = err.Error()
			}
			continue
		}
		delete(scan.failed, forkCheckpoint.Directory)
		for _, id := range ids {
			if sliceIDInRanges(id, req.Ranges) {
				retained[id] = true
			}
		}
	}
	var ranges []run9GCSliceRange
	for _, r := range req.Ranges {
		r.Start = max(r.Start, fallbackFloor)
		if r.Start <= r.EndInclusive {
			ranges = append(ranges, r)
		}
	}
	// Give object deletion its full budget after strict scanning succeeds.
	deadline := time.Now().Add(2 * time.Minute)
	ctx, cancel := context.WithDeadline(ctx, deadline.Add(15*time.Second))
	defer cancel()
	out.run9GCSliceRangesOutput, err = run9GCSliceRanges(ctx, run9GCSliceRangesRequest{
		JuiceFSFormatName: req.Format, ObjectLayout: run9ObjectLayout{format.BlockSize * 1024, format.HashPrefix},
		ObjectStorage: run9ObjectStorageDescriptorFromFormat(*format), Ranges: ranges,
		After: req.After, RetainSliceIDs: retained, DryRun: req.DryRun, WorkDeadline: deadline, MaxDeleteObjects: 65536, Threads: 4,
	})
	if scanPostponed {
		// The round stays pending until every fork checkpoint has been scanned,
		// and starts over so that nothing this round's fallback hid is skipped.
		out.HasMore, out.Next = true, ""
	}
	if out.HasMore {
		// Remember failed scans only through this pass. Otherwise two broken
		// copies could alternately exhaust every retry's budget forever. They
		// still protect by F, also while object enumeration resumes; a later
		// independent pass retries them.
		for _, checkpoint := range req.ForkCheckpoints {
			if reason, failed := scan.failed[checkpoint.Directory]; failed {
				out.ForkCheckpointFailures = append(out.ForkCheckpointFailures, run9FallbackForkCheckpoint{checkpoint.Directory, reason})
			}
		}
	}
	return out, err
}

// Strict list command is also used by the explicit, one-time boundary backfill.
func cmdRun9CheckpointSlices() *cli.Command {
	return &cli.Command{Name: "checkpoint-slices", Hidden: true, Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return fmt.Errorf("checkpoint-slices requires an immutable metadata directory")
		}
		m, err := meta.OpenRun9Checkpoint(c.Args().First())
		if err != nil {
			return err
		}
		defer m.Shutdown()
		ids, err := m.Run9CheckpointSlices(c.Context)
		if err != nil {
			return err
		}
		if err := m.Shutdown(); err != nil {
			return err
		}
		return json.NewEncoder(c.App.Writer).Encode(ids)
	}}
}
