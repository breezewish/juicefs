package cmd

import (
	"context"
	"encoding/json"
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
}

func cmdRun9GCCheckpoint() *cli.Command {
	return &cli.Command{Name: "gc-checkpoint", Hidden: true, Usage: "strictly scan a finalized checkpoint before range GC",
		Flags: []cli.Flag{&cli.StringFlag{Name: "request", Required: true}},
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

func run9GCCheckpoint(ctx context.Context, req run9GCCheckpointRequest) (run9GCSliceRangesOutput, error) {
	m, err := meta.OpenRun9Checkpoint(req.Directory)
	if err != nil {
		return run9GCSliceRangesOutput{}, err
	}
	defer m.Shutdown()
	format, err := m.Load(true)
	if err != nil {
		return run9GCSliceRangesOutput{}, err
	}
	end, err := m.Run9SliceAllocationCounter()
	if err != nil {
		return run9GCSliceRangesOutput{}, err
	}
	if format.Name != req.Format || req.AllocationEnd > end {
		return run9GCSliceRangesOutput{}, fmt.Errorf("checkpoint identity or allocation bound mismatch")
	}
	for _, r := range req.Ranges {
		if r.EndInclusive >= req.AllocationEnd {
			return run9GCSliceRangesOutput{}, fmt.Errorf("range exceeds checkpoint allocation bound")
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
			return run9GCSliceRangesOutput{}, err
		}
	}
	// Even a close error must occur before the first deletion.
	if err := m.Shutdown(); err != nil {
		return run9GCSliceRangesOutput{}, err
	}
	// Give object deletion its full budget after strict scanning succeeds.
	deadline := time.Now().Add(2 * time.Minute)
	ctx, cancel := context.WithDeadline(ctx, deadline.Add(15*time.Second))
	defer cancel()
	return run9GCSliceRanges(ctx, run9GCSliceRangesRequest{
		JuiceFSFormatName: req.Format, ObjectLayout: run9ObjectLayout{format.BlockSize * 1024, format.HashPrefix},
		ObjectStorage: run9ObjectStorageDescriptorFromFormat(*format), Ranges: req.Ranges,
		After: req.After, RetainSliceIDs: retained, DryRun: req.DryRun, WorkDeadline: deadline, MaxDeleteObjects: 65536, Threads: 4,
	})
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
