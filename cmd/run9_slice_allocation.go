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

// run9ForkProtectionFDEnv names an inherited pipe. run9rt reads the lineage
// while this process starts and then writes one run9ForkProtectionRequest.
const run9ForkProtectionFDEnv = "JFS_RUN9_FORK_PROTECTION_FD"

// run9ForkProtectionRequest carries checkpoint GC's current direct-child
// proofs. Its size grows with the number of children.
type run9ForkProtectionRequest struct {
	ForkCheckpoints []run9ForkCheckpoint `json:"fork_checkpoints"`
	ForkFloor       uint64               `json:"fork_floor"`
}

// run9OwnedBytesRequest is what run9rt knows before launch. A per-mount
// private file keeps it out of the process's argument/environment limit.
// Its protection applies only when no pipe is inherited: run9rt puts a
// conservative floor there, so a JuiceFS that ignores the pipe never counts
// Fork data as visible, while an older run9rt sends its proofs there.
type run9OwnedBytesRequest struct {
	run9ForkProtectionRequest
	LastClosedRaw   *int64                     `json:"last_closed_raw"`
	Basis           uint64                     `json:"basis"`
	OwnershipRanges []meta.Run9OwnedEpochRange `json:"ownership_ranges"`
}

// beginRun9OwnedBytes starts counting without delaying the mount: Fork
// protection and the initial count follow in the background. An unavailable
// count never fails the mount.
func beginRun9OwnedBytes(m meta.Meta, epoch uint64, start uint64) error {
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
	receive := func(context.Context) (run9ForkProtectionRequest, error) {
		return request.run9ForkProtectionRequest, nil
	}
	if raw := os.Getenv(run9ForkProtectionFDEnv); raw != "" {
		fd, err := strconv.Atoi(raw)
		if err != nil || fd < 3 {
			return fmt.Errorf("invalid %s", run9ForkProtectionFDEnv)
		}
		receive = receiveRun9ForkProtection(adoptInheritedFile(fd, "fork-protection"))
	}
	format := m.GetFormat().Name
	cfg := meta.Run9OwnedBytesConfig{
		Epoch: epoch, AllocationStart: start, LastClosedRaw: request.LastClosedRaw,
		Basis: request.Basis, OwnershipRanges: request.OwnershipRanges,
		Protection: func(ctx context.Context) (meta.Run9ForkProtection, error) {
			request, err := receive(ctx)
			if err != nil {
				return meta.Run9ForkProtection{}, err
			}
			return resolveRun9ForkProtection(ctx, request, format)
		},
	}
	// Tests shorten the rest between recounts.
	if raw := os.Getenv("JFS_RUN9_OWNED_RECOUNT_INTERVAL"); raw != "" {
		var err error
		if meta.Run9OwnedRecountInterval, err = time.ParseDuration(raw); err != nil || meta.Run9OwnedRecountInterval <= 0 {
			return fmt.Errorf("invalid JFS_RUN9_OWNED_RECOUNT_INTERVAL")
		}
	}
	if err := m.(interface {
		Run9TrackOwnedBytes(meta.Run9OwnedBytesConfig) error
	}).Run9TrackOwnedBytes(cfg); err != nil {
		logger.Warnf("owned bytes unavailable: %s", err)
	}
	return nil
}

// receiveRun9ForkProtection starts draining the pipe at once, so run9rt never
// blocks on a large inventory, and returns how initialization waits for it
// within its own budget. A pipe closed without a complete request fails
// initialization: protection is never assumed empty.
func receiveRun9ForkProtection(pipe *os.File) func(context.Context) (run9ForkProtectionRequest, error) {
	type received struct {
		request run9ForkProtectionRequest
		err     error
	}
	result := make(chan received, 1)
	go func() {
		defer pipe.Close()
		var r received
		r.err = json.NewDecoder(pipe).Decode(&r.request)
		result <- r
	}()
	return func(ctx context.Context) (run9ForkProtectionRequest, error) {
		select {
		case r := <-result:
			if r.err != nil {
				return run9ForkProtectionRequest{}, fmt.Errorf("receive fork protection: %w", r.err)
			}
			return r.request, nil
		case <-ctx.Done():
			return run9ForkProtectionRequest{}, ctx.Err()
		}
	}
}

// resolveRun9ForkProtection reads GC's validated cached slice lists, scanning
// a child's immutable metadata when none is stored yet. A checkpoint it cannot
// use protects by that child's allocation bound instead.
func resolveRun9ForkProtection(ctx context.Context, request run9ForkProtectionRequest, format string) (meta.Run9ForkProtection, error) {
	protection := meta.Run9ForkProtection{Floor: request.ForkFloor}
	scan := run9ForkCheckpointScan{remaining: run9ForkCheckpointScanBudget, failed: make(map[string]string)}
	for _, checkpoint := range request.ForkCheckpoints {
		if err := ctx.Err(); err != nil {
			return meta.Run9ForkProtection{}, err
		}
		ids, err := run9ForkCheckpointSlices(ctx, checkpoint.Directory, format, &scan)
		if err != nil {
			protection.Floor = max(protection.Floor, checkpoint.SourceAllocationEnd)
			logger.Warnf("owned bytes: keep fork checkpoint %s by allocation floor: %s", checkpoint.Directory, err)
			continue
		}
		protection.IDs = append(protection.IDs, ids...)
	}
	return protection, nil
}
