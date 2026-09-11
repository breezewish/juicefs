//go:build linux && run9_checkpoint_research && !nobadger

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// This owner-local research socket is absent from production builds. The
// experiment coordinator must synchronize the guest and confirm its VM is
// stopped before requesting capture. It resumes the VM on captured or error.
func installResearchCheckpoint(v *vfs.VFS, allocationStart *uint64) (func(), error) {
	checkpoint, ok := v.Meta.(interface {
		Run9Checkpoint(context.Context, meta.Run9CheckpointOptions, func(uint64) error) (meta.Run9CheckpointResult, error)
	})
	if !ok || allocationStart == nil {
		return func() {}, nil
	}
	if os.Getenv("RUN9_RESEARCH_STAGE_ALL") == "1" {
		v.Store.(interface{ Run9StageAllWrites() }).Run9StageAllWrites()
	}
	if raw := os.Getenv("RUN9_RESEARCH_UPLOAD_LIMIT_MBPS"); raw != "" {
		limit, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || limit <= 0 {
			return nil, fmt.Errorf("invalid research upload limit %q", raw)
		}
		v.Store.UpdateLimit(limit, 0)
	}
	ticks, err := readProcStatStarttimeTicks(os.Getpid())
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("run9-checkpoint-%d-%d", os.Getpid(), ticks))
	if err := os.Mkdir(dir, 0700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", filepath.Join(dir, "control.sock"))
	if err != nil {
		_ = os.Remove(dir)
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
				encoder := json.NewEncoder(conn)
				var request struct {
					meta.Run9CheckpointOptions
					TimeoutMS        int  `json:"timeout_ms"`
					SkipGCProtection bool `json:"skip_gc_protection"` // negative control
				}
				reportError := func(err error) { _ = encoder.Encode(map[string]any{"phase": "error", "error": err.Error()}) }
				if err := json.NewDecoder(conn).Decode(&request); err != nil {
					reportError(err)
					return
				}
				if request.TimeoutMS <= 0 || request.TimeoutMS > 30000 {
					reportError(fmt.Errorf("timeout_ms must be 1..30000"))
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.TimeoutMS)*time.Millisecond)
				defer cancel()
				go func() {
					// Closing the client connection cancels export. The deferred
					// connection close also releases this reader on normal exit.
					_, _ = io.Copy(io.Discard, conn)
					cancel()
				}()
				forkMountLifecycle.RLock()
				defer forkMountLifecycle.RUnlock()
				if forkFinalizeInProgress || !forkFlushDrainInProgress.CompareAndSwap(false, true) {
					reportError(fmt.Errorf("mount lifecycle busy"))
					return
				}
				defer forkFlushDrainInProgress.Store(false)
				start := time.Now()
				unlockWrites := v.Run9CheckpointWriteBarrier()
				writesLocked := true
				defer func() {
					if writesLocked {
						unlockWrites()
					}
				}()
				if err := v.FlushAll(""); err != nil {
					reportError(err)
					return
				}
				drainer, ok := v.Store.(interface{ WaitForUploadDrain(context.Context) error })
				if !ok {
					reportError(fmt.Errorf("missing upload drain"))
					return
				}
				var uploads *chunk.Run9UploadFence
				if request.Strategy == "checkpoint-async" || request.Strategy == "logical-async" || request.Strategy == "physical-async" {
					uploads = v.Store.(interface{ Run9CaptureUploads() *chunk.Run9UploadFence }).Run9CaptureUploads()
				} else {
					if err := drainer.WaitForUploadDrain(ctx); err != nil {
						reportError(err)
						return
					}
				}
				v.Meta.FlushSession()
				flushMS := float64(time.Since(start).Microseconds()) / 1000
				out, err := checkpoint.Run9Checkpoint(ctx, request.Run9CheckpointOptions, func(counter uint64) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					// CnextChunk is the exclusive reservation limit, so this also
					// protects unspent IDs in the current preallocated batch.
					// The lifecycle read lock keeps finalize from reading this
					// floor until export finishes. Requests are serialized here.
					if !request.SkipGCProtection {
						*allocationStart = max(*allocationStart, counter)
					}
					unlockWrites()
					writesLocked = false
					return encoder.Encode(map[string]any{"phase": "captured", "counter": counter, "flush_ms": flushMS, "uploads": uploads})
				})
				if err != nil {
					reportError(err)
					return
				}
				if uploads != nil {
					if err := uploads.Wait(ctx); err != nil {
						reportError(err)
						return
					}
				}
				remaining := v.Store.(interface{ Run9CaptureUploads() *chunk.Run9UploadFence }).Run9CaptureUploads()
				_ = encoder.Encode(map[string]any{"phase": "complete", "result": out, "uploads": uploads, "remaining_uploads": remaining, "flush_ms": flushMS, "total_ms": float64(time.Since(start).Microseconds()) / 1000})
			}()
		}
	}()
	return func() { _ = listener.Close(); _ = os.RemoveAll(dir) }, nil
}
