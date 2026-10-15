//go:build linux

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
)

var forkFlushDrainFlushAll = func(v *vfs.VFS) error { return v.FlushAll("") }

// installForkFlushDrain serves request-scoped publish fences on an owner-only
// Unix socket. Shutdown joins handlers; FlushAll must finish before teardown,
// even when the requesting connection disappears or its deadline expires.
func installForkFlushDrain(v *vfs.VFS) (func(), error) {
	pid := os.Getpid()
	ticks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return nil, err
	}
	socket := forkFlushDrainSocketPath(pid, ticks)
	directory := filepath.Dir(socket)
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		_ = os.Remove(directory)
		return nil, err
	}
	server := &http.Server{
		ReadTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/flush" {
				http.NotFound(w, r)
				return
			}
			var request forkFlushDrainRequest
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			deadline := time.UnixMilli(request.DeadlineUnixMS)
			ctx, cancel := context.WithDeadline(r.Context(), deadline)
			defer cancel()
			result := runForkFlushDrain(ctx, v)
			result.Ack.Pid = pid
			result.Ack.PidStarttimeTicks = ticks
			if !result.Ok {
				logger.Errorf("flush-drain: phase=%s: %s", result.Ack.Phase, result.Reason)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
		}),
	}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Errorf("flush-drain server: %s", err)
		}
	}()
	return func() {
		_ = server.Shutdown(context.Background())
		_ = os.RemoveAll(directory)
	}, nil
}

func runForkFlushDrain(ctx context.Context, v *vfs.VFS) (result *forkFlushDrainResult) {
	ack := &forkFinalizeAckV1{SchemaVersion: 1, Status: "error", Phase: "queue"}
	result = &forkFlushDrainResult{Ack: ack}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Reason = fmt.Sprintf("panic: %v", recovered)
			ack.Status = "panic"
		}
		ack.Error = result.Reason
		ack.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}()
	select {
	case forkMountPublish <- struct{}{}:
		defer func() { <-forkMountPublish }()
	case <-ctx.Done():
		result.Reason = ctx.Err().Error()
		return result
	}
	// Never queue behind terminal teardown: it may be joining this server.
	if !forkMountLifecycle.TryRLock() {
		result.Reason = "mount is finalizing"
		return result
	}
	defer forkMountLifecycle.RUnlock()
	if forkFinalizeInProgress {
		result.Reason = "mount is finalizing"
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Reason = err.Error()
		return result
	}
	ack.Phase = "flush_all"
	if err := forkFlushDrainFlushAll(v); err != nil {
		result.Reason = err.Error()
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Reason = err.Error()
		return result
	}
	if v.Conf != nil && v.Conf.Chunk != nil && v.Conf.Chunk.Writeback {
		ack.Phase = "upload_drain"
		drainer, ok := v.Store.(interface{ WaitForUploadDrain(context.Context) error })
		if !ok {
			result.Reason = "chunk store does not support WaitForUploadDrain"
			return result
		}
		if err := drainer.WaitForUploadDrain(ctx); err != nil {
			result.Reason = err.Error()
			return result
		}
	}
	result.Ok, result.Drained = true, true
	ack.Status, ack.Phase = "ok", ""
	return result
}
