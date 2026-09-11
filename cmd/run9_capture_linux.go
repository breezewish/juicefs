//go:build linux && !nobadger

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

type run9CaptureRequest struct {
	Directory      string `json:"directory"`
	DeadlineUnixMS int64  `json:"deadline_unix_ms"`
}

type run9CaptureResponse struct {
	Phase    string                   `json:"phase"`
	Error    string                   `json:"error,omitempty"`
	Code     string                   `json:"code,omitempty"`
	Metadata meta.Run9MetadataCapture `json:"metadata"`
	Uploads  *chunk.PendingUploads    `json:"uploads,omitempty"`
	FlushMS  int64                    `json:"flush_ms"`
}

// The owner-local socket serves only managed writable mounts. The coordinator
// synchronizes the guest and pauses the VM before capture, then resumes on the
// captured response. This server knows storage, not VM or product lifecycle.
func installRun9Capture(v *vfs.VFS, allocationStart *uint64) (func(), error) {
	if allocationStart == nil {
		return func() {}, nil
	}
	if _, ok := v.Meta.(interface {
		Run9CaptureMetadata(context.Context, string) (meta.Run9MetadataCapture, error)
	}); !ok {
		return nil, fmt.Errorf("managed mount lacks metadata capture")
	}
	ticks, err := readProcStatStarttimeTicks(os.Getpid())
	if err != nil {
		return nil, err
	}
	directory := filepath.Join("/tmp", fmt.Sprintf("run9-capture-%d-%d", os.Getpid(), ticks))
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", filepath.Join(directory, "control.sock"))
	if err != nil {
		_ = os.Remove(directory)
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	var handlers sync.WaitGroup
	handlers.Add(1)
	go func() {
		defer handlers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				defer conn.Close()
				encoder := json.NewEncoder(conn)
				if err := serveRun9Capture(ctx, conn, encoder, v, allocationStart); errors.Is(err, meta.ErrRun9MetadataUnavailable) {
					// Source recovery failed. Exit without finalize or GC
					// authorization and let the existing mount-loss path converge.
					logger.Fatalf("online capture lost source metadata: %s", err)
				}
			}()
		}
	}()
	return func() {
		cancel()
		_ = listener.Close()
		handlers.Wait()
		_ = os.RemoveAll(directory)
	}, nil
}

func serveRun9Capture(parent context.Context, conn net.Conn, encoder *json.Encoder, v *vfs.VFS, allocationStart *uint64) (resultErr error) {
	var cancel context.CancelFunc
	var readerDone chan struct{}
	defer func() {
		// Send the outcome before cancellation closes the socket. This also
		// covers malformed requests before the cancellation reader is started.
		if resultErr != nil {
			response := run9CaptureResponse{Phase: "error", Error: resultErr.Error()}
			if errors.Is(resultErr, meta.ErrRun9MetadataUnavailable) {
				response.Code = "source_unavailable"
			}
			_ = encoder.Encode(response)
		}
		if cancel != nil {
			cancel()
		}
		_ = conn.Close()
		if readerDone != nil {
			<-readerDone
		}
	}()
	// Bound idle clients, including one that connects but never sends a request.
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var request run9CaptureRequest
	if err := json.NewDecoder(io.LimitReader(conn, 16<<10)).Decode(&request); err != nil {
		return err
	}
	deadline := time.UnixMilli(request.DeadlineUnixMS)
	if !deadline.After(time.Now()) || deadline.After(time.Now().Add(30*time.Minute)) {
		return fmt.Errorf("capture deadline must be within 30 minutes")
	}
	_ = conn.SetDeadline(deadline)
	ctx, cancelRequest := context.WithDeadline(parent, deadline)
	cancel = cancelRequest
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	readerDone = make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _ = io.Copy(io.Discard, conn)
		cancel()
	}()
	// Finalize holds the write side. TryRLock avoids admitting a new handler
	// behind shutdown, whose final cleanup also joins these handlers.
	if !forkMountLifecycle.TryRLock() {
		return fmt.Errorf("mount is finalizing")
	}
	defer forkMountLifecycle.RUnlock()
	if forkFinalizeInProgress {
		return fmt.Errorf("mount is finalizing")
	}
	var response run9CaptureResponse
	if err := func() error {
		if !forkFlushDrainInProgress.CompareAndSwap(false, true) {
			return fmt.Errorf("mount capture or flush is busy")
		}
		defer forkFlushDrainInProgress.Store(false)
		resumeWrites := v.SuspendWrites()
		defer resumeWrites()
		if err := ctx.Err(); err != nil {
			return err
		}
		start := time.Now()
		if err := v.FlushAll(""); err != nil {
			return err
		}
		v.Meta.FlushSession()
		response.FlushMS = time.Since(start).Milliseconds()
		response.Uploads = v.Store.(interface{ CaptureUploads() *chunk.PendingUploads }).CaptureUploads()
		var err error
		response.Metadata, err = v.Meta.(interface {
			Run9CaptureMetadata(context.Context, string) (meta.Run9MetadataCapture, error)
		}).Run9CaptureMetadata(ctx, request.Directory)
		if err != nil {
			return err
		}
		// Protected by the capture exclusion above and the finalize read lock.
		// Include unused preallocated IDs; never roll this conservative floor back.
		*allocationStart = max(*allocationStart, response.Metadata.AllocationEnd)
		return ctx.Err()
	}(); err != nil {
		return err
	}
	response.Phase = "captured"
	if err := encoder.Encode(response); err != nil {
		return err
	}
	// The capture exclusion and source database lock have both been released.
	// Other captures may now proceed while this fixed collection uploads.
	if err := response.Uploads.Wait(ctx); err != nil {
		return err
	}
	response.Phase = "ready"
	return encoder.Encode(response)
}
