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

// Each capture protocol has its own socket in the mount's capture directory,
// so a runtime learns which protocols a mount serves from the sockets that
// exist and never sends a request the mount would not understand.
const (
	// run9CaptureLegacySocket reports captured only after the source database
	// reopens. The runtime then prepares the copy and clones the Fork
	// checkpoint itself. It serves runtimes that predate the current protocol.
	run9CaptureLegacySocket = "control.sock"
	// run9CaptureSocket clones every child of one capture instant together
	// with its Fork checkpoint, reports captured before the source reopens,
	// and reports the source's usage at the capture instant as the copies'
	// size.
	run9CaptureSocket = "capture-v2.sock"
)

type run9CaptureRequest struct {
	// Directory receives the legacy protocol's only copy.
	Directory string `json:"directory,omitempty"`
	// Copies receive the current protocol's children, all cloned from one
	// closed file set, so that one capture serves every child of a source.
	Copies         []run9CaptureCopy `json:"copies,omitempty"`
	DeadlineUnixMS int64             `json:"deadline_unix_ms"`
}

// run9CaptureCopy is one child of a current-protocol capture.
type run9CaptureCopy struct {
	Directory string `json:"directory"`
	// CheckpointDirectory receives the child's Fork checkpoint.
	CheckpointDirectory string `json:"checkpoint_directory"`
}

// run9CaptureMeta is the metadata a managed writable mount captures from.
type run9CaptureMeta interface {
	Run9WaitForkRecord(context.Context) error
	Run9CaptureMetadata(context.Context, string, meta.Run9CaptureOptions) (meta.Run9MetadataCapture, *meta.Run9ForkRecord, error)
}

type run9CaptureResponse struct {
	Phase    string                   `json:"phase"`
	Error    string                   `json:"error,omitempty"`
	Code     string                   `json:"code,omitempty"`
	Metadata meta.Run9MetadataCapture `json:"metadata"`
	Uploads  *chunk.PendingUploads    `json:"uploads,omitempty"`
	FlushMS  int64                    `json:"flush_ms"`
	// Usage is the source's usage at the capture instant, read while writes
	// are suspended: the copy's usage before its first mount. Current
	// protocol only.
	Usage *meta.Run9Usage `json:"usage,omitempty"`
}

// The owner-local sockets serve only managed writable mounts. The coordinator
// synchronizes the guest and pauses the VM before capture, then resumes on the
// captured response. This server knows storage, not VM or product lifecycle.
func installRun9Capture(v *vfs.VFS, allocationStart *uint64) (func(), error) {
	if allocationStart == nil {
		return func() {}, nil
	}
	if _, ok := v.Meta.(run9CaptureMeta); !ok {
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
	ctx, cancel := context.WithCancel(context.Background())
	var listeners []net.Listener
	var handlers sync.WaitGroup
	stop := func() {
		cancel()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		handlers.Wait()
		_ = os.RemoveAll(directory)
	}
	for _, protocol := range []struct {
		socket  string
		current bool
	}{{run9CaptureLegacySocket, false}, {run9CaptureSocket, true}} {
		listener, err := net.Listen("unix", filepath.Join(directory, protocol.socket))
		if err != nil {
			stop()
			return nil, err
		}
		listeners = append(listeners, listener)
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
					if err := serveRun9Capture(ctx, conn, encoder, v, protocol.current); errors.Is(err, meta.ErrRun9MetadataUnavailable) {
						// Source recovery failed. Exit without finalize or GC
						// authorization and let the existing mount-loss path converge.
						logger.Fatalf("online capture lost source metadata: %s", err)
					}
				}()
			}
		}()
	}
	return stop, nil
}

// serveRun9Capture serves one capture request in the legacy or the current
// protocol. Both report captured once the copy exists and ready once the fixed
// upload set is durable; the current protocol reports captured before the
// source reopens, so ready never waits for the reopen either.
func serveRun9Capture(parent context.Context, conn net.Conn, encoder *json.Encoder, v *vfs.VFS, current bool) (resultErr error) {
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
	var directory string
	var copies []string
	if current {
		if request.Directory != "" || len(request.Copies) == 0 {
			return fmt.Errorf("capture requires copies and takes no directory")
		}
		for i, c := range request.Copies {
			if c.Directory == "" || c.CheckpointDirectory == "" {
				return fmt.Errorf("capture copy requires a directory and a checkpoint directory")
			}
			if i != 0 {
				copies = append(copies, c.Directory)
			}
			copies = append(copies, c.CheckpointDirectory)
		}
		directory = request.Copies[0].Directory
	} else {
		if request.Directory == "" || len(request.Copies) != 0 {
			return fmt.Errorf("legacy capture takes one directory")
		}
		directory = request.Directory
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
	var record *meta.Run9ForkRecord
	// ready carries the result of reporting ready once the current protocol
	// has reported captured; the encoder then has that one writer until it ends.
	var ready chan error
	if err := func() (err error) {
		// The coordinator serializes captures of a VM, but resumes it as soon
		// as the previous capture reports its copy, possibly while the source
		// still reopens; wait for that, or for a flush, within the deadline.
		select {
		case forkMountPublish <- struct{}{}:
		case <-ctx.Done():
			return fmt.Errorf("wait for the previous capture or flush: %w", ctx.Err())
		}
		defer func() { <-forkMountPublish }()
		metaCapture := v.Meta.(run9CaptureMeta)
		if err := metaCapture.Run9WaitForkRecord(ctx); err != nil {
			return err
		}
		resumeWrites := v.SuspendWrites()
		defer func() {
			// A source that cannot serve more I/O is lost, and the mount exits
			// once this capture has reported. Keep writes suspended until then:
			// a write admitted meanwhile would be acknowledged to the resumed
			// VM and then dropped with the mount.
			if !errors.Is(err, meta.ErrRun9MetadataUnavailable) {
				resumeWrites()
			}
		}()
		if err := ctx.Err(); err != nil {
			return err
		}
		start := time.Now()
		if err := v.FlushAll(""); err != nil {
			return err
		}
		v.Meta.FlushSession()
		response.FlushMS = time.Since(start).Milliseconds()
		options := meta.Run9CaptureOptions{Copies: copies}
		var capturedErr error
		if current {
			var total, available, availableInodes uint64
			response.Usage = &meta.Run9Usage{}
			if st := v.Meta.StatFS(meta.Background(), meta.RootInode, &total, &available, &response.Usage.UsedInodes, &availableInodes); st != 0 {
				return st
			}
			response.Usage.UsedSpace = total - available
			// The copies are complete: the VM may resume while the source
			// reopens, since guest I/O waits on the suspended writes and the
			// database lock, never on a half-closed database.
			options.Cloned = func(capture meta.Run9MetadataCapture) {
				response.Phase, response.Metadata = "captured", capture
				if capturedErr = encoder.Encode(response); capturedErr != nil {
					return
				}
				ready = make(chan error, 1)
				go func(response run9CaptureResponse) { ready <- reportRun9Ready(ctx, encoder, response) }(response)
			}
		}
		response.Uploads = v.Store.(interface{ CaptureUploads() *chunk.PendingUploads }).CaptureUploads()
		response.Metadata, record, err = metaCapture.Run9CaptureMetadata(ctx, directory, options)
		if err != nil {
			return err
		}
		if capturedErr != nil {
			return capturedErr
		}
		if ready != nil {
			// The copies are reported, so the Fork proceeds as far as this
			// mount is concerned, even if the client has already read ready
			// and gone while the source reopened. Guest I/O waited for this
			// reopen after the VM resumed.
			logger.Infof("online capture: source reopened in %d ms after reporting the copy", response.Metadata.OpenMS)
			return nil
		}
		return ctx.Err()
	}(); err != nil {
		record.Discard()
		if ready != nil {
			// A child reported ready stays valid; the source's own failure
			// follows it on the socket.
			err = errors.Join(err, <-ready)
		}
		return err
	}
	// The Fork proceeds. Its owned-bytes record scans in the background.
	record.Start()
	if ready != nil {
		return <-ready
	}
	// The capture exclusion and source database lock have both been released.
	// Other captures may now proceed while this fixed collection uploads.
	response.Phase = "captured"
	if err := encoder.Encode(response); err != nil {
		return err
	}
	return reportRun9Ready(ctx, encoder, response)
}

// reportRun9Ready reports ready once the capture's fixed upload set is durable.
func reportRun9Ready(ctx context.Context, encoder *json.Encoder, response run9CaptureResponse) error {
	if err := response.Uploads.Wait(ctx); err != nil {
		return err
	}
	response.Phase = "ready"
	return encoder.Encode(response)
}
