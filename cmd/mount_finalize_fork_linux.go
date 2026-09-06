//go:build linux
// +build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

const forkFinalizeDefaultTimeout = 10 * time.Minute

var forkFinalizeFlushAll = func(v *vfs.VFS) error {
	return v.FlushAll("")
}

var forkFinalizeMeasureSize = func(v *vfs.VFS) (uint64, uint64, error) {
	if v.Meta == nil {
		return 0, 0, errors.New("metadata client is unavailable")
	}
	var totalSpace, availableSpace, usedInodes, availableInodes uint64
	if errno := v.Meta.StatFS(meta.Background(), meta.RootInode, &totalSpace, &availableSpace, &usedInodes, &availableInodes); errno != 0 {
		return 0, 0, fmt.Errorf("statfs: %s", errno)
	}
	if availableSpace > totalSpace {
		return 0, 0, fmt.Errorf("statfs available space %d exceeds total space %d", availableSpace, totalSpace)
	}
	return totalSpace - availableSpace, usedInodes, nil
}

func forkFinalizeDaemonTimeout(requested time.Duration) time.Duration {
	if requested <= 0 {
		return forkFinalizeDefaultTimeout
	}

	// Leave headroom so the daemon can still write the terminal ack before the
	// caller's --finalize-timeout expires.
	slack := 5 * time.Second
	if requested/10 < slack {
		slack = requested / 10
	}
	if slack <= 0 || slack >= requested {
		return requested
	}
	return requested - slack
}

func forkFinalizePhaseTimeoutErr(finalizeTimeout, requestedTimeout time.Duration, err error) error {
	return fmt.Errorf("timeout after %s (requested %s): %w", finalizeTimeout, requestedTimeout, err)
}

type forkFinalizeBlockingPhaseResult struct {
	err   error
	panic any
}

func runForkFinalizeBlockingPhase(ctx context.Context, finalizeTimeout, requestedTimeout time.Duration, fn func() error) error {
	done := make(chan forkFinalizeBlockingPhaseResult, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- forkFinalizeBlockingPhaseResult{panic: r}
			}
		}()
		done <- forkFinalizeBlockingPhaseResult{err: fn()}
	}()

	select {
	case result := <-done:
		if result.panic != nil {
			panic(result.panic)
		}
		return result.err
	case <-ctx.Done():
		return forkFinalizePhaseTimeoutErr(finalizeTimeout, requestedTimeout, ctx.Err())
	}
}

// installForkFinalizeHandler installs the fork-only finalize signal handler for `juicefs umount-finalize`.
//
// On SIGUSR2, it requests a one-shot correctness-critical finalize. The signal handler only
// initiates a force umount so `mountMain` can return through the normal mount exit path; the
// main mount goroutine then performs the actual finalize and writes the ack.
func installForkFinalizeHandler(_ meta.Meta, v *vfs.VFS, _ object.ObjectStorage) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR2)
	go func() {
		for range signalChan {
			if !forkFinalizeInProgress.CompareAndSwap(false, true) {
				logger.Infof("Received SIGUSR2 but finalize is already in progress")
				continue
			}
			forkMountLifecycle.Lock()
			if err := writeForkFinalizePendingAckForCurrentProcess(); err != nil {
				logger.Warnf("finalize: write pending ack: %s", err)
			}
			logger.Infof("Received SIGUSR2, requesting finalize via force umount")
			if err := doUmount(v.Conf.Meta.MountPoint, true); err != nil {
				logger.Warnf("finalize: force umount: %s", err)
				forkFinalizeInProgress.Store(false)
				forkMountLifecycle.Unlock()
			}
		}
	}()
}

func runForkFinalizeOnMain(metaCli sessionShutdowner, v *vfs.VFS, blob object.ObjectStorage, sliceAllocationStart *uint64) (resultErr error) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return fmt.Errorf("finalize: read /proc/self/stat: %w", err)
	}
	ackPath := forkFinalizeAckPath(pid, starttimeTicks)
	reqPath := forkFinalizeRequestPath(pid, starttimeTicks)

	requestedTimeout := forkFinalizeDefaultTimeout
	req, err := readForkFinalizeRequest(reqPath)
	if err == nil {
		requestedTimeout, err = time.ParseDuration(req.FinalizeTimeout)
		if err != nil || requestedTimeout <= 0 {
			logger.Warnf("finalize: request has invalid finalize_timeout %q: %v", req.FinalizeTimeout, err)
			requestedTimeout = forkFinalizeDefaultTimeout
		}
		if err := os.Remove(reqPath); err != nil && !os.IsNotExist(err) {
			logger.Warnf("finalize: remove request: %s", err)
		}
	} else if !os.IsNotExist(err) {
		logger.Warnf("finalize: read request: %s", err)
	}

	finalizeTimeout := forkFinalizeDaemonTimeout(requestedTimeout)
	finalizeCtx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
	}

	writePendingAck := func(phase string) {
		ack.Status = "pending"
		ack.Phase = phase
		ack.Error = ""
		if err := writeForkFinalizeAck(ackPath, ack); err != nil {
			logger.Warnf("finalize: write pending ack phase=%s: %s", phase, err)
		}
	}

	var firstPhase string
	var firstErr error
	recordErr := func(phase string, err error) {
		if err == nil {
			return
		}
		if firstErr == nil {
			firstPhase = phase
			firstErr = err
			return
		}
		logger.Errorf("finalize: %s: %s", phase, err)
	}

	defer func() {
		if r := recover(); r != nil {
			ack.Status = "panic"
			ack.Phase = ""
			ack.Error = fmt.Sprintf("panic: %v", r)
			resultErr = fmt.Errorf("finalize panic: %v", r)
		}

		if ack.Status != "panic" {
			if firstErr == nil {
				ack.Status = "ok"
				ack.SliceAllocationStart = sliceAllocationStart
				ack.Phase = ""
				ack.Error = ""
			} else {
				ack.Status = "error"
				ack.Phase = firstPhase
				ack.Error = firstErr.Error()
				if resultErr == nil {
					resultErr = firstErr
				}
			}
		}
		ack.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := writeForkFinalizeAck(ackPath, ack); err != nil {
			logger.Errorf("finalize: write ack: %s", err)
			if resultErr == nil {
				resultErr = fmt.Errorf("finalize: write ack: %w", err)
			}
		}
	}()

	writePendingAck("flush_all")
	recordErr("flush_all", runForkFinalizeBlockingPhase(finalizeCtx, finalizeTimeout, requestedTimeout, func() error {
		return forkFinalizeFlushAll(v)
	}))
	if finalizeCtx.Err() != nil {
		return resultErr
	}

	if v.Conf != nil && v.Conf.Chunk != nil && v.Conf.Chunk.Writeback {
		drainer, ok := v.Store.(interface {
			WaitForUploadDrain(context.Context) error
		})
		if !ok {
			writePendingAck("upload_drain")
			recordErr("upload_drain", errors.New("chunk store does not support WaitForUploadDrain"))
		} else {
			writePendingAck("upload_drain")
			err := drainer.WaitForUploadDrain(finalizeCtx)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				err = forkFinalizePhaseTimeoutErr(finalizeTimeout, requestedTimeout, err)
			}
			recordErr("upload_drain", err)
		}
		if finalizeCtx.Err() != nil {
			return resultErr
		}
	}

	writePendingAck("measure_size")
	usedBytes, usedInodes, sizeErr := forkFinalizeMeasureSize(v)
	if sizeErr != nil {
		ack.SizeError = sizeErr.Error()
	} else {
		ack.UsedBytes = &usedBytes
		ack.UsedInodes = &usedInodes
		ack.SizeMeasuredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	writePendingAck("close_session")
	recordErr("close_session", runForkFinalizeBlockingPhase(finalizeCtx, finalizeTimeout, requestedTimeout, func() error {
		return metaCli.CloseSession()
	}))
	if finalizeCtx.Err() != nil {
		return resultErr
	}

	writePendingAck("shutdown")
	recordErr("shutdown", runForkFinalizeBlockingPhase(finalizeCtx, finalizeTimeout, requestedTimeout, func() error {
		return metaCli.Shutdown()
	}))
	// Process exit will reclaim object-storage resources. Do not add another
	// best-effort shutdown after metadata finalize: a late panic or fatal exit
	// here would suppress the finalize ack and turn a completed finalize into an
	// unprovable one for the caller.

	return resultErr
}

func writeForkFinalizePendingAckForCurrentProcess() error {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return fmt.Errorf("read /proc/self/stat: %w", err)
	}
	return writeForkFinalizeAck(forkFinalizeAckPath(pid, starttimeTicks), &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
		Status:            "pending",
		Phase:             "signal_received",
	})
}

func writeForkFinalizeAck(ackPath string, ack *forkFinalizeAckV1) error {
	ackDir := filepath.Dir(ackPath)
	if err := ensureForkFinalizeAckDir(ackDir); err != nil {
		return err
	}
	if fi, err := os.Lstat(ackPath); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("ack path is a symlink: %s", ackPath)
		}
		if fi.IsDir() {
			return fmt.Errorf("ack path is a directory: %s", ackPath)
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("ack path is not a regular file: %s", ackPath)
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	data, err := json.MarshalIndent(ack, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(ackDir, fmt.Sprintf("tmp-%d-%d-*.json", ack.Pid, ack.PidStarttimeTicks))
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, ackPath); err != nil {
		return err
	}
	if err := syncDir(ackDir); err != nil {
		// fsync the directory is optional (file fsync is the minimum requirement).
		logger.Warnf("finalize: fsync ack dir: %s", err)
	}
	return nil
}

func ensureForkFinalizeAckDir(dir string) error {
	fi, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		fi, err = os.Lstat(dir)
	}
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("ack dir is a symlink: %s", dir)
	}
	if !fi.IsDir() {
		return fmt.Errorf("ack dir is not a directory: %s", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type %T for %s", fi.Sys(), dir)
	}
	expectedUID := uint32(os.Geteuid())
	if st.Uid != expectedUID {
		return fmt.Errorf("ack dir owner uid mismatch (got %d, want %d): %s", st.Uid, expectedUID, dir)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
