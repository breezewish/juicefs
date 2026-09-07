//go:build linux
// +build linux

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
)

var forkFlushDrainFlushAll = func(v *vfs.VFS) error {
	return v.FlushAll("")
}

// installForkFlushDrainHandler installs the fork-only flush-drain signal handler for `juicefs flush-drain`.
//
// On SIGUSR1, it flushes delayed VFS data and drains writeback uploads while keeping the mount alive.
// This gives callers a publish fence for deferred fsync mounts without changing mount lifecycle.
func installForkFlushDrainHandler(v *vfs.VFS) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGUSR1)
	go func() {
		for range signalChan {
			forkMountLifecycle.RLock()
			if forkFinalizeInProgress {
				forkMountLifecycle.RUnlock()
				logger.Infof("Received SIGUSR1 but finalize is already in progress")
				continue
			}
			if !forkFlushDrainInProgress.CompareAndSwap(false, true) {
				forkMountLifecycle.RUnlock()
				logger.Infof("Received SIGUSR1 but flush-drain is already in progress")
				continue
			}
			go func() {
				defer forkMountLifecycle.RUnlock()
				defer forkFlushDrainInProgress.Store(false)
				if err := runForkFlushDrain(v); err != nil {
					logger.Errorf("flush-drain: %s", err)
				}
			}()
		}
	}()
}

func runForkFlushDrain(v *vfs.VFS) (resultErr error) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return fmt.Errorf("flush-drain: read /proc/self/stat: %w", err)
	}
	ackPath := forkFlushDrainAckPath(pid, starttimeTicks)
	reqPath := forkFlushDrainRequestPath(pid, starttimeTicks)

	requestedTimeout := forkFinalizeDefaultTimeout
	req, err := readForkFinalizeRequest(reqPath)
	if err == nil {
		requestedTimeout, err = time.ParseDuration(req.FinalizeTimeout)
		if err != nil || requestedTimeout <= 0 {
			logger.Warnf("flush-drain: request has invalid timeout %q: %v", req.FinalizeTimeout, err)
			requestedTimeout = forkFinalizeDefaultTimeout
		}
		if err := os.Remove(reqPath); err != nil && !os.IsNotExist(err) {
			logger.Warnf("flush-drain: remove request: %s", err)
		}
	} else if !os.IsNotExist(err) {
		logger.Warnf("flush-drain: read request: %s", err)
	}

	flushTimeout := forkFinalizeDaemonTimeout(requestedTimeout)
	flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
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
			logger.Warnf("flush-drain: write pending ack phase=%s: %s", phase, err)
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
		logger.Errorf("flush-drain: %s: %s", phase, err)
	}

	defer func() {
		if r := recover(); r != nil {
			ack.Status = "panic"
			ack.Phase = ""
			ack.Error = fmt.Sprintf("panic: %v", r)
			resultErr = fmt.Errorf("flush-drain panic: %v", r)
		}

		if ack.Status != "panic" {
			if firstErr == nil {
				ack.Status = "ok"
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
			logger.Errorf("flush-drain: write ack: %s", err)
			if resultErr == nil {
				resultErr = fmt.Errorf("flush-drain: write ack: %w", err)
			}
		}
	}()

	writePendingAck("flush_all")
	flushErr := forkFlushDrainFlushAll(v)
	if flushCtx.Err() != nil {
		flushErr = forkFinalizePhaseTimeoutErr(flushTimeout, requestedTimeout, flushCtx.Err())
	}
	recordErr("flush_all", flushErr)
	if flushCtx.Err() != nil {
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
			err := drainer.WaitForUploadDrain(flushCtx)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				err = forkFinalizePhaseTimeoutErr(flushTimeout, requestedTimeout, err)
			}
			recordErr("upload_drain", err)
		}
	}

	return resultErr
}

func forkFlushDrainAckPath(pid int, starttimeTicks uint64) string {
	return filepath.Join(forkFlushDrainAckDir, fmt.Sprintf("%d-%d.json", pid, starttimeTicks))
}

func forkFlushDrainRequestPath(pid int, starttimeTicks uint64) string {
	return filepath.Join(forkFlushDrainAckDir, fmt.Sprintf("%d-%d.req.json", pid, starttimeTicks))
}
