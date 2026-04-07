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
			if err := writeForkFinalizePendingAckForCurrentProcess(); err != nil {
				logger.Warnf("finalize: write pending ack: %s", err)
			}
			logger.Infof("Received SIGUSR2, requesting finalize via force umount")
			if err := doUmount(v.Conf.Meta.MountPoint, true); err != nil {
				logger.Warnf("finalize: force umount: %s", err)
			}
		}
	}()
}

func runForkFinalizeOnMain(metaCli meta.Meta, v *vfs.VFS, blob object.ObjectStorage) (resultErr error) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return fmt.Errorf("finalize: read /proc/self/stat: %w", err)
	}
	ackPath := forkFinalizeAckPath(pid, starttimeTicks)

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
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

		if ack.Status == "" {
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
			logger.Errorf("finalize: write ack: %s", err)
			if resultErr == nil {
				resultErr = fmt.Errorf("finalize: write ack: %w", err)
			}
		}
	}()

	recordErr("flush_all", v.FlushAll(""))

	if v.Conf != nil && v.Conf.Chunk != nil && v.Conf.Chunk.Writeback {
		drainer, ok := v.Store.(interface {
			WaitForUploadDrain(context.Context) error
		})
		if !ok {
			recordErr("upload_drain", errors.New("chunk store does not support WaitForUploadDrain"))
		} else {
			// Intentionally use Background: the mount daemon doesn't know the caller's --finalize-timeout.
			// Bounded waiting is enforced by the `umount-finalize` caller; the daemon writes a terminal
			// ack when finalize completes.
			recordErr("upload_drain", drainer.WaitForUploadDrain(context.Background()))
		}
	}

	recordErr("close_session", metaCli.CloseSession())

	recordErr("shutdown", metaCli.Shutdown())
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
