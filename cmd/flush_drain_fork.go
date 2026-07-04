//go:build linux
// +build linux

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
)

const forkFlushDrainAckDir = "/tmp/juicefs-flush-drain-ack"

type forkFlushDrainResult struct {
	Ok      bool               `json:"ok"`
	Drained bool               `json:"drained"`
	Reason  string             `json:"reason,omitempty"`
	Ack     *forkFinalizeAckV1 `json:"ack,omitempty"`
}

func cmdFlushDrainFork() *cli.Command {
	return &cli.Command{
		Name:      "flush-drain",
		Action:    flushDrain,
		Category:  "SERVICE",
		Usage:     "Flush a mounted volume and wait for writeback uploads without unmounting (fork)",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This command is a fork-only publish fence for deferred fsync mounts:
it flushes delayed VFS data and waits for writeback uploads while keeping the mount daemon alive.

Examples:
$ juicefs flush-drain /mnt/jfs`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "timeout",
				Value: "10m",
				Usage: "timeout waiting for mount daemon to flush and drain uploads (e.g. 10m, 30s)",
			},
		},
	}
}

func flushDrain(ctx *cli.Context) error {
	setup0(ctx, 0, 0)

	result, exitCode := flushDrainRun(ctx)
	printJson(result)
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}

func flushDrainRun(ctx *cli.Context) (*forkFlushDrainResult, int) {
	if ctx.NArg() != 1 {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("expected exactly 1 argument MOUNTPOINT, got %d", ctx.NArg()),
		}, 1
	}

	mp := ctx.Args().Get(0)

	if runtime.GOOS != "linux" {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("flush-drain is only supported on linux (current: %s)", runtime.GOOS),
		}, 1
	}

	flushTimeout, err := parsePositiveDuration(ctx.String("timeout"))
	if err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid --timeout: %v", err),
		}, 1
	}

	conf, err := readVFSConfigWithTimeout(ctx.Context, mp, 3*time.Second)
	if err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read mount config: %v", err),
		}, 1
	}
	if conf.Pid <= 0 {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid mount pid %d in config", conf.Pid),
		}, 1
	}

	pid := conf.Pid
	expectedStarttime, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read /proc/%d/stat: %v", pid, err),
		}, 1
	}

	statusInfo, err := readProcStatusEUIDAndSignals(pid)
	if err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read /proc/%d/status: %v", pid, err),
		}, 1
	}
	mountEUID := statusInfo.EUID
	sigusr1 := int(syscall.SIGUSR1)
	if sigusr1 <= 0 || sigusr1 > 64 {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("unexpected SIGUSR1 value %d", sigusr1),
		}, 1
	}
	sigusr1Mask := uint64(1) << (sigusr1 - 1)
	if statusInfo.SigIgn&sigusr1Mask != 0 {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("mount daemon ignores SIGUSR1 (SigIgn), refusing to send flush-drain signal to pid %d", pid),
		}, 1
	}
	if statusInfo.SigCgt&sigusr1Mask == 0 {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("mount daemon does not catch SIGUSR1 (SigCgt), refusing to send flush-drain signal to pid %d", pid),
		}, 1
	}

	ackPath := forkFlushDrainAckPath(pid, expectedStarttime)
	reqPath := forkFlushDrainRequestPath(pid, expectedStarttime)
	if err := ensureForkFinalizeAckDirForRequester(filepath.Dir(ackPath), mountEUID); err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("flush-drain ack dir is unsafe: %v", err),
		}, 1
	}
	if _, err := finalizeAckDirReady(filepath.Dir(ackPath), mountEUID); err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("flush-drain ack dir is unsafe: %v", err),
		}, 1
	}
	if err := removeRegularFlushDrainAck(ackPath); err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: err.Error(),
		}, 1
	}

	if err := writeForkFinalizeRequest(reqPath, &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: flushTimeout.String(),
	}); err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("write flush-drain request: %v", err),
		}, 1
	}
	if os.Geteuid() == 0 && mountEUID != uint32(os.Geteuid()) {
		if err := os.Chown(reqPath, int(mountEUID), -1); err != nil {
			logger.Warnf("flush-drain: chown request to uid %d: %s", mountEUID, err)
		}
	}
	if err := os.Chmod(reqPath, 0o600); err != nil {
		logger.Warnf("flush-drain: chmod request: %s", err)
	}

	if st3, err := readProcStatStarttimeTicks(pid); err != nil || st3 != expectedStarttime {
		reason := fmt.Sprintf("mount pid %d changed before signalling (starttime %d -> %d)", pid, expectedStarttime, st3)
		if err != nil {
			reason = fmt.Sprintf("mount pid %d changed before signalling: %v", pid, err)
		}
		return &forkFlushDrainResult{Ok: false, Reason: reason}, 1
	}

	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		return &forkFlushDrainResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to send flush-drain signal to pid %d: %v", pid, err),
		}, 1
	}

	startTimeout := 3 * time.Second
	if flushTimeout > 0 && flushTimeout < startTimeout {
		startTimeout = flushTimeout
	}
	startCtx, cancelStart := context.WithTimeout(ctx.Context, startTimeout)
	startAck, startErr := waitFinalizeStart(startCtx, ackPath, pid, expectedStarttime, mountEUID)
	cancelStart()
	if errors.Is(startErr, errForkFinalizeAckPending) {
		startErr = nil
	}
	if startAck != nil && startAck.Status == "ok" {
		return &forkFlushDrainResult{Ok: true, Drained: true, Ack: startAck}, 0
	}
	if startErr != nil {
		result := &forkFlushDrainResult{
			Ok:      false,
			Drained: false,
			Reason:  startErr.Error(),
			Ack:     startAck,
		}
		return result, 1
	}

	flushCtx, cancel := context.WithTimeout(ctx.Context, flushTimeout)
	ack, ackErr := waitFinalizeAck(flushCtx, ackPath, pid, expectedStarttime, mountEUID)
	cancel()
	if ackErr != nil {
		result := &forkFlushDrainResult{
			Ok:      false,
			Drained: false,
			Ack:     ack,
		}
		if errors.Is(ackErr, context.DeadlineExceeded) {
			result.Reason = fmt.Sprintf("timeout waiting flush-drain ack: %s", flushTimeout)
		} else if errors.Is(ackErr, context.Canceled) {
			result.Reason = "canceled waiting flush-drain ack"
		} else {
			result.Reason = strings.ReplaceAll(ackErr.Error(), "finalize ack", "flush-drain ack")
		}
		return result, 1
	}

	return &forkFlushDrainResult{
		Ok:      true,
		Drained: true,
		Ack:     ack,
	}, 0
}

func removeRegularFlushDrainAck(ackPath string) error {
	fi, err := os.Lstat(ackPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat flush-drain ack: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("flush-drain ack path is a symlink: %s", ackPath)
	}
	if fi.IsDir() {
		return fmt.Errorf("flush-drain ack path is a directory: %s", ackPath)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("flush-drain ack path is not a regular file: %s", ackPath)
	}
	return os.Remove(ackPath)
}
