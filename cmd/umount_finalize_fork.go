//go:build !windows
// +build !windows

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

const forkFinalizeAckMaxBytes = 64 * 1024

// Observe completed work promptly: a coarse poll here adds directly to Box
// Stop latency. This does not change upload drain or the finalize success fence.
const forkFinalizeObserveInterval = 20 * time.Millisecond

var errForkFinalizeAckPending = errors.New("finalize ack pending")

func cmdUmountFinalizeFork() *cli.Command {
	return &cli.Command{
		Name:      "umount-finalize",
		Action:    umountFinalize,
		Category:  "SERVICE",
		Usage:     "Unmount a volume and wait for badger terminal finalize (fork)",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This command is a fork-only variant to provide strict finalize semantics for badger (SkipWAL=true):
it returns success only after receiving a finalize ack written by the mount daemon.

Examples:
$ juicefs umount-finalize /mnt/jfs`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "finalize-timeout",
				Value: "10m",
				Usage: "timeout waiting for mount daemon to finish correctness-critical finalize and write ack (e.g. 10m, 30s)",
			},
			&cli.StringFlag{
				Name:  "umount-observe-timeout",
				Value: "3s",
				Usage: "timeout observing system-level force umount (best-effort; does not affect finalize correctness) (e.g. 3s)",
			},
			&cli.StringFlag{
				Name:  "exit-observe-timeout",
				Value: "3s",
				Usage: "timeout observing mount daemon exit after receiving success ack (best-effort) (e.g. 3s)",
			},
		},
	}
}

func umountFinalize(ctx *cli.Context) error {
	// Fork feature: always output structured JSON on failures.
	// Avoid calling setup(ctx, 1) which prints usage errors in plain text.
	setup0(ctx, 0, 0)

	result, exitCode := umountFinalizeRun(ctx)
	printJson(result)
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}

func umountFinalizeRun(ctx *cli.Context) (*forkUmountFinalizeResult, int) {
	if ctx.NArg() != 1 {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("expected exactly 1 argument MOUNTPOINT, got %d", ctx.NArg()),
		}, 1
	}

	mp := ctx.Args().Get(0)

	if runtime.GOOS != "linux" {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("umount-finalize is only supported on linux (current: %s)", runtime.GOOS),
		}, 1
	}

	finalizeTimeout, err := parsePositiveDuration(ctx.String("finalize-timeout"))
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid --finalize-timeout: %v", err),
		}, 1
	}
	umountObserveTimeout, err := parsePositiveDuration(ctx.String("umount-observe-timeout"))
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid --umount-observe-timeout: %v", err),
		}, 1
	}
	exitObserveTimeout, err := parsePositiveDuration(ctx.String("exit-observe-timeout"))
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid --exit-observe-timeout: %v", err),
		}, 1
	}

	conf, err := readVFSConfigWithTimeout(ctx.Context, mp, 3*time.Second)
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read mount config: %v", err),
		}, 1
	}
	if conf.Pid <= 0 {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("invalid mount pid %d in config", conf.Pid),
		}, 1
	}

	pid := conf.Pid
	expectedStarttime, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read /proc/%d/stat: %v", pid, err),
		}, 1
	}

	statusInfo, err := readProcStatusEUIDAndSignals(pid)
	if err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to read /proc/%d/status: %v", pid, err),
		}, 1
	}
	mountEUID := statusInfo.EUID
	sigusr2 := int(syscall.SIGUSR2)
	if sigusr2 <= 0 || sigusr2 > 64 {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("unexpected SIGUSR2 value %d", sigusr2),
		}, 1
	}
	sigusr2Mask := uint64(1) << (sigusr2 - 1)
	if statusInfo.SigIgn&sigusr2Mask != 0 {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("mount daemon ignores SIGUSR2 (SigIgn), refusing to send finalize signal to pid %d", pid),
		}, 1
	}
	if statusInfo.SigCgt&sigusr2Mask == 0 {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("mount daemon does not catch SIGUSR2 (SigCgt), refusing to send finalize signal to pid %d", pid),
		}, 1
	}

	ackPath := forkFinalizeAckPath(pid, expectedStarttime)

	reqPath := forkFinalizeRequestPath(pid, expectedStarttime)
	if err := ensureForkFinalizeAckDirForRequester(filepath.Dir(ackPath), mountEUID); err != nil {
		// Best-effort: the mount daemon can still finalize without the request file;
		// it will fall back to its default timeout.
		logger.Warnf("finalize: ensure ack dir for request: %s", err)
	} else {
		if err := writeForkFinalizeRequest(reqPath, &forkFinalizeRequestV1{
			SchemaVersion:   1,
			FinalizeTimeout: finalizeTimeout.String(),
		}); err != nil {
			// Best-effort: older mount daemons can still work without the request, and
			// we don't want timeout plumbing to become a new failure mode.
			logger.Warnf("finalize: write request: %s", err)
		} else {
			if os.Geteuid() == 0 && mountEUID != uint32(os.Geteuid()) {
				if err := os.Chown(reqPath, int(mountEUID), -1); err != nil {
					logger.Warnf("finalize: chown request to uid %d: %s", mountEUID, err)
				}
			}
			if err := os.Chmod(reqPath, 0o600); err != nil {
				logger.Warnf("finalize: chmod request: %s", err)
			}
		}
	}

	if _, err := finalizeAckDirReady(filepath.Dir(ackPath), mountEUID); err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("finalize ack dir is unsafe: %v", err),
		}, 1
	}

	// Re-check right before signalling to further reduce the chance of signalling a reused pid.
	if st3, err := readProcStatStarttimeTicks(pid); err != nil || st3 != expectedStarttime {
		reason := fmt.Sprintf("mount pid %d changed before signalling (starttime %d -> %d)", pid, expectedStarttime, st3)
		if err != nil {
			reason = fmt.Sprintf("mount pid %d changed before signalling: %v", pid, err)
		}
		return &forkUmountFinalizeResult{Ok: false, Reason: reason}, 1
	}

	if err := syscall.Kill(pid, syscall.SIGUSR2); err != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: fmt.Sprintf("failed to send finalize signal to pid %d: %v", pid, err),
		}, 1
	}

	startTimeout := 3 * time.Second
	if finalizeTimeout > 0 && finalizeTimeout < startTimeout {
		startTimeout = finalizeTimeout
	}
	startCtx, cancelStart := context.WithTimeout(ctx.Context, startTimeout)
	startAck, startErr := waitFinalizeStart(startCtx, ackPath, pid, expectedStarttime, mountEUID)
	cancelStart()
	if errors.Is(startErr, errForkFinalizeAckPending) {
		startErr = nil
	}
	if startAck != nil {
		result := &forkUmountFinalizeResult{
			Ok:                   false,
			Finalized:            startAck.Status == "ok",
			KernelUmountObserved: false,
			DaemonExitObserved:   false,
			Ack:                  startAck,
		}
		if startAck.Status == "ok" {
			result.Ok = true
			result.DaemonExitObserved = waitProcessExit(ctx.Context, pid, expectedStarttime, exitObserveTimeout)
			if !result.DaemonExitObserved {
				logger.Warnf("finalize ack is ok but mount daemon didn't exit within %s, killing it best-effort", exitObserveTimeout)
				killForkMountProcess(conf, pid, expectedStarttime)
			}
			return result, 0
		}
		if startErr != nil {
			result.Reason = startErr.Error()
			killForkMountProcess(conf, pid, expectedStarttime)
			return result, 1
		}
	}
	if startErr != nil {
		return &forkUmountFinalizeResult{
			Ok:     false,
			Reason: startErr.Error(),
		}, 1
	}

	kernelUmountObserved := observeForceUmount(ctx.Context, mp, umountObserveTimeout)

	result := &forkUmountFinalizeResult{
		Ok:                   false,
		Finalized:            false,
		KernelUmountObserved: kernelUmountObserved,
		DaemonExitObserved:   false,
	}

	finalizeCtx, cancel := context.WithTimeout(ctx.Context, finalizeTimeout)
	ack, ackErr := waitFinalizeAck(finalizeCtx, ackPath, pid, expectedStarttime, mountEUID)
	cancel()

	if ack != nil {
		result.Ack = ack
	}
	if ackErr != nil {
		reason, shouldKill := forkFinalizeAckWaitFailure(ackErr, finalizeTimeout)
		result.Reason = reason
		if shouldKill {
			killForkMountProcess(conf, pid, expectedStarttime)
		}
		return result, 1
	}

	result.Finalized = true
	result.Ok = true

	result.DaemonExitObserved = waitProcessExit(ctx.Context, pid, expectedStarttime, exitObserveTimeout)
	if !result.DaemonExitObserved {
		logger.Warnf("finalize ack is ok but mount daemon didn't exit within %s, killing it best-effort", exitObserveTimeout)
		killForkMountProcess(conf, pid, expectedStarttime)
	}

	return result, 0
}

func readVFSConfigWithTimeout(ctx context.Context, mp string, timeout time.Duration) (*vfs.Config, error) {
	var raw []byte
	err := withTimeout(ctx, timeout, func() error {
		configPath, err := findInternalMountConfigPath(mp)
		if err != nil {
			return err
		}
		raw, err = os.ReadFile(configPath)
		return err
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("timeout after %s", timeout)
		}
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("not a JuiceFS mount point")
		}
		return nil, err
	}
	var conf vfs.Config
	if err := json.Unmarshal(raw, &conf); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &conf, nil
}

// findInternalMountConfigPath finds the mount daemon's internal config file and ensures it's the real
// JuiceFS internal node (inode == vfs.ConfigInode).
//
// This avoids trusting a user-provided ".jfs.config"/".config" on an unmounted directory, which could
// otherwise lead to signalling or killing an unrelated process.
func findInternalMountConfigPath(mp string) (string, error) {
	return findInternalMountConfigPathWithLstat(mp, os.Lstat)
}

type forkFinalizeLstatFunc func(name string) (os.FileInfo, error)

func findInternalMountConfigPathWithLstat(mp string, lstat forkFinalizeLstatFunc) (string, error) {
	candidates := []string{
		filepath.Join(mp, ".jfs.config"),
		filepath.Join(mp, ".config"),
	}

	var anyExists bool
	var errs []error
	for _, candidate := range candidates {
		fi, err := lstat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			anyExists = true
			errs = append(errs, err)
			continue
		}
		anyExists = true
		if fi.Mode()&os.ModeSymlink != 0 {
			errs = append(errs, fmt.Errorf("mount config is a symlink: %s", candidate))
			continue
		}
		if fi.IsDir() {
			errs = append(errs, fmt.Errorf("mount config is a directory: %s", candidate))
			continue
		}
		if !fi.Mode().IsRegular() {
			errs = append(errs, fmt.Errorf("mount config is not a regular file: %s", candidate))
			continue
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			errs = append(errs, fmt.Errorf("unexpected stat type %T for %s", fi.Sys(), candidate))
			continue
		}
		if st.Ino != uint64(vfs.ConfigInode) {
			errs = append(errs, fmt.Errorf("mount config inode mismatch (got %d, want %d): %s", st.Ino, vfs.ConfigInode, candidate))
			continue
		}
		return candidate, nil
	}
	if !anyExists {
		return "", os.ErrNotExist
	}
	if len(errs) == 0 {
		return "", fmt.Errorf("no usable mount config found under %s", mp)
	}
	if len(errs) == 1 {
		return "", errs[0]
	}
	return "", errors.Join(errs...)
}

func readFinalizeAckFile(ackPath string) (*forkFinalizeAckV1, error) {
	fi, err := os.Lstat(ackPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read finalize ack: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("finalize ack path is a symlink: %s", ackPath)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("finalize ack path is a directory: %s", ackPath)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("finalize ack path is not a regular file: %s", ackPath)
	}
	if fi.Size() > int64(forkFinalizeAckMaxBytes) {
		return nil, fmt.Errorf("finalize ack too large: %d bytes", fi.Size())
	}

	f, err := os.Open(ackPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read finalize ack: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(f, forkFinalizeAckMaxBytes+1))
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("read finalize ack: %w", err)
	}
	if len(data) > forkFinalizeAckMaxBytes {
		return nil, fmt.Errorf("finalize ack too large: %d bytes", len(data))
	}
	var ack forkFinalizeAckV1
	if err := json.Unmarshal(data, &ack); err != nil {
		return nil, fmt.Errorf("parse finalize ack: %w", err)
	}
	return &ack, nil
}

func validateFinalizeAck(ack *forkFinalizeAckV1, expectedPid int, expectedStarttime uint64) error {
	if ack.SchemaVersion != 1 {
		return fmt.Errorf("unexpected ack schema_version %d", ack.SchemaVersion)
	}
	if ack.Pid != expectedPid || ack.PidStarttimeTicks != expectedStarttime {
		return fmt.Errorf("ack pid mismatch (got %d/%d, want %d/%d)", ack.Pid, ack.PidStarttimeTicks, expectedPid, expectedStarttime)
	}
	if ack.Status == "pending" {
		return errForkFinalizeAckPending
	}
	if ack.Status == "ok" {
		return nil
	}
	if ack.Error != "" {
		return fmt.Errorf("finalize ack status=%s phase=%s: %s", ack.Status, ack.Phase, ack.Error)
	}
	return fmt.Errorf("finalize ack status=%s phase=%s", ack.Status, ack.Phase)
}

func waitFinalizeStart(ctx context.Context, ackPath string, expectedPid int, expectedStarttime uint64, expectedUID uint32) (*forkFinalizeAckV1, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		dirReady, err := finalizeAckDirReady(filepath.Dir(ackPath), expectedUID)
		if err != nil {
			return nil, err
		}

		if dirReady {
			ack, err := readFinalizeAckFile(ackPath)
			if err == nil {
				validateErr := validateFinalizeAck(ack, expectedPid, expectedStarttime)
				if validateErr == nil || errors.Is(validateErr, errForkFinalizeAckPending) {
					return ack, validateErr
				}
				return ack, validateErr
			}
			if !os.IsNotExist(err) {
				return nil, err
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !processMatches(expectedPid, expectedStarttime) {
			return nil, fmt.Errorf("mount daemon exited before finalize start ack")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func waitFinalizeAck(ctx context.Context, ackPath string, expectedPid int, expectedStarttime uint64, expectedUID uint32) (*forkFinalizeAckV1, error) {
	ticker := time.NewTicker(forkFinalizeObserveInterval)
	defer ticker.Stop()
	daemonExited := false
	for {
		var ack *forkFinalizeAckV1
		dirReady, err := finalizeAckDirReady(filepath.Dir(ackPath), expectedUID)
		if err != nil {
			return nil, err
		}

		if dirReady {
			ack, err = readFinalizeAckFile(ackPath)
			if err == nil {
				validateErr := validateFinalizeAck(ack, expectedPid, expectedStarttime)
				if !errors.Is(validateErr, errForkFinalizeAckPending) {
					return ack, validateErr
				}
			}
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			}
		}

		if ctx.Err() != nil {
			// Preserve pending phase evidence, but never treat it as completion.
			return ack, ctx.Err()
		}

		// A pending ack is not proof of completion, even if the daemon is gone.
		if daemonExited {
			return ack, fmt.Errorf("mount daemon exited without finalize ack")
		}
		if !processMatches(expectedPid, expectedStarttime) {
			// Death may follow the terminal rename just after our read. Read
			// once more after observing death before declaring proof missing.
			daemonExited = true
			continue
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			// One final read through the same validation path before returning.
		}
	}
}

func finalizeAckDirReady(dir string, expectedUID uint32) (bool, error) {
	fi, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("finalize ack dir is a symlink: %s", dir)
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("finalize ack dir is not a directory: %s", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("unexpected stat type %T for %s", fi.Sys(), dir)
	}
	if st.Uid != expectedUID {
		return false, fmt.Errorf("finalize ack dir owner uid mismatch (got %d, want %d): %s", st.Uid, expectedUID, dir)
	}
	if fi.Mode().Perm() != 0o700 {
		// Directory permissions are not safe yet: do not read ack until the mount daemon chmods it to 0700.
		return false, nil
	}
	return true, nil
}

func observeForceUmount(ctx context.Context, mp string, timeout time.Duration) bool {
	observeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd, err := startForceUmountCommand(observeCtx, mp)
	if err != nil {
		logger.Warnf("start force umount: %s", err)
		return false
	}

	if err := cmd.Run(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false
		}
		logger.Warnf("force umount failed: %s", err)
		return false
	}
	return true
}

func startForceUmountCommand(ctx context.Context, mp string) (*exec.Cmd, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}
	for _, bin := range []string{"fusermount3", "fusermount"} {
		if _, err := exec.LookPath(bin); err == nil {
			cmd := exec.CommandContext(ctx, bin, "-uz", mp)
			cmd.Stdout = nil
			cmd.Stderr = nil
			return cmd, nil
		}
	}
	cmd := exec.CommandContext(ctx, "umount", "-l", mp)
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd, nil
}

func waitProcessExit(ctx context.Context, pid int, expectedStarttime uint64, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(forkFinalizeObserveInterval)
	defer ticker.Stop()
	for {
		if !processMatches(pid, expectedStarttime) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-ticker.C:
		}
	}
}

func killForkMountProcess(conf *vfs.Config, pid int, expectedStarttime uint64) {
	starttime, ppid, err := readProcStatStarttimeTicksAndPPid(pid)
	if err != nil {
		return
	}
	if starttime != expectedStarttime {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	if conf != nil && conf.CommPath != "" && conf.PPid > 1 && ppid == conf.PPid {
		_ = syscall.Kill(conf.PPid, syscall.SIGKILL)
	}
}

func processMatches(pid int, expectedStarttime uint64) bool {
	st, err := readProcStatStarttimeTicks(pid)
	return err == nil && st == expectedStarttime
}

func readProcStatusEUIDAndSignals(pid int) (*procStatusEUIDAndSignals, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
	if err != nil {
		return nil, err
	}
	return parseProcStatusEUIDAndSignals(string(data))
}

type procStatusEUIDAndSignals struct {
	EUID   uint32
	SigCgt uint64
	SigIgn uint64
}

func parseProcStatusEUIDAndSignals(status string) (*procStatusEUIDAndSignals, error) {
	var (
		euid      uint32
		sigCgt    uint64
		sigIgn    uint64
		foundEUID bool
		foundCgt  bool
		foundIgn  bool
	)
	for _, line := range strings.Split(status, "\n") {
		switch {
		case strings.HasPrefix(line, "Uid:"):
			fields := strings.Fields(line)
			// Uid:    real    effective    saved set    filesystem
			// Example: Uid:    1000    1000    1000    1000
			if len(fields) < 3 {
				return nil, fmt.Errorf("unexpected Uid line: %q", line)
			}
			parsed, err := strconv.ParseUint(fields[2], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("parse Uid effective: %w", err)
			}
			euid = uint32(parsed)
			foundEUID = true
		case strings.HasPrefix(line, "SigCgt:"):
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return nil, fmt.Errorf("unexpected SigCgt line: %q", line)
			}
			parsed, err := strconv.ParseUint(fields[1], 16, 64)
			if err != nil {
				return nil, fmt.Errorf("parse SigCgt: %w", err)
			}
			sigCgt = parsed
			foundCgt = true
		case strings.HasPrefix(line, "SigIgn:"):
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return nil, fmt.Errorf("unexpected SigIgn line: %q", line)
			}
			parsed, err := strconv.ParseUint(fields[1], 16, 64)
			if err != nil {
				return nil, fmt.Errorf("parse SigIgn: %w", err)
			}
			sigIgn = parsed
			foundIgn = true
		}
	}
	if !foundEUID {
		return nil, errors.New("Uid not found")
	}
	if !foundCgt {
		return nil, errors.New("SigCgt not found")
	}
	if !foundIgn {
		return nil, errors.New("SigIgn not found")
	}
	return &procStatusEUIDAndSignals{EUID: euid, SigCgt: sigCgt, SigIgn: sigIgn}, nil
}

func parsePositiveDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, errors.New("empty duration")
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive (got %s)", value)
	}
	return d, nil
}

func forkFinalizeAckWaitFailure(ackErr error, finalizeTimeout time.Duration) (reason string, shouldKill bool) {
	if errors.Is(ackErr, context.DeadlineExceeded) {
		return fmt.Sprintf("timeout waiting finalize ack: %s", finalizeTimeout), true
	}
	if errors.Is(ackErr, context.Canceled) {
		// Do not SIGKILL the mount daemon on cancellation (e.g. user interrupted `umount-finalize`):
		// it may be finalizing correctly after SIGUSR2, and killing it could violate the strict finalize
		// semantics that this fork-only command is designed to provide.
		return "canceled waiting finalize ack", false
	}
	return ackErr.Error(), true
}

func withTimeout(ctx context.Context, timeout time.Duration, fn func() error) error {
	if timeout <= 0 {
		return fn()
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		return timeoutCtx.Err()
	}
}
