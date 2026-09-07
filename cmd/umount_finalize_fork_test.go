//go:build !windows
// +build !windows

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
)

func secureTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("Chmod temp dir: %v", err)
	}
	return dir
}

type stubFileInfo struct {
	mode os.FileMode
	sys  any
}

func (s stubFileInfo) Name() string       { return "" }
func (s stubFileInfo) Size() int64        { return 0 }
func (s stubFileInfo) Mode() os.FileMode  { return s.mode }
func (s stubFileInfo) ModTime() time.Time { return time.Time{} }
func (s stubFileInfo) IsDir() bool        { return s.mode.IsDir() }
func (s stubFileInfo) Sys() any           { return s.sys }

func TestParseProcStatusEUIDAndSignals_Ok(t *testing.T) {
	status := strings.Join([]string{
		"Name:\tjuicefs",
		"Umask:\t0002",
		"Uid:\t1000\t1001\t1002\t1003",
		"SigIgn:\t0000000000000000",
		"SigCgt:\t0000000000000800",
		"",
	}, "\n")
	info, err := parseProcStatusEUIDAndSignals(status)
	if err != nil {
		t.Fatalf("parseProcStatusEUIDAndSignals: %v", err)
	}
	if info.EUID != 1001 {
		t.Fatalf("unexpected euid: %d", info.EUID)
	}
	if info.SigIgn != 0 {
		t.Fatalf("unexpected SigIgn: 0x%x", info.SigIgn)
	}
	if info.SigCgt != 0x800 {
		t.Fatalf("unexpected SigCgt: 0x%x", info.SigCgt)
	}
}

func TestParseProcStatusEUIDAndSignals_MissingUid(t *testing.T) {
	status := "SigIgn:\t0000000000000000\nSigCgt:\t0000000000000800\n"
	_, err := parseProcStatusEUIDAndSignals(status)
	if err == nil {
		t.Fatalf("parseProcStatusEUIDAndSignals should fail")
	}
	if !strings.Contains(err.Error(), "Uid") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseProcStatusEUIDAndSignals_MissingSigCgt(t *testing.T) {
	status := "Uid:\t1000\t1001\t1002\t1003\nSigIgn:\t0000000000000000\n"
	_, err := parseProcStatusEUIDAndSignals(status)
	if err == nil {
		t.Fatalf("parseProcStatusEUIDAndSignals should fail")
	}
	if !strings.Contains(err.Error(), "SigCgt") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseProcStatusEUIDAndSignals_MissingSigIgn(t *testing.T) {
	status := "Uid:\t1000\t1001\t1002\t1003\nSigCgt:\t0000000000000800\n"
	_, err := parseProcStatusEUIDAndSignals(status)
	if err == nil {
		t.Fatalf("parseProcStatusEUIDAndSignals should fail")
	}
	if !strings.Contains(err.Error(), "SigIgn") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePositiveDuration_Seconds(t *testing.T) {
	d, err := parsePositiveDuration("2s")
	if err != nil {
		t.Fatalf("parsePositiveDuration: %v", err)
	}
	if d != 2*time.Second {
		t.Fatalf("unexpected duration: %s", d)
	}
}

func TestParsePositiveDuration_Minutes(t *testing.T) {
	d, err := parsePositiveDuration("10m")
	if err != nil {
		t.Fatalf("parsePositiveDuration: %v", err)
	}
	if d != 10*time.Minute {
		t.Fatalf("unexpected duration: %s", d)
	}
}

func TestParsePositiveDuration_RejectBareNumber(t *testing.T) {
	_, err := parsePositiveDuration("2")
	if err == nil {
		t.Fatalf("parsePositiveDuration should fail")
	}
}

func TestParsePositiveDuration_RejectZero(t *testing.T) {
	_, err := parsePositiveDuration("0")
	if err == nil {
		t.Fatalf("parsePositiveDuration should fail")
	}
	if !strings.Contains(err.Error(), "positive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParsePositiveDuration_Invalid(t *testing.T) {
	_, err := parsePositiveDuration("boom")
	if err == nil {
		t.Fatalf("parsePositiveDuration should fail")
	}
}

func TestForkFinalizeAckWaitFailure_ContextCanceled(t *testing.T) {
	reason, shouldKill := forkFinalizeAckWaitFailure(context.Canceled, 10*time.Second)
	if shouldKill {
		t.Fatalf("context.Canceled should not kill mount")
	}
	if !strings.Contains(reason, "canceled") {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestForkFinalizeAckWaitFailure_DeadlineExceeded(t *testing.T) {
	reason, shouldKill := forkFinalizeAckWaitFailure(context.DeadlineExceeded, 10*time.Second)
	if !shouldKill {
		t.Fatalf("context.DeadlineExceeded should kill mount")
	}
	if !strings.Contains(reason, "timeout waiting finalize ack") {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestForkFinalizeAckWaitFailure_OtherError(t *testing.T) {
	reason, shouldKill := forkFinalizeAckWaitFailure(errors.New("boom"), 10*time.Second)
	if !shouldKill {
		t.Fatalf("non-context errors should kill mount")
	}
	if reason != "boom" {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestFinalizeAckDirReady_Permissions(t *testing.T) {
	dir := t.TempDir()
	expectedUID := uint32(os.Geteuid())

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod insecure: %v", err)
	}
	ready, err := finalizeAckDirReady(dir, expectedUID)
	if err != nil {
		t.Fatalf("finalizeAckDirReady: %v", err)
	}
	if ready {
		t.Fatalf("dir should not be ready when permissions are insecure")
	}

	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("Chmod secure: %v", err)
	}
	ready, err = finalizeAckDirReady(dir, expectedUID)
	if err != nil {
		t.Fatalf("finalizeAckDirReady: %v", err)
	}
	if !ready {
		t.Fatalf("dir should be ready when permissions are 0700")
	}
}

func TestWaitFinalizeAck_Ok(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("waitFinalizeAck should succeed, got %v", err)
	}
	if got.Status != "ok" {
		t.Fatalf("unexpected status: %s", got.Status)
	}
}

func TestWaitFinalizeStart_PendingAck(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "pending",
		Phase:             "signal_received",
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitFinalizeStart(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if !errors.Is(err, errForkFinalizeAckPending) {
		t.Fatalf("waitFinalizeStart should report pending, got %v", err)
	}
	if got == nil || got.Status != "pending" {
		t.Fatalf("unexpected ack: %+v", got)
	}
}

func TestWaitFinalizeAck_PendingEventuallyOk(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live mount process identity requires Linux /proc")
	}
	starttime, err := readProcStatStarttimeTicks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	pending := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               os.Getpid(),
		PidStarttimeTicks: starttime,
		Status:            "pending",
		Phase:             "signal_received",
	}
	data, err := json.Marshal(pending)
	if err != nil {
		t.Fatalf("Marshal pending ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile pending ack: %v", err)
	}

	published := make(chan error, 1)
	t.Cleanup(func() {
		if err := <-published; err != nil {
			t.Errorf("publish finalize ack: %v", err)
		}
	})
	go func() {
		time.Sleep(100 * time.Millisecond)
		finalAck := &forkFinalizeAckV1{
			SchemaVersion:     1,
			Pid:               pending.Pid,
			PidStarttimeTicks: pending.PidStarttimeTicks,
			Status:            "ok",
			FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		}
		finalData, marshalErr := json.Marshal(finalAck)
		if marshalErr != nil {
			published <- marshalErr
			return
		}
		// Match the daemon's atomic publication; truncating the visible ack
		// would expose partial JSON that the reader correctly rejects.
		if err := os.WriteFile(ackPath+".next", finalData, 0o600); err != nil {
			published <- err
			return
		}
		published <- os.Rename(ackPath+".next", ackPath)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitFinalizeAck(ctx, ackPath, pending.Pid, pending.PidStarttimeTicks, uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("waitFinalizeAck should succeed, got %v", err)
	}
	if got.Status != "ok" {
		t.Fatalf("unexpected status: %s", got.Status)
	}
}

func TestWaitFinalizeAck_TimeoutReturnsPendingAck(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("live mount process identity requires Linux /proc")
	}
	starttime, err := readProcStatStarttimeTicks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	pending := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               os.Getpid(),
		PidStarttimeTicks: starttime,
		Status:            "pending",
		Phase:             "upload_drain",
	}
	data, err := json.Marshal(pending)
	if err != nil {
		t.Fatalf("Marshal pending ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile pending ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got, err := waitFinalizeAck(ctx, ackPath, pending.Pid, pending.PidStarttimeTicks, uint32(os.Geteuid()))
	if got == nil || got.Status != "pending" || got.Phase != "upload_drain" {
		t.Fatalf("unexpected ack: %+v", got)
	}
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitFinalizeAck should return context deadline, got %v", err)
	}
}

func TestWaitFinalizeAck_StatusError(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "error",
		Phase:             "flush_all",
		Error:             "boom",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "status=error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_SchemaMismatch(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     2,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "schema_version") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_PidMismatch(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, 124, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "pid mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_RejectSymlink(t *testing.T) {
	dir := secureTempDir(t)
	target := filepath.Join(dir, "target.json")
	ackPath := filepath.Join(dir, "ack.json")

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}
	if err := os.Symlink(target, ackPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_RejectDirectory(t *testing.T) {
	dir := secureTempDir(t)
	ackPath := filepath.Join(dir, "ack.json")
	if err := os.Mkdir(ackPath, 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := waitFinalizeAck(ctx, ackPath, 123, 456, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindInternalMountConfigPathWithLstat_FallbackToDotConfig(t *testing.T) {
	mp := "/mnt/jfs"
	jfsConfig := filepath.Join(mp, ".jfs.config")
	config := filepath.Join(mp, ".config")

	got, err := findInternalMountConfigPathWithLstat(mp, func(name string) (os.FileInfo, error) {
		switch name {
		case jfsConfig:
			return stubFileInfo{
				mode: os.ModeDir | 0o700,
				sys:  &syscall.Stat_t{Ino: 123},
			}, nil
		case config:
			return stubFileInfo{
				mode: 0o400,
				sys:  &syscall.Stat_t{Ino: uint64(vfs.ConfigInode)},
			}, nil
		default:
			return nil, os.ErrNotExist
		}
	})
	if err != nil {
		t.Fatalf("findInternalMountConfigPathWithLstat: %v", err)
	}
	if got != config {
		t.Fatalf("unexpected config path: %s", got)
	}
}

func TestFindInternalMountConfigPathWithLstat_PreferDotJfsConfig(t *testing.T) {
	mp := "/mnt/jfs"
	jfsConfig := filepath.Join(mp, ".jfs.config")
	config := filepath.Join(mp, ".config")

	got, err := findInternalMountConfigPathWithLstat(mp, func(name string) (os.FileInfo, error) {
		switch name {
		case jfsConfig:
			return stubFileInfo{
				mode: 0o400,
				sys:  &syscall.Stat_t{Ino: uint64(vfs.ConfigInode)},
			}, nil
		case config:
			return stubFileInfo{
				mode: 0o400,
				sys:  &syscall.Stat_t{Ino: 456},
			}, nil
		default:
			return nil, os.ErrNotExist
		}
	})
	if err != nil {
		t.Fatalf("findInternalMountConfigPathWithLstat: %v", err)
	}
	if got != jfsConfig {
		t.Fatalf("unexpected config path: %s", got)
	}
}

func TestWaitFinalizeAck_RejectFifoDoesNotBlock(t *testing.T) {
	dir := secureTempDir(t)
	ackPath := filepath.Join(dir, "ack.fifo")
	if err := syscall.Mkfifo(ackPath, 0o600); err != nil {
		t.Fatalf("Mkfifo: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := waitFinalizeAck(ctx, ackPath, 123, 456, uint32(os.Geteuid()))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("waitFinalizeAck should fail")
		}
		if !strings.Contains(err.Error(), "regular") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("waitFinalizeAck blocked on fifo")
	}
}

func TestWaitFinalizeAck_TooLarge(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	data := make([]byte, forkFinalizeAckMaxBytes+1)
	for i := range data {
		data[i] = 'a'
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := waitFinalizeAck(ctx, ackPath, 123, 456, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_RejectSymlinkDir(t *testing.T) {
	tmp := t.TempDir()
	realDir := filepath.Join(tmp, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatalf("Mkdir realDir: %v", err)
	}
	linkDir := filepath.Join(tmp, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatalf("Symlink dir: %v", err)
	}

	ackPath := filepath.Join(linkDir, "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(filepath.Join(realDir, "ack.json"), data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_DirOwnerMismatch(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               123,
		PidStarttimeTicks: 456,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Marshal ack: %v", err)
	}
	if err := os.WriteFile(ackPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile ack: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = waitFinalizeAck(ctx, ackPath, ack.Pid, ack.PidStarttimeTicks, uint32(os.Geteuid())+1)
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail")
	}
	if !strings.Contains(err.Error(), "owner uid mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_DaemonExitedWithoutAck(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := waitFinalizeAck(ctx, ackPath, 0, 1, uint32(os.Geteuid()))
	if err == nil {
		t.Fatalf("waitFinalizeAck should fail when daemon is gone and ack is missing")
	}
	if !strings.Contains(err.Error(), "exited without finalize ack") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitFinalizeAck_DaemonExitedWithPendingAck(t *testing.T) {
	ackPath := filepath.Join(secureTempDir(t), "ack.json")
	if err := os.WriteFile(ackPath, []byte(`{"schema_version":1,"pid":0,"pid_starttime_ticks":1,"status":"pending","phase":"signal_received"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := waitFinalizeAck(ctx, ackPath, 0, 1, uint32(os.Geteuid()))
	if err == nil || !strings.Contains(err.Error(), "exited without finalize ack") {
		t.Fatalf("a pending ack cannot keep a dead daemon alive: %v", err)
	}
}

func TestFindInternalMountConfigPath_NotExist(t *testing.T) {
	dir := secureTempDir(t)
	if _, err := findInternalMountConfigPath(dir); err == nil {
		t.Fatalf("findInternalMountConfigPath should fail")
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindInternalMountConfigPath_InodeMismatch(t *testing.T) {
	dir := secureTempDir(t)
	if err := os.WriteFile(filepath.Join(dir, ".config"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := findInternalMountConfigPath(dir); err == nil {
		t.Fatalf("findInternalMountConfigPath should fail")
	} else if !strings.Contains(err.Error(), "inode mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindInternalMountConfigPath_RejectSymlink(t *testing.T) {
	dir := secureTempDir(t)
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(dir, ".jfs.config")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	if _, err := findInternalMountConfigPath(dir); err == nil {
		t.Fatalf("findInternalMountConfigPath should fail")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindInternalMountConfigPath_RejectDirectory(t *testing.T) {
	dir := secureTempDir(t)
	if err := os.Mkdir(filepath.Join(dir, ".jfs.config"), 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if _, err := findInternalMountConfigPath(dir); err == nil {
		t.Fatalf("findInternalMountConfigPath should fail")
	} else if !strings.Contains(err.Error(), "directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}
