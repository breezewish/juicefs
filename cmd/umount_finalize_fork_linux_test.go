//go:build linux
// +build linux

package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func TestUmountFinalizeRejectsInvalidSuccessAck(t *testing.T) {
	if os.Getenv("JFS_TEST_INVALID_SUCCESS_ACK") == "1" {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGUSR2)
		defer signal.Stop(signals)
		fmt.Println("ready")
		<-signals
		pid := os.Getpid()
		start, err := readProcStatStarttimeTicks(pid)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeForkFinalizeAck(forkFinalizeAckPath(pid, start), &forkFinalizeAckV1{
			SchemaVersion: 2, Pid: pid, PidStarttimeTicks: start, Status: "ok",
		}); err != nil {
			t.Fatal(err)
		}
		<-signals // The requester must reject the proof and kill this child.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUmountFinalizeRejectsInvalidSuccessAck$")
	child.Env = append(os.Environ(), "JFS_TEST_INVALID_SUCCESS_ACK=1")
	child.Stderr = os.Stderr
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	var ackPath, requestPath string
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
		if ackPath != "" {
			_ = os.Remove(ackPath)
			_ = os.Remove(requestPath)
		}
	})
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("signal handler readiness: %q, %v", line, err)
	}
	pid := child.Process.Pid
	start, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatal(err)
	}
	ackPath, requestPath = forkFinalizeAckPath(pid, start), forkFinalizeRequestPath(pid, start)
	// Only replace the FUSE config read. The actual process identity, signal,
	// ACK validation and command result handling remain under test.
	original := readVFSConfigWithTimeout
	readVFSConfigWithTimeout = func(context.Context, string, time.Duration) (*vfs.Config, error) {
		return &vfs.Config{Pid: pid}, nil
	}
	t.Cleanup(func() { readVFSConfigWithTimeout = original })
	flags := flag.NewFlagSet("umount-finalize", flag.ContinueOnError)
	flags.String("finalize-timeout", "1s", "")
	flags.String("umount-observe-timeout", "50ms", "")
	flags.String("exit-observe-timeout", "50ms", "")
	if err := flags.Parse([]string{"/unused-test-mount"}); err != nil {
		t.Fatal(err)
	}
	cliCtx := cli.NewContext(cli.NewApp(), flags, nil)
	cliCtx.Context = ctx
	result, exitCode := umountFinalizeRun(cliCtx)
	if exitCode == 0 || result.Ok || result.Finalized || !strings.Contains(result.Reason, "unexpected ack schema_version 2") {
		t.Fatalf("invalid success ACK must fail: exit=%d result=%+v", exitCode, result)
	}
}

func TestWaitFinalizeAck_WaitsForFile(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackPath := filepath.Join(t.TempDir(), "ack.json")
	if err := os.Chmod(filepath.Dir(ackPath), 0o700); err != nil {
		t.Fatalf("Chmod ack dir: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	published := make(chan error, 1)
	t.Cleanup(func() {
		if err := <-published; err != nil {
			t.Errorf("publish finalize ack: %v", err)
		}
	})
	go func() {
		time.Sleep(300 * time.Millisecond)
		ack := &forkFinalizeAckV1{
			SchemaVersion:     1,
			Pid:               pid,
			PidStarttimeTicks: starttimeTicks,
			Status:            "ok",
			FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		}
		published <- writeForkFinalizeAck(ackPath, ack)
	}()

	_, err = waitFinalizeAck(ctx, ackPath, pid, starttimeTicks, uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("waitFinalizeAck should succeed, got %v", err)
	}
}

func TestWaitFinalizeAck_WaitsForSafeAckDir(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackDir := t.TempDir()
	if err := os.Chmod(ackDir, 0o755); err != nil {
		t.Fatalf("Chmod ackDir: %v", err)
	}
	ackPath := filepath.Join(ackDir, "ack.json")

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := waitFinalizeAck(ctx, ackPath, pid, starttimeTicks, uint32(os.Geteuid()))
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("waitFinalizeAck should not finish before dir perms fixed, got %v", err)
	default:
	}

	if err := os.Chmod(ackDir, 0o700); err != nil {
		t.Fatalf("Chmod ackDir safe: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("waitFinalizeAck should succeed after dir perms fixed, got %v", err)
	}
}

func TestWaitFinalizeAck_Timeout(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackPath := filepath.Join(secureTempDir(t), "ack.json")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got, err := waitFinalizeAck(ctx, ackPath, pid, starttimeTicks, uint32(os.Geteuid()))
	if got == nil {
		// ok
	} else {
		t.Fatalf("waitFinalizeAck should not return ack on timeout, got %+v", got)
	}
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitFinalizeAck should return context deadline, got %v", err)
	}
}
