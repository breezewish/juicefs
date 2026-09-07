//go:build linux
// +build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
