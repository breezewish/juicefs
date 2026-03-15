//go:build linux
// +build linux

package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWriteForkFinalizeAck_WritesFile(t *testing.T) {
	pid := 99991
	starttimeTicks := uint64(time.Now().UnixNano())
	ackPath := filepath.Join(t.TempDir(), "ack.json")

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeForkFinalizeAck(ackPath, ack); err != nil {
		t.Fatalf("writeForkFinalizeAck: %v", err)
	}

	data, err := os.ReadFile(ackPath)
	if err != nil {
		t.Fatalf("ReadFile ack: %v", err)
	}
	var got forkFinalizeAckV1
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal ack: %v", err)
	}
	if got.SchemaVersion != 1 || got.Pid != pid || got.PidStarttimeTicks != starttimeTicks || got.Status != "ok" {
		t.Fatalf("unexpected ack: %+v", got)
	}
}

func TestWriteForkFinalizeAck_RejectSymlinkFile(t *testing.T) {
	pid := 99992
	starttimeTicks := uint64(time.Now().UnixNano())
	ackPath := filepath.Join(t.TempDir(), "ack.json")

	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}
	if err := os.Symlink(target, ackPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeForkFinalizeAck(ackPath, ack); err == nil {
		t.Fatalf("writeForkFinalizeAck should fail")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteForkFinalizeAck_RejectNonRegularFile(t *testing.T) {
	pid := 99993
	starttimeTicks := uint64(time.Now().UnixNano())
	ackPath := filepath.Join(t.TempDir(), "ack.json")

	if err := syscall.Mkfifo(ackPath, 0o600); err != nil {
		t.Fatalf("Mkfifo: %v", err)
	}

	ack := &forkFinalizeAckV1{
		SchemaVersion:     1,
		Pid:               pid,
		PidStarttimeTicks: starttimeTicks,
		Status:            "ok",
		FinishedAt:        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeForkFinalizeAck(ackPath, ack); err == nil {
		t.Fatalf("writeForkFinalizeAck should fail")
	} else if !strings.Contains(err.Error(), "regular") {
		t.Fatalf("unexpected error: %v", err)
	}
}
