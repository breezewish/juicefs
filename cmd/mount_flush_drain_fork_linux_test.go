//go:build linux
// +build linux

package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func TestRunForkFlushDrain_FlushesAndDrainsWithoutClosingSession(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackPath := forkFlushDrainAckPath(pid, starttimeTicks)
	_ = os.Remove(ackPath)
	t.Cleanup(func() {
		_ = os.Remove(ackPath)
	})

	store := &fakeFinalizeChunkStore{}
	v := &vfs.VFS{
		Conf: &vfs.Config{
			Chunk: &chunk.Config{Writeback: true},
		},
		Store: store,
	}

	flushCalls := 0
	origFlushAll := forkFlushDrainFlushAll
	forkFlushDrainFlushAll = func(*vfs.VFS) error {
		flushCalls++
		return nil
	}
	defer func() { forkFlushDrainFlushAll = origFlushAll }()

	if err := runForkFlushDrain(v); err != nil {
		t.Fatalf("runForkFlushDrain should succeed, got %v", err)
	}
	if flushCalls != 1 {
		t.Fatalf("FlushAll should be called once, got %d", flushCalls)
	}
	if store.waitCalls != 1 {
		t.Fatalf("WaitForUploadDrain should be called once, got %d", store.waitCalls)
	}

	data, err := os.ReadFile(ackPath)
	if err != nil {
		t.Fatalf("ReadFile ack: %v", err)
	}
	var ack forkFinalizeAckV1
	if err := json.Unmarshal(data, &ack); err != nil {
		t.Fatalf("Unmarshal ack: %v", err)
	}
	if ack.Status != "ok" {
		t.Fatalf("unexpected ack status: %+v", ack)
	}
	if ack.FinishedAt == "" {
		t.Fatalf("ack should include finished_at: %+v", ack)
	}
}

func TestRemoveRegularFlushDrainAckRejectsUnsafePath(t *testing.T) {
	ackPath := filepath.Join(t.TempDir(), "ack.json")
	if err := os.Symlink("target", ackPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	if err := removeRegularFlushDrainAck(ackPath); err == nil {
		t.Fatalf("removeRegularFlushDrainAck should reject symlink")
	}
}
