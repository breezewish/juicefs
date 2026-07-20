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

func TestRunForkFlushDrainDoesNotAbandonTimedOutFlushAll(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}
	ackPath := forkFlushDrainAckPath(pid, starttimeTicks)
	reqPath := forkFlushDrainRequestPath(pid, starttimeTicks)
	_ = os.Remove(ackPath)
	t.Cleanup(func() {
		_ = os.Remove(ackPath)
		_ = os.Remove(reqPath)
	})
	if err := writeForkFinalizeRequest(reqPath, &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: "50ms",
	}); err != nil {
		t.Fatalf("writeForkFinalizeRequest: %v", err)
	}

	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	origFlushAll := forkFlushDrainFlushAll
	forkFlushDrainFlushAll = func(*vfs.VFS) error {
		close(flushStarted)
		<-releaseFlush
		return nil
	}
	defer func() { forkFlushDrainFlushAll = origFlushAll }()

	done := make(chan error, 1)
	go func() { done <- runForkFlushDrain(&vfs.VFS{Conf: &vfs.Config{}}) }()
	<-flushStarted
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("flush-drain abandoned FlushAll after its timeout: %v", err)
	default:
	}

	close(releaseFlush)
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected deadline error after FlushAll returns, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("flush-drain did not finish after FlushAll returned")
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
