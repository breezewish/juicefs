//go:build linux
// +build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/vfs"
)

type fakeFinalizeChunkStore struct {
	waitErr   error
	waitCalls int
}

func (f *fakeFinalizeChunkStore) WaitForUploadDrain(context.Context) error {
	f.waitCalls++
	return f.waitErr
}

type blockingFinalizeChunkStore struct {
	waitCalls int
}

func (b *blockingFinalizeChunkStore) WaitForUploadDrain(ctx context.Context) error {
	b.waitCalls++
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingFinalizeChunkStore) NewReader(uint64, int) chunk.Reader { return fakeFinalizeReader{} }

func (b *blockingFinalizeChunkStore) NewWriter(uint64) chunk.Writer { return &fakeFinalizeWriter{} }

func (b *blockingFinalizeChunkStore) Remove(uint64, int) error { return nil }

func (b *blockingFinalizeChunkStore) FillCache(uint64, uint32) error { return nil }

func (b *blockingFinalizeChunkStore) EvictCache(uint64, uint32) error { return nil }

func (b *blockingFinalizeChunkStore) CheckCache(uint64, uint32, func(bool, string, int)) error {
	return nil
}

func (b *blockingFinalizeChunkStore) UsedMemory() int64 { return 0 }

func (b *blockingFinalizeChunkStore) UpdateLimit(int64, int64) {}

func (f *fakeFinalizeChunkStore) NewReader(uint64, int) chunk.Reader { return fakeFinalizeReader{} }

func (f *fakeFinalizeChunkStore) NewWriter(uint64) chunk.Writer { return &fakeFinalizeWriter{} }

func (f *fakeFinalizeChunkStore) Remove(uint64, int) error { return nil }

func (f *fakeFinalizeChunkStore) FillCache(uint64, uint32) error { return nil }

func (f *fakeFinalizeChunkStore) EvictCache(uint64, uint32) error { return nil }

func (f *fakeFinalizeChunkStore) CheckCache(uint64, uint32, func(bool, string, int)) error {
	return nil
}

func (f *fakeFinalizeChunkStore) UsedMemory() int64 { return 0 }

func (f *fakeFinalizeChunkStore) UpdateLimit(int64, int64) {}

type fakeFinalizeReader struct{}

func (fakeFinalizeReader) ReadAt(context.Context, *chunk.Page, int) (int, error) { return 0, nil }

type fakeFinalizeWriter struct {
	id uint64
}

func (w *fakeFinalizeWriter) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

func (w *fakeFinalizeWriter) ID() uint64 { return w.id }

func (w *fakeFinalizeWriter) SetID(id uint64) { w.id = id }

func (w *fakeFinalizeWriter) SetWriteback(bool) {}

func (w *fakeFinalizeWriter) FlushTo(int) error { return nil }

func (w *fakeFinalizeWriter) Finish(int) error { return nil }

func (w *fakeFinalizeWriter) Abort() {}

type fakeFinalizeSessionShutdowner struct {
	closeCalls    int
	shutdownCalls int
}

func (f *fakeFinalizeSessionShutdowner) CloseSession() error {
	f.closeCalls++
	return nil
}

func (f *fakeFinalizeSessionShutdowner) Shutdown() error {
	f.shutdownCalls++
	return nil
}

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

func TestRunForkFinalizeOnMain_WritesUploadDrainErrorAck(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackPath := forkFinalizeAckPath(pid, starttimeTicks)
	_ = os.Remove(ackPath)
	t.Cleanup(func() {
		_ = os.Remove(ackPath)
	})

	drainErr := errors.New("pending staging file missing: key k path /tmp/missing")
	metaCli := &fakeFinalizeSessionShutdowner{}
	store := &fakeFinalizeChunkStore{waitErr: drainErr}
	v := &vfs.VFS{
		Conf: &vfs.Config{
			Chunk: &chunk.Config{Writeback: true},
		},
		Store: store,
	}

	origFlushAll := forkFinalizeFlushAll
	forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
	defer func() { forkFinalizeFlushAll = origFlushAll }()

	err = runForkFinalizeOnMain(metaCli, v, nil)
	if !errors.Is(err, drainErr) {
		t.Fatalf("runForkFinalizeOnMain should return upload drain error, got %v", err)
	}
	if metaCli.closeCalls != 1 {
		t.Fatalf("CloseSession should be called once, got %d", metaCli.closeCalls)
	}
	if metaCli.shutdownCalls != 1 {
		t.Fatalf("Shutdown should be called once, got %d", metaCli.shutdownCalls)
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
	if ack.Status != "error" {
		t.Fatalf("unexpected ack status: %+v", ack)
	}
	if ack.Phase != "upload_drain" {
		t.Fatalf("unexpected ack phase: %+v", ack)
	}
	if ack.Error != drainErr.Error() {
		t.Fatalf("unexpected ack error: %+v", ack)
	}
	if ack.FinishedAt == "" {
		t.Fatalf("ack should include finished_at: %+v", ack)
	}
}

func TestRunForkFinalizeOnMain_TimesOutUploadDrainWritesAck(t *testing.T) {
	pid := os.Getpid()
	starttimeTicks, err := readProcStatStarttimeTicks(pid)
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}

	ackPath := forkFinalizeAckPath(pid, starttimeTicks)
	reqPath := forkFinalizeRequestPath(pid, starttimeTicks)
	_ = os.Remove(ackPath)
	_ = os.Remove(reqPath)
	t.Cleanup(func() {
		_ = os.Remove(ackPath)
		_ = os.Remove(reqPath)
	})
	if err := os.MkdirAll(filepath.Dir(ackPath), 0o700); err != nil {
		t.Fatalf("MkdirAll ack dir: %v", err)
	}
	if err := writeForkFinalizeRequest(reqPath, &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: "200ms",
	}); err != nil {
		t.Fatalf("writeForkFinalizeRequest: %v", err)
	}

	metaCli := &fakeFinalizeSessionShutdowner{}
	store := &blockingFinalizeChunkStore{}
	v := &vfs.VFS{
		Conf: &vfs.Config{
			Chunk: &chunk.Config{Writeback: true},
		},
		Store: store,
	}

	origFlushAll := forkFinalizeFlushAll
	forkFinalizeFlushAll = func(*vfs.VFS) error { return nil }
	defer func() { forkFinalizeFlushAll = origFlushAll }()

	done := make(chan error, 1)
	go func() { done <- runForkFinalizeOnMain(metaCli, v, nil) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("runForkFinalizeOnMain should fail on timeout")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("runForkFinalizeOnMain blocked")
	}

	if metaCli.closeCalls != 0 {
		t.Fatalf("CloseSession should not be called on timeout, got %d", metaCli.closeCalls)
	}
	if metaCli.shutdownCalls != 0 {
		t.Fatalf("Shutdown should not be called on timeout, got %d", metaCli.shutdownCalls)
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
	if ack.Status != "error" {
		t.Fatalf("unexpected ack status: %+v", ack)
	}
	if ack.Phase != "upload_drain" {
		t.Fatalf("unexpected ack phase: %+v", ack)
	}
	if !strings.Contains(ack.Error, "timeout after") {
		t.Fatalf("unexpected ack error: %+v", ack)
	}
	if ack.FinishedAt == "" {
		t.Fatalf("ack should include finished_at: %+v", ack)
	}
}
