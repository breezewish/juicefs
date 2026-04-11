//go:build !windows
// +build !windows

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForkFinalizeRequestV1_ValidateRejectsInvalidDuration(t *testing.T) {
	req := &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: "boom",
	}
	if err := req.validate(); err == nil {
		t.Fatalf("validate should fail")
	}
}

func TestWriteAndReadForkFinalizeRequest_RoundTrip(t *testing.T) {
	dir := secureTempDir(t)
	path := filepath.Join(dir, "req.json")

	req := &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: "2s",
	}
	if err := writeForkFinalizeRequest(path, req); err != nil {
		t.Fatalf("writeForkFinalizeRequest: %v", err)
	}
	got, err := readForkFinalizeRequest(path)
	if err != nil {
		t.Fatalf("readForkFinalizeRequest: %v", err)
	}
	if got.SchemaVersion != 1 || got.FinalizeTimeout != "2s" {
		t.Fatalf("unexpected request: %+v", got)
	}
}

func TestWriteForkFinalizeRequest_RejectSymlink(t *testing.T) {
	dir := secureTempDir(t)
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}
	path := filepath.Join(dir, "req.json")
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	req := &forkFinalizeRequestV1{
		SchemaVersion:   1,
		FinalizeTimeout: "2s",
	}
	if err := writeForkFinalizeRequest(path, req); err == nil {
		t.Fatalf("writeForkFinalizeRequest should fail")
	} else if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("unexpected error: %v", err)
	}
}
