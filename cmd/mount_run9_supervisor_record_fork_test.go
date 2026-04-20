//go:build !windows
// +build !windows

package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteRun9SupervisorRecordWritesCurrentProcessIdentity(t *testing.T) {
	recordPath := filepath.Join(t.TempDir(), "supervisor.json")

	if err := writeRun9SupervisorRecord(recordPath); err != nil {
		t.Fatalf("writeRun9SupervisorRecord: %v", err)
	}

	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("ReadFile record: %v", err)
	}

	var record run9SupervisorRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("Unmarshal record: %v", err)
	}

	if record.Pid != os.Getpid() {
		t.Fatalf("unexpected pid: %+v", record)
	}

	starttime, err := readProcStatStarttimeTicks(os.Getpid())
	if err != nil {
		t.Fatalf("readProcStatStarttimeTicks: %v", err)
	}
	if record.PidStarttimeTicks != starttime {
		t.Fatalf("unexpected pid_starttime_ticks: %+v", record)
	}
}
