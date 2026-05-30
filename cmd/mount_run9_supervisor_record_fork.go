//go:build !windows
// +build !windows

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type run9SupervisorRecord struct {
	Pid               int    `json:"pid"`
	PidStarttimeTicks uint64 `json:"pid_starttime_ticks"`
}

func writeRun9SupervisorRecordFromEnv() error {
	path := os.Getenv(run9SupervisorRecordEnv)
	if path == "" {
		return nil
	}
	return writeRun9SupervisorRecord(path)
}

func writeRun9SupervisorRecord(path string) error {
	record := run9SupervisorRecord{Pid: os.Getpid()}

	starttime, err := readProcStatStarttimeTicks(record.Pid)
	if err != nil {
		return fmt.Errorf("read /proc/%d/stat: %w", record.Pid, err)
	}
	record.PidStarttimeTicks = starttime

	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal run9 supervisor record: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp run9 supervisor record in %q: %w", dir, err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp run9 supervisor record %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp run9 supervisor record %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename run9 supervisor record %q -> %q: %w", tmpPath, path, err)
	}
	return nil
}
