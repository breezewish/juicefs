//go:build !windows
// +build !windows

package cmd

import "os"

func writeRun9MountRecordFromEnv() error {
	path := os.Getenv(run9MountRecordEnv)
	if path == "" {
		return nil
	}
	return writeRun9MountRecord(path)
}

func writeRun9MountRecord(path string) error {
	return writeRun9SupervisorRecord(path)
}
