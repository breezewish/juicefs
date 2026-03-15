//go:build !windows
// +build !windows

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const forkFinalizeAckDir = "/tmp/juicefs-finalize-ack"

func forkFinalizeAckPath(pid int, starttimeTicks uint64) string {
	return filepath.Join(forkFinalizeAckDir, fmt.Sprintf("%d-%d.json", pid, starttimeTicks))
}

func readProcStatStarttimeTicks(pid int) (uint64, error) {
	starttime, _, err := readProcStatStarttimeTicksAndPPid(pid)
	return starttime, err
}

func readProcStatStarttimeTicksAndPPid(pid int) (uint64, int, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, 0, err
	}
	line := strings.TrimSpace(string(data))
	rparen := strings.LastIndexByte(line, ')')
	if rparen < 0 {
		return 0, 0, fmt.Errorf("unexpected /proc/%d/stat format", pid)
	}
	rest := strings.Fields(line[rparen+1:])
	// Field 22 is starttime; rest starts at field 3 (state), so index = 22-3 = 19.
	const starttimeIndex = 19
	const ppidIndex = 1
	if len(rest) <= starttimeIndex {
		return 0, 0, fmt.Errorf("unexpected /proc/%d/stat fields: %d", pid, len(rest))
	}
	ppid, err := strconv.Atoi(rest[ppidIndex])
	if err != nil {
		return 0, 0, fmt.Errorf("parse /proc/%d/stat ppid: %w", pid, err)
	}
	starttimeTicks, err := strconv.ParseUint(rest[starttimeIndex], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse /proc/%d/stat starttime: %w", pid, err)
	}
	return starttimeTicks, ppid, nil
}
