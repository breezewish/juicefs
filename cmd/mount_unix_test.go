//go:build !windows
// +build !windows

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v2"
)

func TestNextMountpointCheckInterval(t *testing.T) {
	interval := time.Duration(0)
	got := make([]time.Duration, 0, 7)
	for i := 0; i < 7; i++ {
		interval = nextMountpointCheckInterval(interval)
		got = append(got, interval)
	}

	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		200 * time.Millisecond,
		200 * time.Millisecond,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("interval[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestGenFuseOptSkipsFusermountVersionForRun9(t *testing.T) {
	binDir := t.TempDir()
	probePath := filepath.Join(t.TempDir(), "fusermount-called")
	fusermountPath := filepath.Join(binDir, "fusermount")
	script := "#!/bin/sh\n: > \"$RUN9_FUSERMOUNT_PROBE\"\nprintf 'fusermount version: 2.9.9\\n'\n"
	if err := os.WriteFile(fusermountPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("RUN9_FUSERMOUNT_PROBE", probePath)
	t.Setenv(run9SupervisorRecordEnv, "/run/run9/snap-mount.json")

	options := genFuseOpt(cli.NewContext(cli.NewApp(), nil, nil), "test")

	if strings.Contains(options, "nonempty") {
		t.Fatalf("run9 fuse options unexpectedly contain nonempty: %q", options)
	}
	if _, err := os.Stat(probePath); !os.IsNotExist(err) {
		t.Fatalf("run9 mount unexpectedly probed fusermount version: %v", err)
	}

	t.Setenv(run9SupervisorRecordEnv, "")
	options = genFuseOpt(cli.NewContext(cli.NewApp(), nil, nil), "test")
	if !strings.Contains(options, "nonempty") {
		t.Fatalf("generic FUSE 2 options are missing nonempty: %q", options)
	}
	if _, err := os.Stat(probePath); err != nil {
		t.Fatalf("generic mount did not probe fusermount version: %v", err)
	}
}
