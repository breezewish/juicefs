package cmd

import (
	"runtime"
	"testing"
)

func TestRun9BackgroundMountFastPathEnabled(t *testing.T) {
	t.Setenv(run9SupervisorRecordEnv, "")
	if run9BackgroundMountFastPathEnabled(true) {
		t.Fatal("expected fast path disabled without supervisor record env")
	}

	t.Setenv(run9SupervisorRecordEnv, "/tmp/run9-supervisor.json")
	if run9BackgroundMountFastPathEnabled(false) {
		t.Fatal("expected fast path disabled for foreground mount")
	}

	if runtime.GOOS == "windows" {
		if run9BackgroundMountFastPathEnabled(true) {
			t.Fatal("expected fast path disabled on windows")
		}
		return
	}
	if !run9BackgroundMountFastPathEnabled(true) {
		t.Fatal("expected fast path enabled for background run9 mount")
	}
}
