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

func TestRun9BackgroundMountDirectStage(t *testing.T) {
	t.Setenv(run9SupervisorRecordEnv, "/tmp/run9-supervisor.json")

	if run9BackgroundMountDirectStage(true, 1) {
		t.Fatal("expected direct stage disabled before daemon child is ready")
	}
	if runtime.GOOS == "windows" {
		if run9BackgroundMountDirectStage(true, 2) {
			t.Fatal("expected direct stage disabled on windows")
		}
		return
	}
	if !run9BackgroundMountDirectStage(true, 2) {
		t.Fatal("expected direct stage enabled for daemon child")
	}
	if run9BackgroundMountDirectStage(true, 3) {
		t.Fatal("expected direct stage disabled once already in stage 3")
	}
}
