package utils

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestEmitRun9PerfTraceEventWritesJSONLine(t *testing.T) {
	resetRun9PerfTraceForTest()
	t.Cleanup(resetRun9PerfTraceForTest)

	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv(run9PerfTracePathEnvVar, tracePath)

	EmitRun9PerfTraceEvent("juicefs", "mount_stage3_new_session_end", "snap-123", map[string]any{
		"dur_ms":      int64(37),
		"mount_point": "/mnt/snap-123",
	})
	resetRun9PerfTraceForTest()

	raw, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}

	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	if event["service"] != "juicefs" {
		t.Fatalf("service = %v", event["service"])
	}
	if event["event"] != "mount_stage3_new_session_end" {
		t.Fatalf("event = %v", event["event"])
	}
	if event["request_id"] != "snap-123" {
		t.Fatalf("request_id = %v", event["request_id"])
	}
	fields, ok := event["fields"].(map[string]any)
	if !ok {
		t.Fatalf("fields = %#v", event["fields"])
	}
	if got := fields["mount_point"]; got != "/mnt/snap-123" {
		t.Fatalf("mount_point = %v", got)
	}
	if got := fields["dur_ms"]; got != float64(37) {
		t.Fatalf("dur_ms = %v", got)
	}
	if _, ok := event["ts_unix_ns"].(float64); !ok {
		t.Fatalf("ts_unix_ns = %#v", event["ts_unix_ns"])
	}
}

func resetRun9PerfTraceForTest() {
	if run9PerfTraceValue != nil && run9PerfTraceValue.f != nil {
		_ = run9PerfTraceValue.f.Close()
	}
	run9PerfTraceValue = nil
	run9PerfTraceOnce = sync.Once{}
}
