package utils

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

const run9PerfTracePathEnvVar = "RUN9_PERF_TRACE_PATH"

type run9PerfTraceEvent struct {
	TSUnixNs int64 `json:"ts_unix_ns"`

	Service string `json:"service"`
	Event   string `json:"event"`

	RequestID string         `json:"request_id,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

type run9PerfTraceWriter struct {
	mu sync.Mutex
	f  *os.File
}

var (
	run9PerfTraceOnce  sync.Once
	run9PerfTraceValue *run9PerfTraceWriter
)

func run9PerfTrace() *run9PerfTraceWriter {
	run9PerfTraceOnce.Do(func() {
		path := strings.TrimSpace(os.Getenv(run9PerfTracePathEnvVar))
		if path == "" {
			return
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		run9PerfTraceValue = &run9PerfTraceWriter{f: f}
	})
	return run9PerfTraceValue
}

func Run9PerfTraceEnabled() bool {
	return run9PerfTrace() != nil
}

func EmitRun9PerfTraceEvent(service string, event string, requestID string, fields map[string]any) {
	writer := run9PerfTrace()
	if writer == nil {
		return
	}
	raw, err := json.Marshal(run9PerfTraceEvent{
		TSUnixNs:  time.Now().UnixNano(),
		Service:   service,
		Event:     event,
		RequestID: requestID,
		Fields:    fields,
	})
	if err != nil {
		return
	}
	raw = append(raw, '\n')

	writer.mu.Lock()
	_, _ = writer.f.Write(raw)
	writer.mu.Unlock()
}
