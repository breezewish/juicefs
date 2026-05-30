//go:build !windows
// +build !windows

package cmd

import (
	"testing"
	"time"
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
