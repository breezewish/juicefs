//go:build !nobadger
// +build !nobadger

package meta

import (
	"fmt"
	"testing"
)

func TestBadgerNextChunkOverride(t *testing.T) {
	dir := t.TempDir()

	m1, err := newKVMeta("badger", dir, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := m1.Init(testFormat(), false); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := m1.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %s", err)
	}

	override := int64(12345)
	m2, err := newKVMeta("badger", fmt.Sprintf("%s?nextchunk=%d", dir, override), testConfig())
	if err != nil {
		t.Fatalf("create meta with nextchunk: %s", err)
	}
	if _, err := m2.Load(false); err != nil {
		t.Fatalf("load formatted meta: %s", err)
	}
	kv2 := m2.(*kvMeta)
	got, err := kv2.getCounter("nextChunk")
	if err != nil {
		t.Fatalf("get nextChunk: %s", err)
	}
	if got != override {
		t.Fatalf("expected nextChunk %d, got %d", override, got)
	}
	if err := m2.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %s", err)
	}

	m3, err := newKVMeta("badger", dir, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if _, err := m3.Load(false); err != nil {
		t.Fatalf("load formatted meta: %s", err)
	}
	kv3 := m3.(*kvMeta)
	got, err = kv3.getCounter("nextChunk")
	if err != nil {
		t.Fatalf("get nextChunk: %s", err)
	}
	if got != override {
		t.Fatalf("expected persisted nextChunk %d, got %d", override, got)
	}
	if err := m3.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %s", err)
	}
}

func TestBadgerNextChunkInvalidValue(t *testing.T) {
	dir := t.TempDir()

	m1, err := newKVMeta("badger", dir, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err := m1.Init(testFormat(), false); err != nil {
		t.Fatalf("init meta: %s", err)
	}
	if err := m1.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %s", err)
	}

	if _, err := newKVMeta("badger", dir+"?nextchunk=not-a-number", testConfig()); err == nil {
		t.Fatalf("expected error for invalid nextchunk value")
	}

	m2, err := newKVMeta("badger", dir, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if _, err := m2.Load(false); err != nil {
		t.Fatalf("load formatted meta: %s", err)
	}
	if err := m2.Shutdown(); err != nil {
		t.Fatalf("shutdown meta: %s", err)
	}
}

func TestBadgerNextChunkDuplicateParam(t *testing.T) {
	dir := t.TempDir()
	if _, err := newKVMeta("badger", dir+"?nextchunk=1&nextchunk=2", testConfig()); err == nil {
		t.Fatalf("expected error for duplicate nextchunk param")
	}
}

func TestBadgerUnknownQueryParam(t *testing.T) {
	dir := t.TempDir()
	if _, err := newKVMeta("badger", dir+"?unknown=1", testConfig()); err == nil {
		t.Fatalf("expected error for unknown query param")
	}
}

func TestParseBadgerAddrOptions_WindowsBackslashes(t *testing.T) {
	opts, err := parseBadgerAddrOptions(`C:\tmp\badger?nextchunk=123`)
	if err != nil {
		t.Fatalf("parse addr: %s", err)
	}
	if opts.dir != `C:\tmp\badger` {
		t.Fatalf("expected dir %q, got %q", `C:\tmp\badger`, opts.dir)
	}
	if !opts.overrideNextChunk || opts.nextChunkValue != 123 {
		t.Fatalf("expected nextchunk override=123, got override=%v value=%d", opts.overrideNextChunk, opts.nextChunkValue)
	}

	opts, err = parseBadgerAddrOptions(`C:\tmp\badger`)
	if err != nil {
		t.Fatalf("parse addr: %s", err)
	}
	if opts.dir != `C:\tmp\badger` {
		t.Fatalf("expected dir %q, got %q", `C:\tmp\badger`, opts.dir)
	}
	if opts.overrideNextChunk {
		t.Fatalf("expected no nextchunk override, got override=%v value=%d", opts.overrideNextChunk, opts.nextChunkValue)
	}
}
