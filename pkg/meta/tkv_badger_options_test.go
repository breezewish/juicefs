//go:build !nobadger
// +build !nobadger

package meta

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestBadgerClientUsesRun9ValueLogSize(t *testing.T) {
	client, err := newBadgerClient(t.TempDir())
	if err != nil {
		t.Fatalf("create badger client: %s", err)
	}
	defer func() {
		if err := client.close(); err != nil {
			t.Fatalf("close badger client: %s", err)
		}
	}()

	badgerClient := client.(*badgerClient)
	opts := badgerClient.client.Opts()
	if !opts.SkipWAL {
		t.Fatalf("expected SkipWAL=true")
	}
	if opts.ValueLogFileSize != badgerValueLogFileSize {
		t.Fatalf("expected ValueLogFileSize=%d, got %d", badgerValueLogFileSize, opts.ValueLogFileSize)
	}
	if opts.BlockCacheSize != badgerBlockCacheSize {
		t.Fatalf("expected BlockCacheSize=%d, got %d", badgerBlockCacheSize, opts.BlockCacheSize)
	}
	if opts.IndexCacheSize != badgerIndexCacheSize {
		t.Fatalf("expected IndexCacheSize=%d, got %d", badgerIndexCacheSize, opts.IndexCacheSize)
	}
	if !opts.BypassLockGuard {
		t.Fatal("expected BypassLockGuard=true")
	}
}

func TestBadgerClientReadOnlyAddress(t *testing.T) {
	dir := t.TempDir()
	writable, err := newBadgerClient(dir)
	if err != nil {
		t.Fatalf("create writable badger client: %s", err)
	}
	db := writable.(*badgerClient).client
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("key"), []byte("value"))
	}); err != nil {
		t.Fatalf("seed badger: %s", err)
	}
	if err := writable.close(); err != nil {
		t.Fatalf("close writable badger client: %s", err)
	}

	client, err := newBadgerClient(dir + "?readonly=1")
	if err != nil {
		t.Fatalf("create read-only badger client: %s", err)
	}
	defer func() {
		if err := client.close(); err != nil {
			t.Fatalf("close read-only badger client: %s", err)
		}
	}()

	readOnly := client.(*badgerClient)
	if !readOnly.readOnly || !readOnly.client.Opts().ReadOnly {
		t.Fatal("expected native Badger read-only mode")
	}
	if err := readOnly.txn(Background(), func(txn *kvTxn) error {
		if value := txn.get([]byte("key")); string(value) != "value" {
			t.Fatalf("unexpected value %q", value)
		}
		return nil
	}, 0); err != nil {
		t.Fatalf("read seeded value: %s", err)
	}
	if err := readOnly.txn(Background(), func(txn *kvTxn) error {
		txn.set([]byte("other"), []byte("value"))
		return nil
	}, 0); err == nil {
		t.Fatal("expected native Badger read-only mode to reject writes")
	}
}

func TestBadgerAddressRejectsReadOnlyNextChunkCombination(t *testing.T) {
	_, err := parseBadgerAddrOptions(t.TempDir() + "?readonly=1&nextchunk=42")
	if err == nil {
		t.Fatal("expected read-only and nextchunk combination to fail")
	}
}
