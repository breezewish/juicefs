//go:build !nobadger
// +build !nobadger

package meta

import "testing"

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
}
