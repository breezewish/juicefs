//go:build !nobadger

package meta

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Pause inside the real transaction, after Badger's lifecycle read lock is held.
type pausedDumpClient struct {
	tkvClient
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (c *pausedDumpClient) txn(ctx context.Context, f func(*kvTxn) error, retry int) error {
	return c.tkvClient.txn(ctx, func(tx *kvTxn) error {
		c.once.Do(func() {
			close(c.entered)
			<-c.resume
		})
		return f(tx)
	}, retry)
}

func TestBadgerDirectoryDumpFinishesWithShutdownWaiting(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	require.NoError(t, m.Init(testFormat(), false))
	var child Ino
	require.Zero(t, m.Mkdir(Background(), RootInode, "child", 0755, 0, 0, &child, &Attr{}))
	c := m.client.(*badgerClient)
	paused := &pausedDumpClient{tkvClient: c, entered: make(chan struct{}), resume: make(chan struct{})}
	m.client = paused
	entry := &DumpedEntry{Attr: &DumpedAttr{}}
	dumped := make(chan error, 1)
	go func() { dumped <- m.dumpEntry(RootInode, entry, nil) }()
	select {
	case <-paused.entered:
	case <-time.After(time.Second):
		t.Fatal("directory dump did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- m.Shutdown() }()
	// The paused reader prevents the writer from acquiring the lock. A refused
	// new reader therefore proves Shutdown is queued before the directory scan.
	require.Eventually(t, func() bool {
		if c.dbMu.TryRLock() {
			c.dbMu.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	close(paused.resume)
	select {
	case err := <-dumped:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("directory dump deadlocked with database shutdown")
	}
	require.Contains(t, entry.Entries, "child")
	require.Equal(t, child, entry.Entries["child"].Attr.Inode)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("database shutdown did not finish after directory dump")
	}
}
