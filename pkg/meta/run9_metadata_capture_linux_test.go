//go:build linux && !nobadger

package meta

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMetadataCapturePreservesVersionsAndAllocation(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob, conf.MaxDeletes = true, 0
	client, err := newKVMeta("badger", t.TempDir()+"?nextchunk=4294967296", conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer func() { require.NoError(t, m.Shutdown()) }()
	require.NoError(t, m.Init(testFormat(), false))
	c := m.client.(*badgerClient)
	options := c.client.Opts()
	options.MemTableSize, options.ValueLogFileSize = 1<<20, 1<<20
	options.ValueThreshold = 32
	options.NumLevelZeroTables, options.NumLevelZeroTablesStall = 2, 4
	require.NoError(t, c.client.Close())
	c.client, err = badger.Open(options)
	require.NoError(t, err)
	root := t.TempDir()
	for generation := 0; generation < 5; generation++ {
		for key := 0; key < 1000; key++ {
			require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
				tx.set([]byte(fmt.Sprintf("research-%04d", key)), bytes.Repeat([]byte{byte(generation)}, 2048))
				return nil
			}, 0))
		}
		require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
			tx.set([]byte("CnextChunk"), packCounter(4294967396+int64(generation)))
			if generation == 0 {
				tx.set([]byte("research-deleted"), []byte("original"))
			} else {
				tx.delete([]byte("research-deleted"))
			}
			return nil
		}, 0))
		previous := c.client
		out, err := m.Run9CaptureMetadata(context.Background(), filepath.Join(root, fmt.Sprint(generation)))
		require.Equal(t, uint64(4294967396+generation), out.AllocationEnd)
		require.NoError(t, err)
		require.True(t, previous.IsClosed())
		require.NotSame(t, previous, c.client)
		require.False(t, c.client.IsClosed())
		require.True(t, c.client.Opts().SkipWAL)
		require.Positive(t, out.CloseMS)
		require.Positive(t, out.OpenMS)
		require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
			require.Equal(t, int64(4294967396+generation), parseCounter(tx.get([]byte("CnextChunk"))))
			require.Equal(t, int64(8589934592), parseCounter(tx.get([]byte("Crun9NextChunkLimit"))))
			return nil
		}, 0))
	}
	require.NoError(t, c.client.Flatten(2))
	require.NoError(t, c.client.RunValueLogGC(0.1), "actually reclaim source value logs after repeated reopen")
	for generation := 0; generation < 5; generation++ {
		child, err := newKVMeta("badger", filepath.Join(root, fmt.Sprint(generation))+"?readonly=1", conf)
		require.NoError(t, err)
		reader := child.(*kvMeta).client.(*badgerClient)
		require.NoError(t, reader.client.VerifyChecksum())
		require.NoError(t, reader.txn(context.Background(), func(tx *kvTxn) error {
			for key := 0; key < 1000; key++ {
				require.Equal(t, bytes.Repeat([]byte{byte(generation)}, 2048), tx.get([]byte(fmt.Sprintf("research-%04d", key))))
			}
			if generation == 0 {
				require.Equal(t, []byte("original"), tx.get([]byte("research-deleted")))
			} else {
				require.Nil(t, tx.get([]byte("research-deleted")))
			}
			return nil
		}, 0))
		require.NoError(t, child.Shutdown())
	}
}

func TestMetadataCaptureCloseFailurePoisonsSource(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	root := t.TempDir()
	source := filepath.Join(root, "source")
	client, err := newKVMeta("badger", source, conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	require.NoError(t, m.Init(testFormat(), false))
	_, err = m.Run9CaptureMetadata(context.Background(), filepath.Join(root, "first"))
	require.NoError(t, err)
	// Empty memtable: moving the path keeps open descriptors valid but makes
	// Badger Close's final directory sync fail with a real filesystem error.
	require.NoError(t, os.Rename(source, filepath.Join(root, "moved")))
	_, err = m.Run9CaptureMetadata(context.Background(), filepath.Join(root, "failed"))
	require.ErrorIs(t, err, ErrRun9MetadataUnavailable)
	require.ErrorContains(t, err, "close failed")
	require.NoDirExists(t, filepath.Join(root, "failed"))
	c := m.client.(*badgerClient)
	require.ErrorIs(t, c.txn(context.Background(), func(*kvTxn) error {
		t.Fatal("unavailable source must not enter a transaction")
		return nil
	}, 0), ErrRun9MetadataUnavailable)
	require.ErrorIs(t, c.scan(nil, func([]byte, []byte) bool {
		t.Fatal("unavailable source must not enter a scan")
		return false
	}), ErrRun9MetadataUnavailable)
	require.ErrorIs(t, c.reset(nil), ErrRun9MetadataUnavailable)
	require.ErrorIs(t, m.Shutdown(), ErrRun9MetadataUnavailable)
}

func TestMetadataCaptureCopyFailureReopensSource(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer func() { require.NoError(t, m.Shutdown()) }()
	require.NoError(t, m.Init(testFormat(), false))
	c := m.client.(*badgerClient)
	previous := c.client
	// The destination's missing parent fails mkdir after Close has succeeded.
	_, err = m.Run9CaptureMetadata(context.Background(), filepath.Join(t.TempDir(), "missing", "copy"))
	require.Error(t, err)
	require.True(t, previous.IsClosed())
	require.NotSame(t, previous, c.client)
	require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
		tx.set([]byte("after-copy-error"), []byte("writable"))
		return nil
	}, 0))
}

func TestBadgerScanCallbacksReleaseDatabaseBeforeMaintenance(t *testing.T) {
	client, err := newBadgerClient(t.TempDir())
	require.NoError(t, err)
	c := client.(*badgerClient)
	defer func() { require.NoError(t, c.close()) }()
	require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
		for i := 0; i < 1100; i++ {
			tx.set([]byte(fmt.Sprintf("key-%04d", i)), []byte("before"))
		}
		return nil
	}, 0))
	seen := 0
	require.NoError(t, c.scan([]byte("key-"), func(key, value []byte) bool {
		// A queued capture must be able to acquire the write side even when a
		// maintenance callback is about to re-enter this same database.
		locked := c.dbMu.TryLock()
		require.True(t, locked)
		c.dbMu.Unlock()
		require.Equal(t, []byte("before"), value)
		require.NoError(t, c.txn(context.Background(), func(tx *kvTxn) error {
			tx.delete(key)
			return nil
		}, 0))
		seen++
		return true
	}))
	require.Equal(t, 1100, seen, "resume after a deleted cursor without skipping the next page")
}
