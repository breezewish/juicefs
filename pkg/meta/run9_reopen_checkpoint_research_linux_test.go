//go:build linux && run9_checkpoint_research && !nobadger

package meta

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestResearchReopenCheckpointPreservesVersionsAndAllocation(t *testing.T) {
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
		out, err := m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{
			Directory: filepath.Join(root, fmt.Sprint(generation)), Strategy: "physical-async",
		}, func(counter uint64) error {
			require.Equal(t, uint64(4294967396+generation), counter)
			return nil
		})
		require.NoError(t, err)
		require.True(t, previous.IsClosed())
		require.NotSame(t, previous, c.client)
		require.False(t, c.client.IsClosed())
		require.True(t, c.client.Opts().SkipWAL)
		require.Positive(t, out.CloseMS)
		require.Positive(t, out.ReopenMS)
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

func TestResearchReopenCheckpointCancellationStillReopens(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer func() { require.NoError(t, m.Shutdown()) }()
	require.NoError(t, m.Init(testFormat(), false))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	out, err := m.Run9Checkpoint(ctx, Run9CheckpointOptions{
		Directory: filepath.Join(t.TempDir(), "cancelled"), Strategy: "physical-async", ExportDelayMS: 1000,
	}, func(uint64) error { t.Fatal("cancelled copy must not acknowledge capture"); return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Positive(t, out.CloseMS)
	require.Positive(t, out.ReopenMS)
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set([]byte("research-after-cancel"), []byte("writable"))
		return nil
	}, 0))
}

func TestResearchReopenFailureRejectsFurtherDatabaseAccess(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	require.NoError(t, m.Init(testFormat(), false))
	c := m.client.(*badgerClient)
	_, err = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{
		Directory: filepath.Join(t.TempDir(), "unpublished"), Strategy: "physical-async", FailPhase: "during_reopen",
	}, func(uint64) error { t.Fatal("reopen failure must not acknowledge capture"); return nil })
	require.ErrorContains(t, err, "physical checkpoint reopen failed")
	require.True(t, c.client.IsClosed())
	require.ErrorContains(t, c.txn(context.Background(), func(*kvTxn) error {
		t.Fatal("closed database must never reach a transaction callback")
		return nil
	}, 0), "reopen failed")
	require.ErrorContains(t, c.scan(nil, func([]byte, []byte) bool {
		t.Fatal("closed database must never reach a scan callback")
		return false
	}), "reopen failed")
	require.ErrorContains(t, c.reset(nil), "reopen failed")
	require.ErrorContains(t, m.Shutdown(), "reopen failed")
}

func TestResearchCloseFailureRejectsCheckpoint(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	root := t.TempDir()
	source := filepath.Join(root, "source")
	client, err := newKVMeta("badger", source, conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	require.NoError(t, m.Init(testFormat(), false))
	// First checkpoint leaves an empty active memtable. Moving the directory
	// preserves open file descriptors but makes Close's final directory sync
	// fail; this exercises an actual Close error without failing the flusher.
	_, err = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{
		Directory: filepath.Join(root, "before-error"), Strategy: "physical-async",
	}, func(uint64) error { return nil })
	require.NoError(t, err)
	require.NoError(t, os.Rename(source, filepath.Join(root, "moved")))
	out, err := m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{
		Directory: filepath.Join(root, "must-not-exist"), Strategy: "physical-async",
	}, func(uint64) error { t.Fatal("Close failure must not acknowledge capture"); return nil })
	require.ErrorContains(t, err, "physical checkpoint close failed")
	require.Positive(t, out.CloseMS)
	require.Zero(t, out.ReopenMS)
	require.NoDirExists(t, filepath.Join(root, "must-not-exist"))
	require.ErrorContains(t, m.client.txn(context.Background(), func(*kvTxn) error {
		t.Fatal("unproven Close must not allow more database access")
		return nil
	}, 0), "close failed")
	require.ErrorContains(t, m.Shutdown(), "close failed")
}

func TestResearchReopenFatalErrorKeepsClosedMetadataDurable(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	if source := os.Getenv("RUN9_REOPEN_FATAL_SOURCE"); source != "" {
		client, err := newKVMeta("badger", source, conf)
		require.NoError(t, err)
		m := client.(*kvMeta)
		require.NoError(t, m.Init(testFormat(), false))
		require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
			tx.set([]byte("research-durable"), []byte("committed-before-close"))
			return nil
		}, 0))
		_, _ = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{
			Directory: source + "-candidate", Strategy: "physical-async", FailPhase: "reopen_value_directory",
		}, func(uint64) error { fmt.Println("UNEXPECTED_CAPTURE_ACK"); return nil })
		t.Fatal("expected Badger discard-stats initialization to terminate this subprocess")
	}
	source := filepath.Join(t.TempDir(), "source")
	command := exec.Command(os.Args[0], "-test.run=^TestResearchReopenFatalErrorKeepsClosedMetadataDurable$")
	command.Env = append(os.Environ(), "RUN9_REOPEN_FATAL_SOURCE="+source)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Contains(t, string(output), "unable to open:")
	require.Contains(t, string(output), "MANIFEST/DISCARD")
	require.NotContains(t, string(output), "UNEXPECTED_CAPTURE_ACK")
	// The original database was fully closed before Open failed. This does
	// not restore a JuiceFS mount or its pending uploads; it only proves the
	// database itself remains readable and writable under valid I/O options.
	client, err := newKVMeta("badger", source, conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		require.Equal(t, []byte("committed-before-close"), tx.get([]byte("research-durable")))
		tx.set([]byte("research-reopened"), []byte("writable"))
		return nil
	}, 0))
	require.NoError(t, m.Shutdown())
}
