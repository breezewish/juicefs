//go:build linux && run9_checkpoint_research && !nobadger

package meta

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResearchCheckpointKeepsOneViewWhileSourceChanges(t *testing.T) {
	for _, strategy := range []string{"physical", "logical", "logical-blocking"} {
		t.Run(strategy, func(t *testing.T) {
			conf := testConfig()
			conf.NoBGJob = true
			conf.MaxDeletes = 0
			client, err := newKVMeta("badger", t.TempDir(), conf)
			require.NoError(t, err)
			m := client.(*kvMeta)
			defer func() { require.NoError(t, m.Shutdown()) }()
			require.NoError(t, m.Init(testFormat(), false))
			require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
				tx.set([]byte("CnextChunk"), packCounter(4096))
				tx.set([]byte("research-a"), []byte("before"))
				tx.set([]byte("research-b"), []byte("before"))
				return nil
			}, 0))
			dest := filepath.Join(t.TempDir(), "snapshot")
			out, err := m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{Directory: dest, Strategy: strategy}, func(counter uint64) error {
				require.Equal(t, uint64(4096), counter)
				return m.client.txn(context.Background(), func(tx *kvTxn) error {
					tx.set([]byte("research-a"), []byte("after"))
					tx.delete([]byte("research-b"))
					tx.set([]byte("research-c"), []byte("new"))
					return nil
				}, 0)
			})
			require.NoError(t, err)
			require.Equal(t, uint64(4096), out.Counter)
			child, err := newKVMeta("badger", dest, conf)
			require.NoError(t, err)
			defer func() { require.NoError(t, child.Shutdown()) }()
			require.NoError(t, child.(*kvMeta).client.txn(context.Background(), func(tx *kvTxn) error {
				require.Equal(t, []byte("before"), tx.get([]byte("research-a")))
				require.Equal(t, []byte("before"), tx.get([]byte("research-b")))
				require.Nil(t, tx.get([]byte("research-c")))
				return nil
			}, 0))
			require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
				require.Equal(t, []byte("after"), tx.get([]byte("research-a")))
				require.Nil(t, tx.get([]byte("research-b")))
				return nil
			}, 0))
		})
	}
}

func TestResearchPhysicalCheckpointReopensAfterCopyFailure(t *testing.T) {
	for _, phase := range []string{"after_close", "during_copy"} {
		t.Run(phase, func(t *testing.T) {
			conf := testConfig()
			conf.NoBGJob = true
			client, err := newKVMeta("badger", t.TempDir(), conf)
			require.NoError(t, err)
			m := client.(*kvMeta)
			defer func() { require.NoError(t, m.Shutdown()) }()
			require.NoError(t, m.Init(testFormat(), false))
			require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error { tx.set([]byte("CnextChunk"), packCounter(8192)); return nil }, 0))
			_, err = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{Directory: filepath.Join(t.TempDir(), "failed"), Strategy: "physical", FailPhase: phase}, func(uint64) error { t.Fatal("failed clone must not acknowledge capture"); return nil })
			require.ErrorContains(t, err, "injected failure")
			require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error { tx.set([]byte("research-after-failure"), []byte("writable")); return nil }, 0))
			_, err = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{Directory: filepath.Join(t.TempDir(), "retry"), Strategy: "physical"}, func(uint64) error { return nil })
			require.NoError(t, err)
		})
	}
}

func TestResearchLogicalCheckpointCancellationReleasesSnapshot(t *testing.T) {
	conf := testConfig()
	conf.NoBGJob = true
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer func() { require.NoError(t, m.Shutdown()) }()
	require.NoError(t, m.Init(testFormat(), false))
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error { tx.set([]byte("CnextChunk"), packCounter(4096)); return nil }, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	_, err = m.Run9Checkpoint(ctx, Run9CheckpointOptions{Directory: filepath.Join(t.TempDir(), "cancelled"), Strategy: "logical", ExportDelayMS: 5000}, func(uint64) error { cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Second)
	// A following physical checkpoint needs the exclusive database lock. It
	// proves that the cancelled logical read transaction released its lifetime.
	_, err = m.Run9Checkpoint(context.Background(), Run9CheckpointOptions{Directory: filepath.Join(t.TempDir(), "next"), Strategy: "physical"}, func(uint64) error { return nil })
	require.NoError(t, err)
}
