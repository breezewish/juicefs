//go:build !nobadger

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9RetiredSlicesSelectsOnlyNegativeReferencesInWindow(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 0
	client, err := newKVMeta("badger", t.TempDir(), conf)
	require.NoError(t, err)
	m := client.(*kvMeta)
	defer func() { require.NoError(t, m.Shutdown()) }()
	require.NoError(t, m.Init(testFormat(), false))
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set(m.sliceKey(99, 100), packCounter(-1)) // previous mount
		tx.set(m.sliceKey(100, 100), packCounter(-1))
		tx.set(m.sliceKey(101, 100), packCounter(0)) // still one reference
		tx.set(m.sliceKey(102, 100), packCounter(1)) // shared locally
		tx.set(m.sliceKey(103, 100), packCounter(-1))
		tx.set(m.sliceKey(104, 100), packCounter(-1)) // outside window
		return nil
	}, 0))
	slices, next, err := m.Run9RetiredSlices(context.Background(), 100, 104, "", 10)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Equal(t, []Slice{{Id: 100, Size: 100}, {Id: 103, Size: 100}}, slices)
	slices, next, err = m.Run9RetiredSlices(context.Background(), 100, 104, "", 1)
	require.NoError(t, err)
	require.NotEmpty(t, next)
	require.Equal(t, []Slice{{Id: 100, Size: 100}}, slices)
	slices, next, err = m.Run9RetiredSlices(context.Background(), 100, 104, next, 2)
	require.NoError(t, err)
	require.Empty(t, slices) // two live records still advance the cursor
	require.NotEmpty(t, next)
	slices, next, err = m.Run9RetiredSlices(context.Background(), 100, 104, next, 2)
	require.NoError(t, err)
	require.Equal(t, []Slice{{Id: 103, Size: 100}}, slices)
	require.Empty(t, next)
	_, _, err = m.Run9RetiredSlices(context.Background(), 100, 104, "broken", 2)
	require.ErrorContains(t, err, "cursor")
	require.NoError(t, m.client.txn(context.Background(), func(tx *kvTxn) error {
		tx.set(m.sliceKey(103, 100), []byte{1})
		return nil
	}, 0))
	slices, _, err = m.Run9RetiredSlices(context.Background(), 100, 104, "", 10)
	require.ErrorContains(t, err, "malformed")
	require.Nil(t, slices) // never authorize the prefix of an invalid scan
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = m.Run9RetiredSlices(ctx, 100, 104, "", 10)
	require.ErrorIs(t, err, context.Canceled)
}
