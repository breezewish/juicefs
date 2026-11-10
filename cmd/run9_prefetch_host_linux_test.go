//go:build linux

package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openTestLimiter(t *testing.T, dir string, slots int, bytesPerSecond int64) *hostPrefetchLimiter {
	t.Helper()
	l, err := openHostPrefetchLimiter(dir, slots, bytesPerSecond)
	require.NoError(t, err)
	t.Cleanup(l.close)
	return l
}

func TestHostPrefetchLimiterBoundsConcurrentFetches(t *testing.T) {
	// Two limiters on one directory stand for two mount processes.
	dir := t.TempDir()
	first := openTestLimiter(t, dir, 2, 1<<40)
	second := openTestLimiter(t, dir, 2, 1<<40)

	doneA, err := first.admit(context.Background(), 1)
	require.NoError(t, err)
	doneB, err := second.admit(context.Background(), 1)
	require.NoError(t, err)

	// Both slots are held: a third fetch waits until its context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = second.admit(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// A released slot admits the next fetch.
	doneA(true)
	doneC, err := second.admit(context.Background(), 1)
	require.NoError(t, err)
	doneB(true)
	doneC(true)
}

func TestHostPrefetchLimiterSlotsOfOneMountAreDistinct(t *testing.T) {
	// flock belongs to an open file description: one mount's two fetches must
	// not both take slot 0 through the same description.
	l := openTestLimiter(t, t.TempDir(), 2, 1<<40)
	doneA, err := l.admit(context.Background(), 1)
	require.NoError(t, err)
	doneB, err := l.admit(context.Background(), 1)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = l.admit(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	doneA(true)
	doneB(true)
}

func TestHostPrefetchLimiterGivesAFreedSlotToAWaitingMount(t *testing.T) {
	dir := t.TempDir()
	busy := openTestLimiter(t, dir, 1, 1<<40)
	waiting := openTestLimiter(t, dir, 1, 1<<40)
	done, err := busy.admit(context.Background(), 1)
	require.NoError(t, err)

	admitted := make(chan func(bool))
	go func() {
		next, err := waiting.admit(context.Background(), 1)
		require.NoError(t, err)
		admitted <- next
	}()
	require.Eventually(t, waiting.anyoneWaiting, time.Second, time.Millisecond)

	// The busy mount releases its slot and asks again at once: the mount
	// already waiting gets it first.
	done(true)
	retaken := make(chan func(bool))
	go func() {
		next, err := busy.admit(context.Background(), 1)
		require.NoError(t, err)
		retaken <- next
	}()
	next := <-admitted
	select {
	case <-retaken:
		t.Fatal("the releasing mount retook the slot ahead of the waiting one")
	case <-time.After(50 * time.Millisecond):
	}
	next(true)
	(<-retaken)(true)
}

func TestHostPrefetchLimiterPacesFetchesAcrossMounts(t *testing.T) {
	dir := t.TempDir()
	first := openTestLimiter(t, dir, 4, 1<<20)
	second := openTestLimiter(t, dir, 4, 1<<20)

	// At 1 MiB/s, 256 KiB reserves 250 ms: the next fetch, from either
	// mount, starts that much later.
	startA, _, err := first.reserve(256 << 10)
	require.NoError(t, err)
	startB, _, err := second.reserve(256 << 10)
	require.NoError(t, err)
	startC, nextC, err := first.reserve(256 << 10)
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, startB.Sub(startA))
	require.Equal(t, 250*time.Millisecond, startC.Sub(startB))

	// A fetch that will not run returns the last reservation.
	first.unreserve(startC, nextC)
	startD, nextD, err := second.reserve(256 << 10)
	require.NoError(t, err)
	require.Equal(t, startC.UnixNano(), startD.UnixNano())

	// It cannot return one another fetch has reserved after.
	_, _, err = first.reserve(256 << 10)
	require.NoError(t, err)
	second.unreserve(startD, nextD)
	startF, _, err := first.reserve(256 << 10)
	require.NoError(t, err)
	require.Equal(t, nextD.Add(250*time.Millisecond).UnixNano(), startF.UnixNano())
}

func TestHostPrefetchLimiterReturnsTheReservationOfACancelledFetch(t *testing.T) {
	dir := t.TempDir()
	l := openTestLimiter(t, dir, 4, 1<<20)
	// Push the pace one second ahead, then cancel a fetch waiting behind it.
	ahead, _, err := l.reserve(1 << 20)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = l.admit(ctx, 1<<20)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	start, _, err := l.reserve(1)
	require.NoError(t, err)
	require.Equal(t, ahead.Add(time.Second).UnixNano(), start.UnixNano(), "the cancelled fetch's second is returned")
}
