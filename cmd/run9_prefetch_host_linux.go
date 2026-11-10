//go:build linux

package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// hostPrefetchLimiter bounds the prefetches of every mount on a host, each its
// own process, through lock files in one host directory:
//
//   - slot-<i>, for i below slots: a fetch holds an exclusive flock on one for
//     its duration, so at most slots fetches run at once. The kernel drops the
//     lock when its holder exits, so a crashed mount leaks no slot.
//   - waiting: every process with a fetch waiting for a slot holds a shared
//     flock on it. A fetch that finds anyone waiting first yields for two
//     slot polls, so every fetch already waiting tries a freed slot before it
//     does: a mount that just released a slot cannot keep taking it back from
//     the others.
//   - pace: holds the time, in Unix nanoseconds, from which the next fetch may
//     start. Each fetch reserves size/bytesPerSecond after it under an
//     exclusive flock and waits until its own start, so fetches start at no
//     more than bytesPerSecond in total. A fetch cancelled before its start
//     returns its reservation if no later fetch has reserved after it.
//
// Every lock is taken on a file description this process opened once: flock
// locks belong to open file descriptions, so two of this mount's fetches
// never share one slot's lock.
type hostPrefetchLimiter struct {
	dir            string
	bytesPerSecond int64

	mu      sync.Mutex
	slots   []*os.File
	held    []bool
	waiting *os.File // shared-locked while waiters > 0
	waiters int
	probe   *os.File // tries an exclusive lock to see whether anyone waits
}

// hostPrefetchSlotPoll is how often a fetch retries when every slot is held.
// A held slot is held for a block fetch, tens to hundreds of milliseconds.
const hostPrefetchSlotPoll = 20 * time.Millisecond

// openHostPrefetchLimiter opens dir's lock files for this mount; close it
// when the prefetch ends.
func openHostPrefetchLimiter(dir string, slots int, bytesPerSecond int64) (*hostPrefetchLimiter, error) {
	l := &hostPrefetchLimiter{dir: dir, bytesPerSecond: bytesPerSecond, held: make([]bool, slots)}
	open := func(name string) (*os.File, error) {
		return os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, 0o600)
	}
	var err error
	for i := range slots {
		var f *os.File
		if f, err = open(fmt.Sprintf("slot-%d", i)); err != nil {
			break
		}
		l.slots = append(l.slots, f)
	}
	if err == nil {
		l.waiting, err = open("waiting")
	}
	if err == nil {
		l.probe, err = open("waiting")
	}
	if err != nil {
		l.close()
		return nil, err
	}
	return l, nil
}

func (l *hostPrefetchLimiter) close() {
	for _, f := range append(l.slots, l.waiting, l.probe) {
		if f != nil {
			_ = f.Close()
		}
	}
}

// admit waits for a free slot and the fetch's turn in the host's bandwidth,
// and returns the release of the slot.
func (l *hostPrefetchLimiter) admit(ctx context.Context, size int) (func(bool), error) {
	if l.anyoneWaiting() {
		if err := sleepUntil(ctx, time.Now().Add(2*hostPrefetchSlotPoll)); err != nil {
			return nil, err
		}
	}
	slot, err := l.acquireSlot(ctx)
	if err != nil {
		return nil, err
	}
	start, next, err := l.reserve(size)
	if err == nil {
		if err = sleepUntil(ctx, start); err != nil {
			l.unreserve(start, next)
		}
	}
	if err != nil {
		l.releaseSlot(slot)
		return nil, err
	}
	return func(bool) { l.releaseSlot(slot) }, nil
}

func (l *hostPrefetchLimiter) acquireSlot(ctx context.Context) (int, error) {
	if slot, err := l.tryAcquireSlot(); slot >= 0 || err != nil {
		return slot, err
	}
	if err := l.beginWaiting(); err != nil {
		return -1, err
	}
	defer l.endWaiting()
	for {
		select {
		case <-ctx.Done():
			return -1, context.Cause(ctx)
		case <-time.After(hostPrefetchSlotPoll):
		}
		if slot, err := l.tryAcquireSlot(); slot >= 0 || err != nil {
			return slot, err
		}
	}
}

// tryAcquireSlot locks a slot no process holds, or returns -1.
func (l *hostPrefetchLimiter) tryAcquireSlot() (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, f := range l.slots {
		if l.held[i] {
			continue
		}
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			l.held[i] = true
			return i, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return -1, fmt.Errorf("lock prefetch slot %d: %w", i, err)
		}
	}
	return -1, nil
}

func (l *hostPrefetchLimiter) releaseSlot(slot int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = unix.Flock(int(l.slots[slot].Fd()), unix.LOCK_UN)
	l.held[slot] = false
}

func (l *hostPrefetchLimiter) beginWaiting() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.waiters == 0 {
		if err := unix.Flock(int(l.waiting.Fd()), unix.LOCK_SH); err != nil {
			return fmt.Errorf("mark prefetch waiting: %w", err)
		}
	}
	l.waiters++
	return nil
}

func (l *hostPrefetchLimiter) endWaiting() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waiters--
	if l.waiters == 0 {
		_ = unix.Flock(int(l.waiting.Fd()), unix.LOCK_UN)
	}
}

// anyoneWaiting reports whether a fetch of any mount, this one included,
// waits for a slot.
func (l *hostPrefetchLimiter) anyoneWaiting() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if unix.Flock(int(l.probe.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		return true
	}
	_ = unix.Flock(int(l.probe.Fd()), unix.LOCK_UN)
	return false
}

// reserve takes size bytes of the host's bandwidth and returns when the fetch
// may start and when the next one may.
func (l *hostPrefetchLimiter) reserve(size int) (start, next time.Time, err error) {
	err = l.withPace(func(f *os.File) error {
		stored, err := readPace(f)
		if err != nil {
			return err
		}
		now := time.Now()
		start = stored
		// Holders reserve at most one block each, so a later start than that
		// comes from a wall clock stepped back; start from now instead.
		if start.Before(now) || start.After(now.Add(l.maxAhead())) {
			start = now
		}
		next = start.Add(time.Duration(float64(size) / float64(l.bytesPerSecond) * float64(time.Second)))
		return writePace(f, next)
	})
	return start, next, err
}

// unreserve returns the reservation [start, next) of a fetch that will not
// run, unless another fetch has reserved after it.
func (l *hostPrefetchLimiter) unreserve(start, next time.Time) {
	_ = l.withPace(func(f *os.File) error {
		stored, err := readPace(f)
		if err != nil || !stored.Equal(next) {
			return err
		}
		return writePace(f, start)
	})
}

func (l *hostPrefetchLimiter) withPace(fn func(*os.File) error) error {
	f, err := os.OpenFile(filepath.Join(l.dir, "pace"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock prefetch pace: %w", err)
	}
	return fn(f)
}

func readPace(f *os.File) (time.Time, error) {
	var buf [8]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil && err != io.EOF {
		return time.Time{}, err
	}
	return time.Unix(0, int64(binary.LittleEndian.Uint64(buf[:]))), nil
}

func writePace(f *os.File, t time.Time) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(t.UnixNano()))
	_, err := f.WriteAt(buf[:], 0)
	return err
}

// maxAhead is the furthest the pace can run ahead of now: every slot holder
// reserving one maximal block.
func (l *hostPrefetchLimiter) maxAhead() time.Duration {
	const maxBlockSize = 16 << 20
	return time.Duration(float64(len(l.slots)*maxBlockSize) / float64(l.bytesPerSecond) * float64(time.Second))
}

func sleepUntil(ctx context.Context, t time.Time) error {
	wait := time.Until(t)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}
