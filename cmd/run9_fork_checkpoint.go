package cmd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

const run9ForkCheckpointSlicesHeader = "run9-fork-checkpoint-slices-v1\n"

var errRun9ForkCheckpointScanPostponed = errors.New("not scanned yet: this round's scan budget is used up")

// Cached lists and previously failed scans do not consume the metadata scan
// budget, so each attempt can advance to another unscanned copy.
type run9ForkCheckpointScan struct {
	remaining time.Duration
	failed    map[string]string
}

// run9ForkCheckpointSlices returns every slice referenced by the metadata a
// child received at Fork. That set never changes, so the first strict scan
// stores it as <directory>/slices and later rounds read the list instead of
// <directory>/meta. The result is deletion evidence: any unreadable or
// inconsistent source is an error, and the caller then protects by the fork
// bound instead. Only first-time scans consume scan.remaining.
func run9ForkCheckpointSlices(ctx context.Context, directory string, format string, scan *run9ForkCheckpointScan) ([]uint64, error) {
	path := filepath.Join(directory, "slices")
	raw, err := os.ReadFile(path)
	if err == nil {
		return decodeRun9ForkCheckpointSlices(raw)
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if reason, failed := scan.failed[directory]; failed {
		return nil, errors.New(reason)
	}
	if scan.remaining <= 0 {
		return nil, errRun9ForkCheckpointScanPostponed
	}
	started := time.Now()
	defer func() { scan.remaining -= time.Since(started) }()
	private, err := copyRun9GCCheckpoint(ctx, filepath.Join(directory, "meta"))
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(private)
	m, err := meta.OpenRun9Checkpoint(private)
	if err != nil {
		return nil, err
	}
	defer m.Shutdown()
	loaded, err := m.Load(true)
	if err != nil {
		return nil, err
	}
	if loaded.Name != format {
		return nil, fmt.Errorf("fork checkpoint belongs to another format")
	}
	ids, err := m.Run9CheckpointSlices(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.Shutdown(); err != nil {
		return nil, err
	}
	// Publish only a complete, synced file that reads back as the scan result:
	// the caller removes meta afterwards.
	encoded := encodeRun9ForkCheckpointSlices(ids)
	if decoded, err := decodeRun9ForkCheckpointSlices(encoded); err != nil || !slices.Equal(decoded, ids) {
		return nil, fmt.Errorf("fork checkpoint slices do not read back as scanned")
	}
	tmp, err := os.CreateTemp(directory, ".slices-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(encoded); err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	return ids, os.Rename(tmp.Name(), path)
}

// The list is the header, the count, each ascending ID as the difference from
// its predecessor, and a CRC-32 of everything before it.
func encodeRun9ForkCheckpointSlices(ids []uint64) []byte {
	out := binary.AppendUvarint([]byte(run9ForkCheckpointSlicesHeader), uint64(len(ids)))
	var previous uint64
	for _, id := range ids {
		out = binary.AppendUvarint(out, id-previous)
		previous = id
	}
	return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
}

func decodeRun9ForkCheckpointSlices(raw []byte) ([]uint64, error) {
	header := len(run9ForkCheckpointSlicesHeader)
	if len(raw) < header+4 || string(raw[:header]) != run9ForkCheckpointSlicesHeader {
		return nil, fmt.Errorf("malformed fork checkpoint slices")
	}
	body, sum := raw[:len(raw)-4], binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, fmt.Errorf("malformed fork checkpoint slices")
	}
	body = body[header:]
	count, n := binary.Uvarint(body)
	if n <= 0 || count > uint64(len(body)) {
		return nil, fmt.Errorf("malformed fork checkpoint slices")
	}
	body = body[n:]
	ids := make([]uint64, 0, count)
	var previous uint64
	for range count {
		delta, n := binary.Uvarint(body)
		// Slice ID zero is never allocated, so every difference is positive.
		if n <= 0 || delta == 0 || previous+delta < previous {
			return nil, fmt.Errorf("malformed fork checkpoint slices")
		}
		previous += delta
		ids = append(ids, previous)
		body = body[n:]
	}
	if len(body) != 0 {
		return nil, fmt.Errorf("malformed fork checkpoint slices")
	}
	return ids, nil
}
