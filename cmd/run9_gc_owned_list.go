package cmd

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

// Walk only existing million-ID directories intersecting ownership. This keeps
// a small Snap from enumerating all its ancestors' blocks. Stores without
// delimiter listing (including sharded stores) retain the flat-list fallback.
// Both paths emit keys in lexical order, so the same resume cursor is valid.
func listRun9GCOwnedObjects(ctx context.Context, store object.ObjectStorage, req run9GCSliceRangesRequest) (<-chan object.Object, *run9GCSliceRangeScanStats) {
	out := make(chan object.Object, run9GCSliceRangesListPageSize)
	stats := newRun9GCSliceRangeScanStats()
	forward := func(prefix string) error {
		objects, part, err := listRun9GCSliceRangeObjects(ctx, store, prefix, req.After, true)
		if err != nil {
			return err
		}
		for obj := range objects {
			select {
			case out <- obj:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		<-part.done
		requests, listed := part.snapshot()
		stats.listRequests.Add(requests)
		stats.listedObjects.Add(listed)
		return part.err
	}
	var walk func(string, int) error
	walk = func(prefix string, depth int) error {
		marker, token := "", ""
		for {
			entries, more, next, err := store.List(ctx, prefix, marker, token, "/", run9GCSliceRangesListPageSize, true)
			if errors.Is(err, utils.ENOTSUP) {
				return forward(prefix)
			}
			if err != nil {
				return err
			}
			stats.record(len(entries))
			for _, entry := range entries {
				if entry == nil {
					return fmt.Errorf("invalid directory listing")
				}
				key := entry.Key()
				marker = key
				if !entry.IsDir() || key == prefix || (key <= req.After && !strings.HasPrefix(req.After, key)) {
					continue
				}
				name := strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/")
				if req.ObjectLayout.HashPrefix && depth == 0 {
					hash, err := strconv.ParseUint(name, 16, 8)
					if err != nil || name != fmt.Sprintf("%02X", hash) {
						continue
					}
					if err := walk(key, 1); err != nil {
						return err
					}
					continue
				}
				million, err := strconv.ParseUint(name, 10, 64)
				if err != nil || name != strconv.FormatUint(million, 10) {
					continue
				}
				for _, r := range req.Ranges {
					// Divide bounds instead of multiplying directory IDs to avoid overflow.
					if r.Start/1000000 <= million && million <= r.EndInclusive/1000000 {
						if err := forward(key); err != nil {
							return err
						}
						break
					}
				}
			}
			if !more {
				return nil
			}
			token = next
		}
	}
	go func() {
		defer close(out)
		defer close(stats.done)
		if err := walk("chunks/", 0); err != nil && ctx.Err() == nil {
			stats.err = err
		}
	}()
	return out, stats
}
