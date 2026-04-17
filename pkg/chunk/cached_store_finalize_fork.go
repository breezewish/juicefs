package chunk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

var errPendingStagingMissing = errors.New("pending staging file missing")

var statPathForUploadDrain = os.Stat

// WaitForUploadDrain waits until writeback uploads are fully drained.
//
// Fork divergence: this is used by the `umount-finalize` flow to provide stronger
// correctness guarantees when badger is running with SkipWAL enabled.
//
// It waits until:
//  1. There is no pending staging key (len(pendingKeys) == 0)
//  2. There is no in-flight uploading request (len(currentUpload) == 0)
//  3. All cache directories have empty `rawstaging` directory
func (store *cachedStore) WaitForUploadDrain(ctx context.Context) error {
	if !store.conf.Writeback {
		return nil
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		done, err := store.isUploadDrained()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (store *cachedStore) isUploadDrained() (bool, error) {
	store.pendingMutex.Lock()
	pendingItems := make([]*pendingItem, 0, len(store.pendingKeys))
	for _, item := range store.pendingKeys {
		pendingItems = append(pendingItems, item)
	}
	store.pendingMutex.Unlock()

	if len(pendingItems) != 0 {
		for _, item := range pendingItems {
			if item == nil || item.uploading.Load() {
				continue
			}
			if item.fpath == "" {
				if !store.isCurrentPendingSnapshot(item) {
					continue
				}
				return false, fmt.Errorf("%w: key %s has empty staging path", errPendingStagingMissing, item.key)
			}
			if _, err := statPathForUploadDrain(item.fpath); err != nil {
				if os.IsNotExist(err) {
					if !store.isCurrentPendingSnapshot(item) {
						continue
					}
					if store.pendingBlockStillReadable(item.key) {
						return false, nil
					}
					return false, fmt.Errorf("%w: key %s path %s", errPendingStagingMissing, item.key, item.fpath)
				}
				return false, fmt.Errorf("stat pending staging file for key %s path %s: %w", item.key, item.fpath, err)
			}
		}
		return false, nil
	}
	if len(store.currentUpload) != 0 {
		return false, nil
	}

	stagingRoots, err := store.listStagingRoots()
	if err != nil {
		return false, err
	}
	if len(stagingRoots) == 0 {
		return false, errors.New("empty staging roots in writeback mode")
	}
	for _, root := range stagingRoots {
		empty, err := isDirEmptyRecursive(root)
		if err != nil {
			return false, err
		}
		if !empty {
			return false, nil
		}
	}
	return true, nil
}

func (store *cachedStore) isCurrentPendingSnapshot(item *pendingItem) bool {
	if item == nil {
		return false
	}
	store.pendingMutex.Lock()
	current := store.pendingKeys[item.key]
	store.pendingMutex.Unlock()
	return current != nil && current == item && !current.uploading.Load() && current.fpath == item.fpath
}

func (store *cachedStore) pendingBlockStillReadable(key string) bool {
	if store == nil || store.bcache == nil {
		return false
	}
	_, ok := store.bcache.exist(key)
	return ok
}

func (store *cachedStore) listStagingRoots() ([]string, error) {
	switch m := store.bcache.(type) {
	case *cacheManager:
		m.Lock()
		roots := make([]string, 0, len(m.stores))
		for _, s := range m.stores {
			if s == nil {
				continue
			}
			roots = append(roots, filepath.Join(s.dir, stagingDir))
		}
		m.Unlock()
		return roots, nil
	case *memcache:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected cache manager type %T", store.bcache)
	}
}

func isDirEmptyRecursive(dir string) (bool, error) {
	_, err := statPathForUploadDrain(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}

	var errNotEmpty = errors.New("not empty")
	err = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		return errNotEmpty
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, errNotEmpty) {
		return false, nil
	}
	return false, err
}
