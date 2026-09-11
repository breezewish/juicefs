package chunk

import (
	"context"
	"fmt"
	"sync"
)

type pendingUpload struct {
	bytes int
	done  chan struct{}
	err   error // published before closing done
}

type uploadTracker struct {
	sync.Mutex
	pending map[string]*pendingUpload
}

func (tracker *uploadTracker) start(key string, size int) {
	tracker.Lock()
	defer tracker.Unlock()
	if tracker.pending == nil {
		tracker.pending = make(map[string]*pendingUpload)
	}
	if tracker.pending[key] == nil {
		tracker.pending[key] = &pendingUpload{bytes: size, done: make(chan struct{})}
	}
}

func (tracker *uploadTracker) complete(key string) {
	tracker.Lock()
	defer tracker.Unlock()
	if upload := tracker.pending[key]; upload != nil {
		delete(tracker.pending, key)
		close(upload.done)
	}
}

func (tracker *uploadTracker) abandon(key string) {
	tracker.Lock()
	defer tracker.Unlock()
	if upload := tracker.pending[key]; upload != nil {
		upload.err = fmt.Errorf("checkpoint upload abandoned: %s", key)
		delete(tracker.pending, key)
		close(upload.done)
	}
}

// PendingUploads waits for the sealed, immutable object uploads that existed
// at capture. Later parent writes cannot extend the collection. The source mount
// must remain alive, and the caller must protect captured slices from GC.
// This collection is process-local; losing the source before completion
// fails the unpublished snapshot instead of declaring it ready.
type PendingUploads struct {
	uploads []*pendingUpload
	Blocks  int   `json:"blocks"`
	Bytes   int64 `json:"bytes"`
}

// CaptureUploads must follow FlushAll under the VFS data-write barrier.
// Every staged write registers before acknowledging Finish; successful PUTs
// remove registrations. Capturing under the same mutex makes completion races
// harmless: an object is either already stored, or retained in this collection.
func (store *cachedStore) CaptureUploads() *PendingUploads {
	tracker := &store.uploads
	tracker.Lock()
	defer tracker.Unlock()
	pending := &PendingUploads{}
	for _, upload := range tracker.pending {
		pending.uploads = append(pending.uploads, upload)
		pending.Bytes += int64(upload.bytes)
	}
	pending.Blocks = len(pending.uploads)
	return pending
}

// Wait returns only after every captured object's PUT has succeeded. Upload
// failures continue through the normal uploader's retry path until the request
// is canceled; cancellation never turns a partial snapshot into a ready one.
func (pending *PendingUploads) Wait(ctx context.Context) error {
	for _, upload := range pending.uploads {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-upload.done:
			if upload.err != nil {
				return upload.err
			}
		}
	}
	return ctx.Err()
}
