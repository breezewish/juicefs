//go:build run9_checkpoint_research

package chunk

import (
	"context"
	"fmt"
	"sync"
)

type researchUpload struct {
	bytes int
	done  chan struct{}
	err   error // published before closing done
}

type researchUploadTracker struct {
	sync.Mutex
	pending map[string]*researchUpload
}

func (tracker *researchUploadTracker) start(key string, size int) {
	tracker.Lock()
	defer tracker.Unlock()
	if tracker.pending == nil {
		tracker.pending = make(map[string]*researchUpload)
	}
	if tracker.pending[key] == nil {
		tracker.pending[key] = &researchUpload{bytes: size, done: make(chan struct{})}
	}
}

func (tracker *researchUploadTracker) complete(key string) {
	tracker.Lock()
	defer tracker.Unlock()
	if upload := tracker.pending[key]; upload != nil {
		delete(tracker.pending, key)
		close(upload.done)
	}
}

func (tracker *researchUploadTracker) abandon(key string) {
	tracker.Lock()
	defer tracker.Unlock()
	if upload := tracker.pending[key]; upload != nil {
		upload.err = fmt.Errorf("checkpoint upload abandoned: %s", key)
		delete(tracker.pending, key)
		close(upload.done)
	}
}

// Run9UploadFence waits for the sealed, immutable object uploads that existed
// at capture. Later parent writes cannot extend the fence. The source mount
// must remain alive, and the caller must protect captured slices from GC.
// This research fence is process-local; losing the source before completion
// fails the unpublished snapshot instead of declaring it ready.
type Run9UploadFence struct {
	uploads []*researchUpload
	Blocks  int   `json:"blocks"`
	Bytes   int64 `json:"bytes"`
}

// Run9CaptureUploads must follow FlushAll under the VFS data-write barrier.
// Every staged write registers before acknowledging Finish; successful PUTs
// remove registrations. Capturing under the same mutex makes completion races
// harmless: an object is either already stored, or retained in this fence.
func (store *cachedStore) Run9CaptureUploads() *Run9UploadFence {
	tracker := &store.researchUploads
	tracker.Lock()
	defer tracker.Unlock()
	fence := &Run9UploadFence{}
	for _, upload := range tracker.pending {
		fence.uploads = append(fence.uploads, upload)
		fence.Bytes += int64(upload.bytes)
	}
	fence.Blocks = len(fence.uploads)
	return fence
}

// Wait returns only after every captured object's PUT has succeeded. Upload
// failures continue through the normal uploader's retry path until the request
// is canceled; cancellation never turns a partial snapshot into a ready one.
func (fence *Run9UploadFence) Wait(ctx context.Context) error {
	for _, upload := range fence.uploads {
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

// Run9StageAllWrites is configured before exposing the research mount. It
// removes the normal 2 MiB synchronous-upload exception. Full/unavailable local
// staging still takes JuiceFS's synchronous upload path and can lengthen pause.
func (store *cachedStore) Run9StageAllWrites() {
	store.conf.WritebackThresholdSize = store.conf.BlockSize + 1
}
