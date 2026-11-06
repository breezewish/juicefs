//go:build linux

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// forkPageCacheMaxConcurrency bounds the worker threads of one warm request.
const forkPageCacheMaxConcurrency = 64

var (
	forkPageCacheWarmCtx, cancelForkPageCacheWarms = context.WithCancel(context.Background())
	forkPageCacheWarmers                           sync.WaitGroup
)

// routeForkPageCache serves the recorded prewarm page-cache routes: the read
// trace of a recording mount, and warming this mount's page cache.
func routeForkPageCache(mux *http.ServeMux, v *vfs.VFS) {
	mux.HandleFunc("GET /read-trace", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		maxFiles, maxFilesErr := strconv.Atoi(query.Get("max_files"))
		maxBytes, maxBytesErr := strconv.ParseUint(query.Get("max_bytes"), 10, 64)
		maxListBytes, maxListBytesErr := strconv.Atoi(query.Get("max_list_bytes"))
		if maxFilesErr != nil || maxBytesErr != nil || maxListBytesErr != nil || maxFiles <= 0 || maxBytes == 0 || maxListBytes <= 0 {
			http.Error(w, "need positive max_files, max_bytes and max_list_bytes", http.StatusBadRequest)
			return
		}
		// Finalize cannot close the metadata engine under a running trace.
		if !admitForkMountRequest() {
			http.Error(w, "mount is finalizing", http.StatusConflict)
			return
		}
		defer forkMountLifecycle.RUnlock()
		files, err := v.ReadTrace(meta.WrapContext(r.Context()), vfs.ReadTraceLimits{MaxFiles: maxFiles, MaxBytes: maxBytes, MaxListBytes: maxListBytes})
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(forkReadTraceResult{Ok: true, Files: files})
	})
	mux.HandleFunc("POST /warm-page-cache", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		deadline, deadlineErr := strconv.ParseInt(query.Get("deadline_unix_ms"), 10, 64)
		maxBytes, maxBytesErr := strconv.ParseUint(query.Get("max_bytes"), 10, 64)
		concurrency, concurrencyErr := strconv.Atoi(query.Get("concurrency"))
		if deadlineErr != nil || maxBytesErr != nil || concurrencyErr != nil || maxBytes == 0 || concurrency <= 0 || concurrency > forkPageCacheMaxConcurrency {
			http.Error(w, fmt.Sprintf("need deadline_unix_ms, positive max_bytes and concurrency 1..%d", forkPageCacheMaxConcurrency), http.StatusBadRequest)
			return
		}
		var files []vfs.PageCacheFile
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, forkControlMaxBody)).Decode(&files); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), time.UnixMilli(deadline))
		defer cancel()
		done, err := beginForkPageCacheWarm(cancel)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		defer done()
		result, err := v.WarmPageCache(ctx, files, maxBytes, concurrency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(forkPageCacheWarmResult{Ok: true, PageCacheWarmResult: result})
	})
}

// beginForkPageCacheWarm admits one warm unless the mount is finalizing, and
// arranges for stopForkPageCacheWarms to cancel it.
func beginForkPageCacheWarm(cancel context.CancelFunc) (done func(), err error) {
	if !admitForkMountRequest() {
		return nil, errors.New("mount is finalizing")
	}
	defer forkMountLifecycle.RUnlock()
	forkPageCacheWarmers.Add(1)
	stop := context.AfterFunc(forkPageCacheWarmCtx, cancel)
	return func() {
		stop()
		forkPageCacheWarmers.Done()
	}, nil
}

// admitForkMountRequest read-locks forkMountLifecycle unless the mount is
// finalizing; the caller then releases it with RUnlock. It never queues behind
// finalize, which may be joining the control server that serves the request.
func admitForkMountRequest() bool {
	if !forkMountLifecycle.TryRLock() {
		return false
	}
	if forkFinalizeInProgress {
		forkMountLifecycle.RUnlock()
		return false
	}
	return true
}

// stopForkPageCacheWarms cancels running warms and waits until they close their
// files. The caller holds forkMountLifecycle and has set forkFinalizeInProgress,
// so no warm starts afterwards. A file left open on the mount would keep the
// kernel from ending a lazily unmounted FUSE connection, and finalize from
// ever seeing the mount end.
func stopForkPageCacheWarms() {
	cancelForkPageCacheWarms()
	forkPageCacheWarmers.Wait()
}
