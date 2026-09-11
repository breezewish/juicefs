//go:build !nobadger
// +build !nobadger

/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/utils"
)

type badgerTxn struct {
	t *badger.Txn
	c *badger.DB
}

func (tx *badgerTxn) get(key []byte) []byte {
	item, err := tx.t.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		panic(err)
	}
	return value
}

func (tx *badgerTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = tx.get(key)
	}
	return values
}

func (tx *badgerTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	var prefix bool
	options := badger.IteratorOptions{}
	if keysOnly {
		options.PrefetchValues = false
		options.PrefetchSize = 0
	}
	if bytes.Equal(nextKey(begin), end) {
		prefix = true
		options.Prefix = begin
	}
	it := tx.t.NewIterator(options)
	if prefix {
		it.Rewind()
	} else {
		it.Seek(begin)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		item := it.Item()
		if !prefix && bytes.Compare(item.Key(), end) >= 0 {
			break
		}
		var value []byte
		if !keysOnly {
			var err error
			value, err = item.ValueCopy(nil)
			if err != nil {
				panic(err)
			}
		}
		if !handler(item.KeyCopy(nil), value) {
			break
		}
	}
}

func (tx *badgerTxn) exist(prefix []byte) bool {
	it := tx.t.NewIterator(badger.IteratorOptions{
		Prefix:       prefix,
		PrefetchSize: 1,
	})
	defer it.Close()
	it.Rewind()
	return it.Valid()
}

func (tx *badgerTxn) set(key, value []byte) {
	if err := tx.t.Set(key, value); err != nil {
		panic(err)
	}
}

func (tx *badgerTxn) append(key []byte, value []byte) {
	list := append(tx.get(key), value...)
	tx.set(key, list)
}

func (tx *badgerTxn) incrBy(key []byte, value int64) int64 {
	buf := tx.get(key)
	newCounter := parseCounter(buf)
	if value != 0 {
		newCounter += value
		tx.set(key, packCounter(newCounter))
	}
	return newCounter
}

func (tx *badgerTxn) delete(key []byte) {
	if err := tx.t.Delete(key); err != nil {
		panic(err)
	}
}

type badgerClient struct {
	// The research physical checkpoint closes and reopens only the database,
	// keeping the metadata session and FUSE handles alive. Every database user,
	// including value-log GC, must participate in this lifecycle gate.
	dbMu      sync.RWMutex
	dbError   error
	client    *badger.DB
	readOnly  bool
	ticker    *time.Ticker
	done      chan struct{}
	gcDone    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (c *badgerClient) name() string {
	return "badger"
}

func (c *badgerClient) shouldRetry(err error) bool {
	return err == badger.ErrConflict
}

func (c *badgerClient) config(key string) interface{} {
	return nil
}

func (c *badgerClient) simpleTxn(ctx context.Context, f func(*kvTxn) error, retry int) (err error) {
	return c.txn(ctx, f, retry)
}

func (c *badgerClient) txn(ctx context.Context, f func(*kvTxn) error, retry int) (err error) {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	if c.dbError != nil {
		return c.dbError
	}
	tx := &badgerTxn{c.client.NewTransaction(!c.readOnly), c.client}
	defer func() { tx.t.Discard() }()
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if ok {
				err = fe
			} else {
				panic(r)
			}
		}
	}()
	err = f(&kvTxn{tx, retry})
	if err != nil {
		return err
	}
	if c.readOnly {
		return nil
	}
	// tx.t may differ from the original
	return tx.t.Commit()
}

func (c *badgerClient) scan(prefix []byte, handler func(key []byte, value []byte) bool) error {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	if c.dbError != nil {
		return c.dbError
	}
	tx := c.client.NewTransaction(false)
	defer tx.Discard()
	it := tx.NewIterator(badger.IteratorOptions{
		Prefix:         prefix,
		PrefetchValues: true,
		PrefetchSize:   10240,
	})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if !handler(item.KeyCopy(nil), value) {
			break
		}
	}
	return nil
}

func (c *badgerClient) reset(prefix []byte) error {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	if c.dbError != nil {
		return c.dbError
	}
	if prefix == nil {
		return c.client.DropAll()
	}
	return c.client.DropPrefix(prefix)
}

func (c *badgerClient) close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.ticker.Stop()
		<-c.gcDone
		c.dbMu.Lock()
		defer c.dbMu.Unlock()
		if c.dbError != nil {
			c.closeErr = c.dbError
			return
		}
		c.closeErr = c.client.Close()
	})
	return c.closeErr
}

func (c *badgerClient) gc() {}

type badgerAddrOptions struct {
	dir string

	overrideNextChunk bool
	nextChunkValue    int64
	readOnly          bool
}

const (
	badgerValueLogFileSize = 64 << 20
	badgerBlockCacheSize   = 16 << 20
	badgerIndexCacheSize   = 16 << 20
)

func parseBadgerAddrOptions(addr string) (badgerAddrOptions, error) {
	// addr is a filesystem path (may contain Windows backslashes), with optional query.
	// Avoid url.Parse("badger://"+addr) because it rejects inputs like `C:\data\badger`.
	dir, rawQuery, hasQuery := strings.Cut(addr, "?")
	if dir == "" {
		return badgerAddrOptions{}, fmt.Errorf("invalid badger address %q: empty path", addr)
	}
	if !hasQuery || rawQuery == "" {
		return badgerAddrOptions{dir: dir}, nil
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return badgerAddrOptions{}, fmt.Errorf("parse badger address query %q: %w", rawQuery, err)
	}
	for key := range query {
		if key != "nextchunk" && key != "readonly" {
			return badgerAddrOptions{}, fmt.Errorf("unsupported badger address query parameter %q (only nextchunk and readonly are supported)", key)
		}
	}

	var opts badgerAddrOptions
	opts.dir = dir
	if nextChunkValues, ok := query["nextchunk"]; ok {
		if len(nextChunkValues) != 1 {
			return badgerAddrOptions{}, fmt.Errorf("badger address nextchunk must be specified once, got %d", len(nextChunkValues))
		}
		nextChunkStr := nextChunkValues[0]
		opts.nextChunkValue, err = strconv.ParseInt(nextChunkStr, 10, 64)
		if err != nil {
			return badgerAddrOptions{}, fmt.Errorf("invalid nextchunk value %q: %w", nextChunkStr, err)
		}
		opts.overrideNextChunk = true
	}
	if readOnlyValues, ok := query["readonly"]; ok {
		if len(readOnlyValues) != 1 {
			return badgerAddrOptions{}, fmt.Errorf("badger address readonly must be specified once, got %d", len(readOnlyValues))
		}
		opts.readOnly, err = strconv.ParseBool(readOnlyValues[0])
		if err != nil {
			return badgerAddrOptions{}, fmt.Errorf("invalid readonly value %q: %w", readOnlyValues[0], err)
		}
	}
	if opts.readOnly && opts.overrideNextChunk {
		return badgerAddrOptions{}, fmt.Errorf("badger address readonly and nextchunk cannot be combined")
	}
	return opts, nil
}

func emitRun9BadgerTrace(event string, start time.Time, dir string, fields map[string]any) {
	if !utils.Run9PerfTraceEnabled() {
		return
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["dur_ms"] = time.Since(start).Milliseconds()
	fields["dir"] = dir
	requestID := filepath.Base(dir)
	if requestID == "meta" {
		requestID = filepath.Base(filepath.Dir(dir))
	}
	utils.EmitRun9PerfTraceEvent("juicefs", event, requestID, fields)
}

func newBadgerClient(addr string) (tkvClient, error) {
	// Fork divergence: allow the query param `nextchunk` in the badger address.
	// `nextchunk=<n>` overrides the tkv counter "nextChunk" (key "CnextChunk") so forked metadata won't conflict.
	opts, err := parseBadgerAddrOptions(addr)
	if err != nil {
		return nil, err
	}
	opt := badger.DefaultOptions(opts.dir)
	opt.Logger = utils.GetLogger("badger")
	opt.MetricsEnabled = false
	// Fork divergence: use customized badger with SkipWAL enabled by default.
	opt.SkipWAL = true
	// Fork divergence: run9 stores badger metadata inside the shared_meta JuiceFS mount.
	// The upstream 1 GiB default preallocates 2 GiB *.vlog files, and we've seen
	// DB.Close surface ENOENT on truncate plus dangling vlog entries on that outer
	// FUSE/object-storage layer. Keep value logs small enough that create/close stays
	// on the boring path while preserving badger's normal lifecycle.
	opt.ValueLogFileSize = badgerValueLogFileSize
	// Fork divergence: run9 opens one small badger metadata DB per mounted snap.
	// The upstream 256 MiB block cache is disproportionate for this workload and
	// adds fixed startup cost on every first exec mount.
	opt.BlockCacheSize = badgerBlockCacheSize
	// Fork divergence: the upstream zero value keeps every SSTable index and
	// bloom filter in memory. run9 opens these DBs on the exec hot path, so keep
	// a small on-demand index cache instead of front-loading all table indexes.
	opt.IndexCacheSize = badgerIndexCacheSize
	// Fork divergence: run9rt owns a distributed per-snap lock before any
	// writable Badger client can open this directory. Badger's local LOCK file
	// duplicates that invariant and adds shared-FUSE metadata round trips to
	// every first exec mount.
	opt.BypassLockGuard = true
	// Fork divergence: offline run9 file readers open a cloned metadata directory
	// without taking a writer lock. Badger's native read-only mode is the safety
	// boundary that prevents those long-lived readers from mutating the clone.
	opt.ReadOnly = opts.readOnly
	openStart := time.Now()
	client, err := badger.Open(opt)
	openFields := map[string]any{
		"override_nextchunk": opts.overrideNextChunk,
		"read_only":          opts.readOnly,
	}
	if err != nil {
		openFields["error"] = err.Error()
	}
	emitRun9BadgerTrace("badger_open_end", openStart, opts.dir, openFields)
	if err != nil {
		return nil, err
	}

	if opts.overrideNextChunk {
		overrideStart := time.Now()
		if err := client.Update(func(txn *badger.Txn) error {
			if err := txn.Set([]byte("CnextChunk"), packCounter(opts.nextChunkValue)); err != nil {
				return err
			}
			limit := opts.nextChunkValue + (1 << 32)
			if opts.nextChunkValue > math.MaxInt64-(1<<32) {
				limit = math.MaxInt64
			}
			return txn.Set([]byte("Crun9NextChunkLimit"), packCounter(limit))
		}); err != nil {
			emitRun9BadgerTrace("badger_open_nextchunk_override_end", overrideStart, opts.dir, map[string]any{
				"nextchunk": opts.nextChunkValue,
				"error":     err.Error(),
			})
			_ = client.Close()
			return nil, fmt.Errorf("failed to set nextchunk to %d: %w", opts.nextChunkValue, err)
		}
		emitRun9BadgerTrace("badger_open_nextchunk_override_end", overrideStart, opts.dir, map[string]any{
			"nextchunk": opts.nextChunkValue,
		})
	}

	ticker := time.NewTicker(time.Hour)
	done := make(chan struct{})
	gcDone := make(chan struct{})
	wrapped := &badgerClient{
		client: client, readOnly: opts.readOnly, ticker: ticker, done: done, gcDone: gcDone,
	}
	go func() {
		defer close(gcDone)
		if opts.readOnly {
			<-done
			return
		}
		for {
			select {
			case <-ticker.C:
				for {
					select {
					case <-done:
						return
					default:
					}
					wrapped.dbMu.RLock()
					err := wrapped.dbError
					if err == nil {
						err = wrapped.client.RunValueLogGC(0.7)
					}
					wrapped.dbMu.RUnlock()
					if err != nil {
						break
					}
				}
			case <-done:
				return
			}
		}
	}()

	return wrapped, nil
}

func init() {
	Register("badger", newKVMeta)
	drivers["badger"] = newBadgerClient
}
