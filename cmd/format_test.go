/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package cmd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func TestFixObjectSize(t *testing.T) {
	t.Run("Should make sure the size is in range", func(t *testing.T) {
		cases := []struct {
			input, expected uint64
		}{
			{30 << 10, 64 << 10},
			{0, 64 << 10},
			{2 << 40, 16 << 20},
			{16 << 21, 16 << 20},
		}
		for _, c := range cases {
			if size := fixObjectSize(c.input); size != c.expected {
				t.Fatalf("Expected %d, got %d", c.expected, size)
			}
		}
	})
	t.Run("Should use powers of two", func(t *testing.T) {
		cases := []struct {
			input, expected uint64
		}{
			{150 << 10, 128 << 10},
			{99 << 10, 64 << 10},
			{1077 << 10, 1024 << 10},
		}
		for _, c := range cases {
			if size := fixObjectSize(c.input); size != c.expected {
				t.Fatalf("Expected %d, got %d", c.expected, size)
			}
		}
	})
}

func TestFormat(t *testing.T) {
	rdb := resetTestMeta()
	if err := Main([]string{"", "format", "--bucket", t.TempDir(), testMeta, testVolume}); err != nil {
		t.Fatalf("format error: %s", err)
	}
	body, err := rdb.Get(context.Background(), "setting").Bytes()
	if err != nil {
		t.Fatalf("get setting: %s", err)
	}
	f := meta.Format{}
	if err = json.Unmarshal(body, &f); err != nil {
		t.Fatalf("json unmarshal: %s", err)
	}
	if f.Name != testVolume {
		t.Fatalf("volume name %s != expected %s", f.Name, testVolume)
	}

	if err = Main([]string{"", "format", testMeta, testVolume, "--capacity", "1", "--inodes", "1000"}); err != nil {
		t.Fatalf("format error: %s", err)
	}
	if body, err = rdb.Get(context.Background(), "setting").Bytes(); err != nil {
		t.Fatalf("get setting: %s", err)
	}
	if err = json.Unmarshal(body, &f); err != nil {
		t.Fatalf("json unmarshal: %s", err)
	}
	if f.Capacity != 1<<30 || f.Inodes != 1000 {
		t.Fatalf("unexpected volume: %+v", f)
	}
}

func TestFormatNoUpdateClosesBadgerMeta(t *testing.T) {
	metaDir := t.TempDir()
	bucketDir := t.TempDir()
	metaURI := "badger://" + metaDir
	volume := "testbadger"

	if err := Main([]string{"", "format", "--bucket", bucketDir, metaURI, volume}); err != nil {
		t.Fatalf("format error: %s", err)
	}
	if err := Main([]string{"", "format", "--no-update", metaURI, volume}); err != nil {
		t.Fatalf("format error with --no-update: %s", err)
	}

	opt := badger.DefaultOptions(metaDir)
	opt.Logger = utils.GetLogger("badger-test")
	opt.MetricsEnabled = false
	opt.SkipWAL = true
	db, err := badger.Open(opt)
	if err != nil {
		t.Fatalf("badger should be openable after format: %s", err)
	}
	_ = db.Close()
}
