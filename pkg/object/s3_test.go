/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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

package object

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_s3client_full_string(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{endpoint: "s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "http://s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "s3://s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "https://mybucket.s3.us-east-2.amazonaws.com", want: "s3://mybucket/"},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			stor, err := newS3(tt.endpoint, "", "", "")
			if err != nil {
				t.Fatalf("newS3() error = %v", err)
			}
			assert.Equalf(t, tt.want, stor.String(), "Display full address of s3 compatible object storage")
		})
	}
}

func TestS3ClientDeleteObjectsRetriesSlowDown(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if !r.URL.Query().Has("delete") {
			t.Errorf("query = %s, want delete", r.URL.RawQuery)
		}

		w.Header().Set("Content-Type", "application/xml")
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`))
	}))
	t.Cleanup(server.Close)

	storage, err := newS3(server.URL+"/bucket", "access-key", "secret-key", "")
	require.NoError(t, err)
	client, ok := storage.(*s3client)
	require.True(t, ok)

	err = client.DeleteObjects(context.Background(), []string{"chunks/0/0/1_0_1"})
	require.NoError(t, err)
	require.Equal(t, int32(2), calls.Load())
}

func TestS3ClientListRetriesSlowDown(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Errorf("unexpected listing request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/xml")
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`))
			return
		}
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>chunks/0/0/1_0_1</Key><Size>1</Size><LastModified>2026-10-14T00:00:00Z</LastModified></Contents></ListBucketResult>`))
	}))
	t.Cleanup(server.Close)
	storage, err := newS3(server.URL+"/bucket", "access-key", "secret-key", "")
	require.NoError(t, err)
	objects, more, token, err := storage.List(context.Background(), "chunks/", "", "", "", 1000, false)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.Equal(t, "chunks/0/0/1_0_1", objects[0].Key())
	require.False(t, more)
	require.Empty(t, token)
	require.Equal(t, int32(2), calls.Load())
}

func TestS3ClientListDoesNotRetryAccessDenied(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>Access denied</Message></Error>`))
	}))
	t.Cleanup(server.Close)
	storage, err := newS3(server.URL+"/bucket", "access-key", "secret-key", "")
	require.NoError(t, err)
	objects, more, token, err := storage.List(context.Background(), "chunks/", "", "", "", 1000, false)
	require.ErrorContains(t, err, "AccessDenied")
	require.Empty(t, objects)
	require.False(t, more)
	require.Empty(t, token)
	require.Equal(t, int32(1), calls.Load())
}
