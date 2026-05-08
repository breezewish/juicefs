package object

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type bulkDeleteStub struct {
	ObjectStorage
}

func (s *bulkDeleteStub) DeleteObjects(context.Context, []string, ...AttrGetter) error { return nil }

func TestSupportsBulkDeleteThroughWrappers(t *testing.T) {
	require.False(t, SupportsBulkDelete(mustNewMemStore(t)))

	base := &bulkDeleteStub{ObjectStorage: mustNewMemStore(t)}
	require.True(t, SupportsBulkDelete(base))
	require.True(t, SupportsBulkDelete(WithPrefix(base, "chunks/")))

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encryptor := NewRSAEncryptor(privKey)
	dataEncryptor, err := NewDataEncryptor(encryptor, AES256GCM_RSA)
	require.NoError(t, err)
	require.True(t, SupportsBulkDelete(NewEncrypted(WithPrefix(base, "chunks/"), dataEncryptor)))
}

func TestSupportsBulkDeleteForShardedStores(t *testing.T) {
	orig := storages["test-bulk-delete"]
	storages["test-bulk-delete"] = func(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
		return &bulkDeleteStub{ObjectStorage: mustNewMemStoreWithName(t, endpoint)}, nil
	}
	t.Cleanup(func() {
		if orig == nil {
			delete(storages, "test-bulk-delete")
			return
		}
		storages["test-bulk-delete"] = orig
	})

	sharded, err := NewSharded("test-bulk-delete", "%d", "", "", "", 2)
	require.NoError(t, err)
	require.True(t, SupportsBulkDelete(sharded))

	memSharded, err := NewSharded("mem", "%d", "", "", "", 2)
	require.NoError(t, err)
	require.False(t, SupportsBulkDelete(memSharded))
}

func TestShardedDeleteObjectsRunsIndependentGroupsInParallel(t *testing.T) {
	release := make(chan struct{})
	started := make(chan string, 2)
	storeA := &blockingBulkDeleteStore{
		ObjectStorage: mustNewMemStore(t),
		name:          "store-a",
		started:       started,
		release:       release,
	}
	storeB := &blockingBulkDeleteStore{
		ObjectStorage: mustNewMemStore(t),
		name:          "store-b",
		started:       started,
		release:       release,
	}
	s := &sharded{stores: []ObjectStorage{storeA, storeB}}

	keyA := keyForShard(t, s, storeA)
	keyB := keyForShard(t, s, storeB)

	done := make(chan error, 1)
	go func() {
		done <- s.DeleteObjects(context.Background(), []string{keyA, keyB})
	}()

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case name := <-started:
			got[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("expected both shard groups to start before release, got %v", got)
		}
	}

	close(release)
	require.NoError(t, <-done)
}

type blockingBulkDeleteStore struct {
	ObjectStorage
	name    string
	started chan<- string
	release <-chan struct{}
}

func (s *blockingBulkDeleteStore) DeleteObjects(ctx context.Context, keys []string, getters ...AttrGetter) error {
	select {
	case s.started <- s.name:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func keyForShard(t *testing.T, s *sharded, store ObjectStorage) string {
	t.Helper()
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if s.pick(key) == store {
			return key
		}
	}
	t.Fatal("failed to find a key for shard")
	return ""
}

func mustNewMemStore(t *testing.T) ObjectStorage {
	t.Helper()
	s, err := newMem("mem", "", "", "")
	require.NoError(t, err)
	return s
}

func mustNewMemStoreWithName(t *testing.T, name string) ObjectStorage {
	t.Helper()
	s, err := newMem(name, "", "", "")
	require.NoError(t, err)
	return s
}
