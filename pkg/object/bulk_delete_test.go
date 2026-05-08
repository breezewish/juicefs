package object

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"

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
