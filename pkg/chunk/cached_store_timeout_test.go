package chunk

import (
	"bytes"
	"context"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

type lateReadStorage struct {
	object.ObjectStorage
	released <-chan struct{}
	finished chan<- struct{}
}

func (s *lateReadStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	<-ctx.Done()
	<-s.released
	attrs := object.ApplyGetters(getters...)
	attrs.SetRequestID("late-request").SetStorageClass("late-class")
	return &lateReadBody{Reader: bytes.NewReader([]byte("late")), finished: s.finished}, nil
}

type lateReadBody struct {
	*bytes.Reader
	finished chan<- struct{}
}

func (r *lateReadBody) Close() error { close(r.finished); return nil }

func TestObjectReadTimeoutKeepsLateResultsPrivate(t *testing.T) {
	for _, ranged := range []bool{false, true} {
		name := "full"
		if ranged {
			name = "range"
		}
		t.Run(name, func(t *testing.T) {
			released, finished := make(chan struct{}), make(chan struct{})
			mem, err := object.CreateStorage("mem", "", "", "", "")
			require.NoError(t, err)
			storage := &lateReadStorage{ObjectStorage: mem, released: released, finished: finished}
			conf := defaultConf
			conf.CacheDir = "memory"
			conf.GetTimeout = time.Millisecond
			store := NewCachedStore(storage, conf, nil).(*cachedStore)
			page := NewPage(make([]byte, 4))
			defer page.Release()
			if ranged {
				_, err = store.loadRange(context.Background(), "late", page, 0)
				require.ErrorIs(t, err, errTryFullRead)
			} else {
				err = store.load(context.Background(), "late", page, false, false)
				require.Error(t, err)
				require.Contains(t, err.Error(), utils.ErrFuncTimeout.Error())
			}
			copy(page.Data, []byte("next"))
			close(released)
			for i := 0; i < 1000; i++ {
				require.NotContains(t, err.Error(), "late-request")
			}
			<-finished
			require.Equal(t, []byte("next"), page.Data)
		})
	}
}
