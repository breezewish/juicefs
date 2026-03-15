package cmd

import (
	"errors"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

type fakeSessionShutdowner struct {
	closeErr    error
	shutdownErr error
}

func (f *fakeSessionShutdowner) CloseSession() error { return f.closeErr }

func (f *fakeSessionShutdowner) Shutdown() error { return f.shutdownErr }

type shutdownTrackingStorage struct {
	object.ObjectStorage
	shutdownCalled bool
}

func (s *shutdownTrackingStorage) Shutdown() { s.shutdownCalled = true }

func TestShutdownSessionAndResources_JoinsErrors(t *testing.T) {
	closeErr := errors.New("close session failed")
	shutdownErr := errors.New("shutdown failed")
	m := &fakeSessionShutdowner{
		closeErr:    closeErr,
		shutdownErr: shutdownErr,
	}

	err := shutdownSessionAndResources(m, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, err, shutdownErr)
}

func TestShutdownSessionAndResources_ShutsDownObjectStorage(t *testing.T) {
	blob, err := object.CreateStorage("mem", "test", "", "", "")
	require.NoError(t, err)
	wrapped := &shutdownTrackingStorage{ObjectStorage: blob}

	m := &fakeSessionShutdowner{}
	err = shutdownSessionAndResources(m, wrapped)
	require.NoError(t, err)
	require.True(t, wrapped.shutdownCalled)
}
