package cmd

import (
	"errors"

	"github.com/juicedata/juicefs/pkg/object"
)

type sessionShutdowner interface {
	CloseSession() error
	Shutdown() error
}

func shutdownSessionAndResources(metaCli sessionShutdowner, blob object.ObjectStorage) error {
	// Fork divergence: ensure meta is shut down on mount exit.
	// This is critical for badger with SkipWAL enabled (memtables must be flushed).
	closeErr := metaCli.CloseSession()
	// Shut down meta before object storage to flush badger memtables as early as possible.
	shutdownErr := metaCli.Shutdown()
	object.Shutdown(blob)
	return errors.Join(closeErr, shutdownErr)
}
