package vfs

import (
	"compress/gzip"
	"io"
	"os"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

type pausedBackupMeta struct {
	meta.Meta
	started chan struct{}
	resume  chan struct{}
}

func (m pausedBackupMeta) Name() string { return "badger" }

func (m pausedBackupMeta) DumpMeta(w io.Writer, _ meta.Ino, _ int, _, _, _ bool) error {
	close(m.started)
	<-m.resume
	_, err := io.WriteString(w, `{"marker":"first"}`)
	return err
}

type immediateBackupMeta struct{ meta.Meta }

func (immediateBackupMeta) Name() string { return "badger" }

func (immediateBackupMeta) DumpMeta(w io.Writer, _ meta.Ino, _ int, _, _, _ bool) error {
	_, err := io.WriteString(w, `{"marker":"second"}`)
	return err
}

func TestConcurrentBackupsKeepTheirOwnTemporaryFiles(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	first, err := object.CreateStorage("file", t.TempDir()+"/", "", "", "")
	require.NoError(t, err)
	second, err := object.CreateStorage("file", t.TempDir()+"/", "", "", "")
	require.NoError(t, err)
	now := time.Date(2026, 11, 3, 8, 11, 4, 0, time.UTC)
	started, resume := make(chan struct{}), make(chan struct{})
	defer close(resume)
	done := make(chan error, 1)
	go func() {
		_, err := backup(pausedBackupMeta{started: started, resume: resume}, first, now, true, false)
		done <- err
	}()
	<-started
	// The second backup finishes and removes its temporary file while the
	// first still owns an open dump with the same timestamp and remote key.
	_, err = backup(immediateBackupMeta{}, second, now, true, false)
	require.NoError(t, err)
	resume <- struct{}{}
	require.NoError(t, <-done)

	firstDump, err := first.Get(t.Context(), "meta/dump-2026-11-03-081104.json.gz", 0, -1)
	require.NoError(t, err)
	defer firstDump.Close()
	firstGzip, err := gzip.NewReader(firstDump)
	require.NoError(t, err)
	defer firstGzip.Close()
	firstBody, err := io.ReadAll(firstGzip)
	require.NoError(t, err)
	require.JSONEq(t, `{"marker":"first"}`, string(firstBody))

	secondDump, err := second.Get(t.Context(), "meta/dump-2026-11-03-081104.json.gz", 0, -1)
	require.NoError(t, err)
	defer secondDump.Close()
	secondGzip, err := gzip.NewReader(secondDump)
	require.NoError(t, err)
	defer secondGzip.Close()
	secondBody, err := io.ReadAll(secondGzip)
	require.NoError(t, err)
	require.JSONEq(t, `{"marker":"second"}`, string(secondBody))
	remaining, err := os.ReadDir(os.TempDir())
	require.NoError(t, err)
	require.Empty(t, remaining)
}
