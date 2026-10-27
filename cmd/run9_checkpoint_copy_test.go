//go:build linux

package cmd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestRun9GCCheckpointOwnsMappedFiles(t *testing.T) {
	if mode := os.Getenv("RUN9_TEST_CHECKPOINT_COPY_MODE"); mode != "" {
		original := os.Getenv("RUN9_TEST_CHECKPOINT_COPY_SOURCE")
		writeRun9TestMetadata(t, original, "snap", t.TempDir(), 11, 12)
		directory := original
		if mode == "private" {
			var err error
			directory, err = copyRun9GCCheckpoint(context.Background(), original)
			require.NoError(t, err)
			defer os.RemoveAll(directory)
		}
		reader, err := meta.OpenRun9Checkpoint(directory)
		require.NoError(t, err)
		defer reader.Shutdown()
		tables, err := filepath.Glob(filepath.Join(original, "*.sst"))
		require.NoError(t, err)
		require.NotEmpty(t, tables)
		// Deterministically invalidate source pages after mmap, without affecting
		// the reader-owned copy. This models the fatal page fault, not a claim
		// about which remote filesystem operation triggered a historical fault.
		for _, table := range tables {
			require.NoError(t, os.Truncate(table, 0))
		}
		require.NoError(t, os.RemoveAll(original))
		ids, err := reader.Run9CheckpointSlices(context.Background())
		require.NoError(t, err)
		require.Equal(t, []uint64{11, 12}, ids)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	before := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRun9GCCheckpointOwnsMappedFiles$")
	before.Env = append(os.Environ(), "RUN9_TEST_CHECKPOINT_COPY_MODE=shared", "RUN9_TEST_CHECKPOINT_COPY_SOURCE="+t.TempDir())
	output, err := before.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "SIGBUS", "original shared mmap must reproduce the fatal fault")

	after := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRun9GCCheckpointOwnsMappedFiles$")
	after.Env = append(os.Environ(), "RUN9_TEST_CHECKPOINT_COPY_MODE=private", "RUN9_TEST_CHECKPOINT_COPY_SOURCE="+t.TempDir())
	output, err = after.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "PASS")
	t.Log("shared mmap reproduces SIGBUS; private copy retains exact live slices after source removal")
}

func TestRun9GCCheckpointCopyRejectsInvalidEvidenceAndCancellation(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "MANIFEST"), []byte("manifest"), 0600))
	require.NoError(t, os.Symlink("MANIFEST", filepath.Join(source, "linked")))
	directory, err := copyRun9GCCheckpoint(context.Background(), source)
	require.ErrorContains(t, err, "not a regular file")
	require.NoDirExists(t, directory)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = copyRun9GCCheckpoint(ctx, source)
	require.ErrorIs(t, err, context.Canceled)
	entries, err := os.ReadDir(tmp)
	require.NoError(t, err)
	require.Empty(t, entries, "failed reads must not leave private metadata copies")
}
