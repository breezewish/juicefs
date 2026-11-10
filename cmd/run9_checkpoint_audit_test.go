package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun9CheckpointAuditRecoversCompleteBoundedProof(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "MANIFEST"), []byte("immutable metadata"), 0600))
	retained := make(map[uint64]bool)
	want := make([]uint64, 20000)
	for i := range want {
		want[i] = 12982801373813573 + uint64(i)*129
		retained[want[i]] = true
	}
	req := run9GCCheckpointRequest{Directory: "/snaps/ab/snap/checkpoints/g9/meta", Format: "snap", AllocationEnd: want[len(want)-1] + 1}
	ranges := []run9GCSliceRange{{Start: want[0], EndInclusive: req.AllocationEnd - 1}}
	var output bytes.Buffer
	require.NoError(t, encodeRun9CheckpointAudit(&output, req, directory, retained, ranges))
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	var header struct {
		Event     string            `json:"event"`
		Directory string            `json:"checkpoint_directory"`
		Count     int               `json:"retained_count"`
		Parts     int               `json:"proof_parts"`
		Hash      string            `json:"proof_sha256"`
		Metadata  map[string]string `json:"metadata_sha256"`
		AuditID   string            `json:"audit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	require.Equal(t, "checkpoint_gc_authorized", header.Event)
	require.Equal(t, req.Directory, header.Directory)
	require.Equal(t, len(want), header.Count)
	require.Greater(t, header.Parts, 1)
	require.Len(t, lines, header.Parts+1)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte("immutable metadata"))), header.Metadata["MANIFEST"])
	var proof []byte
	for i, line := range lines[1:] {
		require.Less(t, len(line), 24*1024)
		var part struct {
			Index   int    `json:"proof_part"`
			Data    string `json:"proof_data"`
			AuditID string `json:"audit_id"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &part))
		require.Equal(t, i, part.Index)
		require.Equal(t, header.AuditID, part.AuditID)
		raw, err := base64.StdEncoding.DecodeString(part.Data)
		require.NoError(t, err)
		proof = append(proof, raw...)
	}
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(proof)), header.Hash)
	ids, err := decodeRun9ForkCheckpointSlices(proof)
	require.NoError(t, err)
	require.Equal(t, want, ids)
}
