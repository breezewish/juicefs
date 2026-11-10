package cmd

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/version"
)

// writeRun9CheckpointAudit preserves the exact deletion authority before the
// first object DELETE. New generations prune old checkpoints, so a later 404
// cannot otherwise be compared with the slice set that authorized collection.
// This is diagnostic evidence, not a new source of deletion authorization.
func writeRun9CheckpointAudit(req run9GCCheckpointRequest, directory string, retained map[uint64]bool, ranges []run9GCSliceRange) (resultErr error) {
	if len(ranges) == 0 {
		return nil
	}
	var writer io.Writer = os.Stderr
	if logDir := os.Getenv("RUN9_LOG_DIR"); logDir != "" {
		// The existing runtime CloudWatch collector owns this JSON-lines file.
		// Write directly: runtime wrappers capture helper stderr and discard it
		// on success, and a killed helper never returns its result to Portal.
		f, err := os.OpenFile(filepath.Join(logDir, "run9rt.json.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open checkpoint audit log: %w", err)
		}
		defer func() {
			if err := f.Close(); resultErr == nil {
				resultErr = err
			}
		}()
		writer = f
	}
	return encodeRun9CheckpointAudit(writer, req, directory, retained, ranges)
}

// Parts are bounded well below a CloudWatch event. The binary payload uses the
// existing checksummed, delta-varint slice-list format; concatenating decoded
// parts recovers every retained ID without logging user files or credentials.
func encodeRun9CheckpointAudit(writer io.Writer, req run9GCCheckpointRequest, directory string, retained map[uint64]bool, ranges []run9GCSliceRange) error {
	ids := make([]uint64, 0, len(retained))
	for id := range retained {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	proof := encodeRun9ForkCheckpointSlices(ids)
	files := make(map[string]string)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		f, err := os.Open(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, f)
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		files[entry.Name()] = fmt.Sprintf("%x", hash.Sum(nil))
	}
	const partBytes = 12 * 1024
	parts := (len(proof) + partBytes - 1) / partBytes
	fields := map[string]any{
		"ts": time.Now().UTC().Format(time.RFC3339Nano), "level": "INFO",
		"service": "juicefs", "event": "checkpoint_gc_authorized",
		"juicefs_version":   version.Version(),
		"deployment_prefix": os.Getenv("RUN9_DEPLOYMENT_PREFIX"), "env_name": os.Getenv("RUN9_ENV_NAME"),
		"region": os.Getenv("RUN9_AWS_REGION"), "host_instance_id": os.Getenv("RUN9_INSTANCE_ID"),
		"audit_id": uuid.NewString(), "checkpoint_directory": req.Directory, "format": req.Format,
		"allocation_end": req.AllocationEnd, "deleted": req.Deleted, "dry_run": req.DryRun,
		"after": req.After, "ranges": ranges, "retained_count": len(ids),
		"proof_encoding": "base64/run9-fork-checkpoint-slices-v1", "proof_sha256": fmt.Sprintf("%x", sha256.Sum256(proof)),
		"proof_parts": parts, "metadata_sha256": files,
	}
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(fields); err != nil {
		return fmt.Errorf("write checkpoint audit header: %w", err)
	}
	delete(fields, "metadata_sha256")
	delete(fields, "ranges")
	fields["event"] = "checkpoint_gc_retained_slices"
	for part := 0; part < parts; part++ {
		fields["proof_part"] = part
		fields["proof_data"] = base64.StdEncoding.EncodeToString(proof[part*partBytes : min((part+1)*partBytes, len(proof))])
		if err := encoder.Encode(fields); err != nil {
			return fmt.Errorf("write checkpoint audit part %d: %w", part, err)
		}
	}
	return nil
}
