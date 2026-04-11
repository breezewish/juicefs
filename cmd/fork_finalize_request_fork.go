//go:build !windows
// +build !windows

package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const forkFinalizeRequestMaxBytes = 16 * 1024

type forkFinalizeRequestV1 struct {
	SchemaVersion   int    `json:"schema_version"`
	FinalizeTimeout string `json:"finalize_timeout"`
}

func (r *forkFinalizeRequestV1) validate() error {
	if r == nil {
		return fmt.Errorf("nil request")
	}
	if r.SchemaVersion != 1 {
		return fmt.Errorf("unexpected request schema_version %d", r.SchemaVersion)
	}
	finalizeTimeout := strings.TrimSpace(r.FinalizeTimeout)
	if finalizeTimeout == "" {
		return fmt.Errorf("finalize_timeout is required")
	}
	parsed, err := time.ParseDuration(finalizeTimeout)
	if err != nil {
		return fmt.Errorf("parse finalize_timeout: %w", err)
	}
	if parsed <= 0 {
		return fmt.Errorf("finalize_timeout must be positive (got %s)", r.FinalizeTimeout)
	}
	return nil
}

func ensureForkFinalizeAckDirForRequester(dir string, expectedUID uint32) error {
	fi, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0o700); err != nil && !os.IsExist(err) {
			return err
		}
		fi, err = os.Lstat(dir)
	}
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("finalize ack dir is a symlink: %s", dir)
	}
	if !fi.IsDir() {
		return fmt.Errorf("finalize ack dir is not a directory: %s", dir)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("unexpected stat type %T for %s", fi.Sys(), dir)
	}
	if st.Uid != expectedUID {
		if err := os.Chown(dir, int(expectedUID), -1); err != nil {
			return fmt.Errorf("chown finalize ack dir uid to %d: %w", expectedUID, err)
		}
		fi, err = os.Lstat(dir)
		if err != nil {
			return err
		}
		st, ok = fi.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("unexpected stat type %T for %s", fi.Sys(), dir)
		}
		if st.Uid != expectedUID {
			return fmt.Errorf("finalize ack dir owner uid mismatch (got %d, want %d): %s", st.Uid, expectedUID, dir)
		}
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	return nil
}

func readForkFinalizeRequest(path string) (*forkFinalizeRequestV1, error) {
	fi, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read finalize request: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("finalize request path is a symlink: %s", path)
	}
	if fi.IsDir() {
		return nil, fmt.Errorf("finalize request path is a directory: %s", path)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("finalize request path is not a regular file: %s", path)
	}
	if fi.Size() > int64(forkFinalizeRequestMaxBytes) {
		return nil, fmt.Errorf("finalize request too large: %d bytes", fi.Size())
	}

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("read finalize request: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(f, forkFinalizeRequestMaxBytes+1))
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("read finalize request: %w", err)
	}
	if len(data) > forkFinalizeRequestMaxBytes {
		return nil, fmt.Errorf("finalize request too large: %d bytes", len(data))
	}

	var req forkFinalizeRequestV1
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("parse finalize request: %w", err)
	}
	if err := req.validate(); err != nil {
		return nil, err
	}
	return &req, nil
}

func writeForkFinalizeRequest(path string, req *forkFinalizeRequestV1) error {
	if err := req.validate(); err != nil {
		return err
	}

	reqDir := filepath.Dir(path)
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("finalize request path is a symlink: %s", path)
		}
		if fi.IsDir() {
			return fmt.Errorf("finalize request path is a directory: %s", path)
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("finalize request path is not a regular file: %s", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(reqDir, "tmp-finalize-req-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
