package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/juicedata/juicefs/pkg/meta"
)

type run9FormatEmptyOutput struct {
	run9DescribeFormatOutput
	UsedBytes  uint64 `json:"used_bytes"`
	UsedInodes uint64 `json:"used_inodes"`
}

// finishRun9EmptyFormat is the no-mount initialization path. It consumes the
// existing format client and reports success only after checked metadata close.
// No VFS, session, writeback worker or data slice has existed in this lifecycle.
func finishRun9EmptyFormat(m meta.Meta, format *meta.Format, epoch uint64, guest *meta.Run9RootPermissions) (err error) {
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, m.Shutdown())
		}
	}()
	if err := meta.InitRun9EmptyFilesystem(m, epoch, uint32(os.Geteuid()), uint32(os.Getegid()), guest); err != nil {
		return err
	}
	var total, available, usedInodes, availableInodes uint64
	if st := m.StatFS(meta.Background(), meta.RootInode, &total, &available, &usedInodes, &availableInodes); st != 0 {
		return fmt.Errorf("stat empty filesystem: %w", st)
	}
	out := run9FormatEmptyOutput{
		run9DescribeFormatOutput: run9DescribeFormatOutput{
			OK: true, JuiceFSFormatName: format.Name,
			ObjectLayout:  run9ObjectLayout{BlockSizeBytes: format.BlockSize * 1024, HashPrefix: format.HashPrefix},
			ObjectStorage: run9ObjectStorageDescriptorFromFormat(*format),
		},
		UsedBytes: total - available, UsedInodes: usedInodes,
	}
	closed = true
	if err := m.Shutdown(); err != nil {
		return fmt.Errorf("close empty filesystem: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
