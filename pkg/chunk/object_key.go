package chunk

import (
	"fmt"
	"strconv"
	"strings"
)

// ObjectBlockKey is the parsed identity and size encoded in one chunk object key.
type ObjectBlockKey struct {
	SliceID    uint64
	BlockIndex uint64
	BlockSize  uint64
}

// FormatObjectBlockKey returns the object key used for one JuiceFS slice block.
func FormatObjectBlockKey(sliceID uint64, blockIndex uint64, blockSize uint64, hashPrefix bool) string {
	if hashPrefix {
		return fmt.Sprintf("chunks/%02X/%d/%d_%d_%d", sliceID%256, sliceID/1000/1000, sliceID, blockIndex, blockSize)
	}
	return fmt.Sprintf("chunks/%d/%d/%d_%d_%d", sliceID/1000/1000, sliceID/1000, sliceID, blockIndex, blockSize)
}

// ParseObjectBlockKey parses and validates the path layout of one JuiceFS slice block object key.
func ParseObjectBlockKey(key string, hashPrefix bool) (ObjectBlockKey, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "chunks" {
		return ObjectBlockKey{}, false
	}

	block, ok := parseObjectBlockName(parts[3])
	if !ok {
		return ObjectBlockKey{}, false
	}
	if hashPrefix {
		if parts[1] != fmt.Sprintf("%02X", block.SliceID%256) {
			return ObjectBlockKey{}, false
		}
		if parts[2] != strconv.FormatUint(block.SliceID/1000/1000, 10) {
			return ObjectBlockKey{}, false
		}
		return block, true
	}

	if parts[1] != strconv.FormatUint(block.SliceID/1000/1000, 10) {
		return ObjectBlockKey{}, false
	}
	if parts[2] != strconv.FormatUint(block.SliceID/1000, 10) {
		return ObjectBlockKey{}, false
	}
	return block, true
}

func parseObjectBlockName(name string) (ObjectBlockKey, bool) {
	parts := strings.Split(name, "_")
	if len(parts) != 3 {
		return ObjectBlockKey{}, false
	}
	sliceID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return ObjectBlockKey{}, false
	}
	blockIndex, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return ObjectBlockKey{}, false
	}
	blockSize, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return ObjectBlockKey{}, false
	}
	return ObjectBlockKey{SliceID: sliceID, BlockIndex: blockIndex, BlockSize: blockSize}, true
}
