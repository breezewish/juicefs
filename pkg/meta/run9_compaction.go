package meta

// run9CompactionSlices masks prefix bytes overwritten by the untouched tail.
// Checkpoint GC retains the complete chunk's effective references, so objects
// visible only in the bounded compaction prefix may already be gone. Replacing
// those bytes with holes is safe: the tail remains after the same atomic prefix
// replacement, and concurrent appends can only cover more bytes. Keep the original
// prefix bounds and retirement list; neither deletion authority nor batch size
// changes. Native online-deletion backends retain upstream behavior.
func run9CompactionSlices(prefix, tail []*slice, pos, size uint32) []Slice {
	masked := append([]*slice(nil), prefix...)
	for _, s := range tail {
		start, end := max(pos, s.pos), min(pos+size, s.pos+s.len)
		if start < end {
			masked = append(masked, &slice{pos: start, len: end - start})
		}
	}
	// Native compaction trims leading/trailing holes. Preserve its exact bounds
	// even when masking makes the remaining interval entirely sparse.
	var out []Slice
	var offset uint32
	for _, s := range buildSlice(masked) {
		start, end := max(offset, pos), min(offset+s.Len, pos+size)
		if start < end {
			part := s
			if part.Id != 0 {
				part.Off += start - offset
			}
			part.Len = end - start
			out = append(out, part)
		}
		offset += s.Len
		if offset >= pos+size {
			break
		}
	}
	return out
}
