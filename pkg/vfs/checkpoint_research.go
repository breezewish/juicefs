//go:build run9_checkpoint_research

package vfs

// Run9CheckpointWriteBarrier waits for admitted data writes and prevents later
// VFS writes from creating dirty buffers until the returned function is called.
// Callers drain existing buffers/uploads while holding this barrier, then fix
// a metadata view. This does not freeze all metadata or replace guest syncfs.
func (v *VFS) Run9CheckpointWriteBarrier() func() {
	v.checkpointWrites.Lock()
	return v.checkpointWrites.Unlock
}
