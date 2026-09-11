//go:build !run9_checkpoint_research

package chunk

type researchUploadTracker struct{}

func (*researchUploadTracker) start(string, int) {}
func (*researchUploadTracker) complete(string)   {}
func (*researchUploadTracker) abandon(string)    {}
