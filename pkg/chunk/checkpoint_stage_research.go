//go:build run9_checkpoint_research

package chunk

func (store *cachedStore) Run9StageAllWrites() {
	store.conf.WritebackThresholdSize = store.conf.BlockSize + 1
}
