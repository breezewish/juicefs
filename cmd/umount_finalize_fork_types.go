package cmd

// forkFinalizeAckV1 is the finalize ack schema written by the mount daemon for `juicefs umount-finalize`.
//
// The mount daemon writes a terminal ack to prove whether the correctness-critical finalize has finished.
// Status is "ok" on success, otherwise it's a terminal failure status such as "error" or "panic".
type forkFinalizeAckV1 struct {
	SchemaVersion     int    `json:"schema_version"`
	Pid               int    `json:"pid"`
	PidStarttimeTicks uint64 `json:"pid_starttime_ticks"`
	Status            string `json:"status"`
	Phase             string `json:"phase,omitempty"`
	Error             string `json:"error,omitempty"`
	FinishedAt        string `json:"finished_at,omitempty"`
}

// forkUmountFinalizeResult is the structured JSON output of `juicefs umount-finalize`.
type forkUmountFinalizeResult struct {
	Ok                   bool               `json:"ok"`
	Finalized            bool               `json:"finalized"`
	KernelUmountObserved bool               `json:"kernel_umount_observed"`
	DaemonExitObserved   bool               `json:"daemon_exit_observed"`
	Reason               string             `json:"reason,omitempty"`
	Ack                  *forkFinalizeAckV1 `json:"ack,omitempty"`
}
