package metrics

type DatabaseMetrics struct {
	conns             GaugeVec
	inflight          GaugeVec
	query             CounterVec
	queryLatency      TimerVec
	exec              CounterVec
	execLatency       TimerVec
	txBegin           CounterVec
	txBeginLatency    TimerVec
	txExec            CounterVec
	txExecLatency     TimerVec
	txQuery           CounterVec
	txQueryLatency    TimerVec
	txCommit          CounterVec
	txCommitLatency   TimerVec
	txRollback        CounterVec
	txRollbackLatency TimerVec
}
