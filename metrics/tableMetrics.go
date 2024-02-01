package metrics

type TableMetrics struct {
	alive           GaugeVec
	limit           GaugeVec
	size            GaugeVec
	inflight        GaugeVec
	inflightLatency TimerVec
	wait            GaugeVec
	waitLatency     TimerVec
}
