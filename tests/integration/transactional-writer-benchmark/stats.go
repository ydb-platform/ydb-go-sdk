package transactionalwriterbenchmark

type workerStats struct {
	LogicalTransactions uint64
	Committed           uint64
	Failed              uint64
}

type phaseStats struct {
	workerStats
}

func mergeWorkerStats(all []workerStats) phaseStats {
	var merged phaseStats
	for i := range all {
		stats := &all[i]
		merged.LogicalTransactions += stats.LogicalTransactions
		merged.Committed += stats.Committed
		merged.Failed += stats.Failed
	}

	return merged
}
