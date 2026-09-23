//nolint:tagliatelle // Benchmark reports intentionally use analysis-friendly snake_case JSON.
package main

import (
	"math"
	"slices"
	"time"
)

type workerStats struct {
	LogicalTransactions uint64
	Committed           uint64
	Failed              uint64
	Cancelled           uint64
	Attempts            uint64
	Retries             uint64
	Messages            uint64
	Bytes               uint64
	FirstError          string
	TransactionLatency  []time.Duration
	TableLatency        []time.Duration
	WriterStartLatency  []time.Duration
	WriterWriteLatency  []time.Duration
}

type phaseStats struct {
	workerStats

	Duration time.Duration
	Aborted  bool
}

type latencySummary struct {
	Count  int     `json:"count"`
	MinMS  float64 `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

type latencyReport struct {
	Transaction latencySummary  `json:"transaction"`
	TableExec   *latencySummary `json:"table_exec,omitempty"`
	WriterStart latencySummary  `json:"writer_start"`
	WriterWrite latencySummary  `json:"writer_write"`
}

type phaseReport struct {
	DurationSeconds       float64       `json:"duration_seconds"`
	LogicalTransactions   uint64        `json:"logical_transactions"`
	CommittedTransactions uint64        `json:"committed_transactions"`
	FailedTransactions    uint64        `json:"failed_transactions"`
	CancelledTransactions uint64        `json:"cancelled_transactions"`
	TransactionAttempts   uint64        `json:"transaction_attempts"`
	Retries               uint64        `json:"retries"`
	CommittedMessages     uint64        `json:"committed_messages"`
	CommittedPayloadBytes uint64        `json:"committed_payload_bytes"`
	TransactionsPerSecond float64       `json:"transactions_per_second"`
	MessagesPerSecond     float64       `json:"messages_per_second"`
	PayloadMiBPerSecond   float64       `json:"payload_mib_per_second"`
	AbortedByErrorLimit   bool          `json:"aborted_by_error_limit"`
	FirstError            string        `json:"first_error,omitempty"`
	Latency               latencyReport `json:"latency_ms"`
}

func mergeWorkerStats(all []workerStats, duration time.Duration, aborted bool) phaseStats {
	merged := phaseStats{
		Duration: duration,
		Aborted:  aborted,
	}

	for i := range all {
		stats := &all[i]
		merged.LogicalTransactions += stats.LogicalTransactions
		merged.Committed += stats.Committed
		merged.Failed += stats.Failed
		merged.Cancelled += stats.Cancelled
		merged.Attempts += stats.Attempts
		merged.Retries += stats.Retries
		merged.Messages += stats.Messages
		merged.Bytes += stats.Bytes
		if merged.FirstError == "" {
			merged.FirstError = stats.FirstError
		}
		merged.TransactionLatency = append(merged.TransactionLatency, stats.TransactionLatency...)
		merged.TableLatency = append(merged.TableLatency, stats.TableLatency...)
		merged.WriterStartLatency = append(merged.WriterStartLatency, stats.WriterStartLatency...)
		merged.WriterWriteLatency = append(merged.WriterWriteLatency, stats.WriterWriteLatency...)
	}

	return merged
}

func (s phaseStats) report(skipTableWrite bool) phaseReport {
	seconds := s.Duration.Seconds()
	report := phaseReport{
		DurationSeconds:       seconds,
		LogicalTransactions:   s.LogicalTransactions,
		CommittedTransactions: s.Committed,
		FailedTransactions:    s.Failed,
		CancelledTransactions: s.Cancelled,
		TransactionAttempts:   s.Attempts,
		Retries:               s.Retries,
		CommittedMessages:     s.Messages,
		CommittedPayloadBytes: s.Bytes,
		AbortedByErrorLimit:   s.Aborted,
		FirstError:            s.FirstError,
		Latency: latencyReport{
			Transaction: summarizeLatencies(s.TransactionLatency),
			WriterStart: summarizeLatencies(s.WriterStartLatency),
			WriterWrite: summarizeLatencies(s.WriterWriteLatency),
		},
	}
	if !skipTableWrite {
		table := summarizeLatencies(s.TableLatency)
		report.Latency.TableExec = &table
	}
	if seconds > 0 {
		report.TransactionsPerSecond = float64(s.Committed) / seconds
		report.MessagesPerSecond = float64(s.Messages) / seconds
		report.PayloadMiBPerSecond = float64(s.Bytes) / (1024 * 1024) / seconds
	}

	return report
}

func summarizeLatencies(samples []time.Duration) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}

	slices.Sort(samples)

	var total time.Duration
	for _, sample := range samples {
		total += sample
	}

	return latencySummary{
		Count:  len(samples),
		MinMS:  durationMilliseconds(samples[0]),
		MeanMS: durationMilliseconds(total) / float64(len(samples)),
		P50MS:  durationMilliseconds(nearestRank(samples, 0.50)),
		P95MS:  durationMilliseconds(nearestRank(samples, 0.95)),
		P99MS:  durationMilliseconds(nearestRank(samples, 0.99)),
		MaxMS:  durationMilliseconds(samples[len(samples)-1]),
	}
}

func nearestRank(sortedSamples []time.Duration, percentile float64) time.Duration {
	index := max(0, int(math.Ceil(percentile*float64(len(sortedSamples))))-1)
	if index >= len(sortedSamples) {
		index = len(sortedSamples) - 1
	}

	return sortedSamples[index]
}

func durationMilliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}
