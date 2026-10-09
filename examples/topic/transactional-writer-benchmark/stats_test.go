package main

import (
	"testing"
	"time"
)

func TestSummarizeLatencies(t *testing.T) {
	t.Parallel()

	summary := summarizeLatencies([]time.Duration{
		4 * time.Millisecond,
		time.Millisecond,
		3 * time.Millisecond,
		2 * time.Millisecond,
	})

	if summary.Count != 4 {
		t.Fatalf("Count = %d, want 4", summary.Count)
	}
	if summary.MinMS != 1 {
		t.Fatalf("MinMS = %f, want 1", summary.MinMS)
	}
	if summary.MeanMS != 2.5 {
		t.Fatalf("MeanMS = %f, want 2.5", summary.MeanMS)
	}
	if summary.P50MS != 2 {
		t.Fatalf("P50MS = %f, want 2", summary.P50MS)
	}
	if summary.P95MS != 4 {
		t.Fatalf("P95MS = %f, want 4", summary.P95MS)
	}
	if summary.P99MS != 4 {
		t.Fatalf("P99MS = %f, want 4", summary.P99MS)
	}
	if summary.MaxMS != 4 {
		t.Fatalf("MaxMS = %f, want 4", summary.MaxMS)
	}
}

func TestPhaseReportRates(t *testing.T) {
	t.Parallel()

	report := phaseStats{
		workerStats: workerStats{
			Committed: 20,
			Messages:  40,
			Bytes:     2 * 1024 * 1024,
		},
		Duration: 2 * time.Second,
	}.report(true)

	if report.TransactionsPerSecond != 10 {
		t.Fatalf("TransactionsPerSecond = %f, want 10", report.TransactionsPerSecond)
	}
	if report.MessagesPerSecond != 20 {
		t.Fatalf("MessagesPerSecond = %f, want 20", report.MessagesPerSecond)
	}
	if report.PayloadMiBPerSecond != 1 {
		t.Fatalf("PayloadMiBPerSecond = %f, want 1", report.PayloadMiBPerSecond)
	}
	if report.Latency.TableExec != nil {
		t.Fatal("TableExec is non-nil for skipped table write")
	}
}
