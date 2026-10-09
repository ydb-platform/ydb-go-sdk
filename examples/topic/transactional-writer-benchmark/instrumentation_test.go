package main

import "testing"

func TestInstrumentationSnapshotSubtractsWarmup(t *testing.T) {
	t.Parallel()

	before := instrumentationSnapshot{
		DescribeTopicCalls:   2,
		StreamWriteOpens:     3,
		WriteRequests:        4,
		WriteRequestMessages: 5,
	}
	after := instrumentationSnapshot{
		DescribeTopicCalls:   7,
		StreamWriteOpens:     11,
		WriteRequests:        17,
		WriteRequestMessages: 23,
	}

	got := after.subtract(before)
	if got.DescribeTopicCalls != 5 {
		t.Fatalf("DescribeTopicCalls = %d, want 5", got.DescribeTopicCalls)
	}
	if got.StreamWriteOpens != 8 {
		t.Fatalf("StreamWriteOpens = %d, want 8", got.StreamWriteOpens)
	}
	if got.WriteRequests != 13 {
		t.Fatalf("WriteRequests = %d, want 13", got.WriteRequests)
	}
	if got.WriteRequestMessages != 18 {
		t.Fatalf("WriteRequestMessages = %d, want 18", got.WriteRequestMessages)
	}
}
