package gtrace

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicOnReaderMetricHooksForwardFields(t *testing.T) {
	ctx := context.Background()
	sessionError := errors.New("session error")

	// Generated forwarding helpers must remain safe when the corresponding
	// optional callback is not configured.
	require.NotPanics(t, func() {
		TopicOnReaderCommitQueued(&trace.Topic{}, &ctx, "endpoint", "database", "topic", "consumer", "reader", 7, 8, 9)
		TopicOnReaderCommitAcknowledged(&trace.Topic{}, &ctx, "endpoint", "database", "topic", "consumer", "reader", 7, 8, 9)
		TopicOnReaderSessionError(
			&trace.Topic{}, &ctx, "endpoint", "database", "consumer", "reader",
			"retry", "UNAVAILABLE", "transport_error", sessionError,
		)
		TopicOnReaderLocalBufferChanged(&trace.Topic{}, &ctx, "endpoint", "database", "topic", "consumer", "reader", -3)
		TopicOnReaderReceivedBytes(&trace.Topic{}, &ctx, "endpoint", "database", "consumer", "reader", 123)
		TopicOnReaderCreditBalanceChanged(&trace.Topic{}, &ctx, "endpoint", "database", "consumer", "reader", -456)
	})

	var queued trace.TopicReaderCommitQueuedInfo
	var acknowledged trace.TopicReaderCommitAcknowledgedInfo
	var actualSessionError trace.TopicReaderSessionErrorInfo
	var localBuffer trace.TopicReaderLocalBufferChangedInfo
	var receivedBytes trace.TopicReaderReceivedBytesInfo
	var creditBalance trace.TopicReaderCreditBalanceChangedInfo
	tracer := &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued = info
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			acknowledged = info
		},
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			actualSessionError = info
		},
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			localBuffer = info
		},
		OnReaderReceivedBytes: func(info trace.TopicReaderReceivedBytesInfo) {
			receivedBytes = info
		},
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			creditBalance = info
		},
	}

	TopicOnReaderCommitQueued(tracer, &ctx, "endpoint", "database", "topic", "consumer", "reader", 7, 8, 9)
	TopicOnReaderCommitAcknowledged(tracer, &ctx, "endpoint", "database", "topic", "consumer", "reader", 7, 8, 9)
	TopicOnReaderSessionError(
		tracer, &ctx, "endpoint", "database", "consumer", "reader",
		"retry", "UNAVAILABLE", "transport_error", sessionError,
	)
	TopicOnReaderLocalBufferChanged(tracer, &ctx, "endpoint", "database", "topic", "consumer", "reader", -3)
	TopicOnReaderReceivedBytes(tracer, &ctx, "endpoint", "database", "consumer", "reader", 123)
	TopicOnReaderCreditBalanceChanged(tracer, &ctx, "endpoint", "database", "consumer", "reader", -456)

	require.Equal(t, trace.TopicReaderCommitQueuedInfo{
		Context:            &ctx,
		Endpoint:           "endpoint",
		Database:           "database",
		Topic:              "topic",
		Consumer:           "consumer",
		ReaderName:         "reader",
		PartitionID:        7,
		PartitionSessionID: 8,
		MessagesCount:      9,
	}, queued)
	require.Equal(t, trace.TopicReaderCommitAcknowledgedInfo{
		Context:            &ctx,
		Endpoint:           "endpoint",
		Database:           "database",
		Topic:              "topic",
		Consumer:           "consumer",
		ReaderName:         "reader",
		PartitionID:        7,
		PartitionSessionID: 8,
		MessagesCount:      9,
	}, acknowledged)
	require.Equal(t, trace.TopicReaderSessionErrorInfo{
		Context:       &ctx,
		Endpoint:      "endpoint",
		Database:      "database",
		Consumer:      "consumer",
		ReaderName:    "reader",
		RetryDecision: "retry",
		StatusCode:    "UNAVAILABLE",
		ErrorType:     "transport_error",
		Error:         sessionError,
	}, actualSessionError)
	require.Equal(t, trace.TopicReaderLocalBufferChangedInfo{
		Context:       &ctx,
		Endpoint:      "endpoint",
		Database:      "database",
		Topic:         "topic",
		Consumer:      "consumer",
		ReaderName:    "reader",
		MessagesDelta: -3,
	}, localBuffer)
	require.Equal(t, trace.TopicReaderReceivedBytesInfo{
		Context:    &ctx,
		Endpoint:   "endpoint",
		Database:   "database",
		Consumer:   "consumer",
		ReaderName: "reader",
		Bytes:      123,
	}, receivedBytes)
	require.Equal(t, trace.TopicReaderCreditBalanceChangedInfo{
		Context:    &ctx,
		Endpoint:   "endpoint",
		Database:   "database",
		Consumer:   "consumer",
		ReaderName: "reader",
		BytesDelta: -456,
	}, creditBalance)
}

func TestComposeReaderMetricHooksWithNilCallbacks(t *testing.T) {
	composed := Compose(&trace.Topic{}, &trace.Topic{})

	require.NotPanics(t, func() {
		composed.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{})
		composed.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{})
		composed.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{})
		composed.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{})
		composed.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{})
		composed.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{})
	})
}

func TestComposeReaderMetricHooksCallsBothCallbacks(t *testing.T) {
	wantError := errors.New("expected session error")
	wantQueued := trace.TopicReaderCommitQueuedInfo{MessagesCount: 1}
	wantAcknowledged := trace.TopicReaderCommitAcknowledgedInfo{MessagesCount: 2}
	wantSessionError := trace.TopicReaderSessionErrorInfo{Error: wantError}
	wantLocalBuffer := trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: -3}
	wantReceivedBytes := trace.TopicReaderReceivedBytesInfo{Bytes: 4}
	wantCreditBalance := trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: -5}

	var calls []string
	lhs := &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			require.Equal(t, wantQueued, info)
			calls = append(calls, "lhs queued")
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			require.Equal(t, wantAcknowledged, info)
			calls = append(calls, "lhs acknowledged")
		},
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			require.Equal(t, wantSessionError, info)
			calls = append(calls, "lhs session error")
		},
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			require.Equal(t, wantLocalBuffer, info)
			calls = append(calls, "lhs local buffer")
		},
		OnReaderReceivedBytes: func(info trace.TopicReaderReceivedBytesInfo) {
			require.Equal(t, wantReceivedBytes, info)
			calls = append(calls, "lhs received bytes")
		},
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			require.Equal(t, wantCreditBalance, info)
			calls = append(calls, "lhs credit balance")
		},
	}
	rhs := &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			require.Equal(t, wantQueued, info)
			calls = append(calls, "rhs queued")
		},
		OnReaderCommitAcknowledged: func(info trace.TopicReaderCommitAcknowledgedInfo) {
			require.Equal(t, wantAcknowledged, info)
			calls = append(calls, "rhs acknowledged")
		},
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			require.Equal(t, wantSessionError, info)
			calls = append(calls, "rhs session error")
		},
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			require.Equal(t, wantLocalBuffer, info)
			calls = append(calls, "rhs local buffer")
		},
		OnReaderReceivedBytes: func(info trace.TopicReaderReceivedBytesInfo) {
			require.Equal(t, wantReceivedBytes, info)
			calls = append(calls, "rhs received bytes")
		},
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			require.Equal(t, wantCreditBalance, info)
			calls = append(calls, "rhs credit balance")
		},
	}

	recovered := make([]any, 0)
	composed := Compose(lhs, rhs, WithTopicPanicCallback(func(value any) {
		recovered = append(recovered, value)
	}))
	composed.OnReaderCommitQueued(wantQueued)
	composed.OnReaderCommitAcknowledged(wantAcknowledged)
	composed.OnReaderSessionError(wantSessionError)
	composed.OnReaderLocalBufferChanged(wantLocalBuffer)
	composed.OnReaderReceivedBytes(wantReceivedBytes)
	composed.OnReaderCreditBalanceChanged(wantCreditBalance)

	require.Empty(t, recovered)
	require.Equal(t, []string{
		"lhs queued", "rhs queued",
		"lhs acknowledged", "rhs acknowledged",
		"lhs session error", "rhs session error",
		"lhs local buffer", "rhs local buffer",
		"lhs received bytes", "rhs received bytes",
		"lhs credit balance", "rhs credit balance",
	}, calls)
}

func TestComposeReaderMetricHooksRecoversPanics(t *testing.T) {
	lhs := &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			panic("commit queued panic")
		},
		OnReaderCommitAcknowledged: func(trace.TopicReaderCommitAcknowledgedInfo) {
			panic("commit acknowledged panic")
		},
		OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
			panic("session error panic")
		},
		OnReaderLocalBufferChanged: func(trace.TopicReaderLocalBufferChangedInfo) {
			panic("local buffer panic")
		},
		OnReaderReceivedBytes: func(trace.TopicReaderReceivedBytesInfo) {
			panic("received bytes panic")
		},
		OnReaderCreditBalanceChanged: func(trace.TopicReaderCreditBalanceChangedInfo) {
			panic("credit balance panic")
		},
	}

	rhsCalls := 0
	rhs := &trace.Topic{
		OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {
			rhsCalls++
		},
		OnReaderCommitAcknowledged: func(trace.TopicReaderCommitAcknowledgedInfo) {
			rhsCalls++
		},
		OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
			rhsCalls++
		},
		OnReaderLocalBufferChanged: func(trace.TopicReaderLocalBufferChangedInfo) {
			rhsCalls++
		},
		OnReaderReceivedBytes: func(trace.TopicReaderReceivedBytesInfo) {
			rhsCalls++
		},
		OnReaderCreditBalanceChanged: func(trace.TopicReaderCreditBalanceChangedInfo) {
			rhsCalls++
		},
	}

	var recovered []any
	composed := Compose(lhs, rhs, WithTopicPanicCallback(func(value any) {
		recovered = append(recovered, value)
	}))
	require.NotPanics(t, func() {
		composed.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{})
		composed.OnReaderCommitAcknowledged(trace.TopicReaderCommitAcknowledgedInfo{})
		composed.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{})
		composed.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{})
		composed.OnReaderReceivedBytes(trace.TopicReaderReceivedBytesInfo{})
		composed.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{})
	})

	require.Equal(t, []any{
		"commit queued panic",
		"commit acknowledged panic",
		"session error panic",
		"local buffer panic",
		"received bytes panic",
		"credit balance panic",
	}, recovered)
	require.Zero(t, rhsCalls)
}
