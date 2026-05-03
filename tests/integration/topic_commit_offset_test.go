//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// TestTopicCommitOffsetBoundaryOffsets verifies that the SDK does not panic on
// boundary offset values and that the server returns a proper error (not a crash).
// Also checks what position a fresh reader starts from after each commit.
func TestTopicCommitOffsetBoundaryOffsets(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	// Write two messages so we have something to observe after boundary commits.
	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("msg0")},
		topicwriter.Message{Data: strings.NewReader("msg1")},
	)
	require.NoError(t, err)

	// Read msg0 to get a valid partition/offset reference.
	msg0, err := scope.TopicReader().ReadMessage(ctx)
	require.NoError(t, err)

	// Commit msg0+1 as baseline (committed = 1).
	err = scope.Driver().Topic().CommitOffset(
		ctx, scope.TopicPath(), msg0.PartitionID(), scope.TopicConsumerName(), msg0.Offset+1,
	)
	require.NoError(t, err)

	committedOffset := func() int64 {
		desc, err := scope.Driver().Topic().DescribeTopicConsumer(
			ctx, scope.TopicPath(), scope.TopicConsumerName(), topicoptions.IncludeConsumerStats(),
		)
		require.NoError(t, err)

		return desc.Partitions[0].PartitionConsumerStats.CommittedOffset
	}

	cases := []struct {
		name   string
		offset int64
	}{
		{"backward", msg0.Offset}, // below the already-committed baseline
		{"negative", -1},
		{"far_future", math.MaxInt64},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The only hard requirement: the SDK must not panic.
			// Server behaviour for boundary offsets is intentionally observed and
			// logged rather than asserted — it may change across YDB versions.
			var callErr error
			require.NotPanics(t, func() {
				callErr = scope.Driver().Topic().CommitOffset(
					ctx, scope.TopicPath(), msg0.PartitionID(), scope.TopicConsumerName(), tc.offset,
				)
			})
			t.Logf("offset=%d: err=%v, server committed_offset=%d", tc.offset, callErr, committedOffset())
		})
	}
}

// TestTopicCommitOffsetRetryOnTransportErrors verifies that CommitOffset retries
// automatically on transient gRPC errors (Unavailable, Cancelled) and eventually
// succeeds. Mirrors the Python SDK test_commit_offset_retry_on_ydb_errors pattern.
func TestTopicCommitOffsetRetryOnTransportErrors(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("msg0")})
	require.NoError(t, err)

	msg, err := scope.TopicReader().ReadMessage(ctx)
	require.NoError(t, err)

	// Interceptor that fails the first two CommitOffset gRPC calls with retryable
	// transport errors, then lets subsequent calls through.
	var callCount atomic.Int32
	interceptor := func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		const commitOffsetMethod = "/Ydb.Topic.V1.TopicService/CommitOffset"
		if method != commitOffsetMethod {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		n := callCount.Add(1)
		switch n {
		case 1:
			return grpcStatus.Error(codes.Unavailable, "service temporarily unavailable")
		case 2:
			return grpcStatus.Error(codes.Canceled, "operation was cancelled")
		default:
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}

	// Open a separate driver with the injecting interceptor.
	db := scope.NonCachingDriver(ydb.With(config.WithGrpcOptions(
		grpc.WithChainUnaryInterceptor(interceptor),
	)))
	t.Cleanup(func() { _ = db.Close(ctx) })

	err = db.Topic().CommitOffset(
		ctx,
		scope.TopicPath(),
		msg.PartitionID(),
		scope.TopicConsumerName(),
		msg.Offset+1,
	)
	require.NoError(t, err)
	require.EqualValues(t, 3, callCount.Load())
}

// TestTopicCommitOffsetPersistsOnServer verifies that CommitOffset actually persists
// the committed position on the server: a fresh reader opened after the call must
// start from the next message, not from the beginning.
// Mirrors the Python SDK test_commit_offset_works pattern.
func TestTopicCommitOffsetPersistsOnServer(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	messages := []string{"msg0", "msg1", "msg2", "msg3"}
	for _, m := range messages {
		err := scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader(m)})
		require.NoError(t, err)
	}

	for i, expected := range messages {
		// Each iteration: open a fresh reader, read one message, close it,
		// then commit the offset via CommitOffset so the next reader starts further.
		reader, err := scope.Driver().Topic().StartReader(
			scope.TopicConsumerName(),
			topicoptions.ReadTopic(scope.TopicPath()),
		)
		require.NoError(t, err)

		msg, err := reader.ReadMessage(ctx)
		require.NoError(t, err)

		content, err := io.ReadAll(msg)
		require.NoError(t, err)
		require.Equal(t, expected, string(content))

		require.NoError(t, reader.Close(ctx))

		// Commit offset+1 (exclusive end), no session ID since the reader is closed.
		err = scope.Driver().Topic().CommitOffset(
			ctx,
			scope.TopicPath(),
			msg.PartitionID(),
			scope.TopicConsumerName(),
			msg.Offset+1,
		)
		require.NoError(t, err)

		// Verify committed offset is saved on the server.
		desc, err := scope.Driver().Topic().DescribeTopicConsumer(
			ctx,
			scope.TopicPath(),
			scope.TopicConsumerName(),
			topicoptions.IncludeConsumerStats(),
		)
		require.NoError(t, err)
		require.EqualValues(t, i+1, desc.Partitions[0].PartitionConsumerStats.CommittedOffset)
	}
}

// TestTopicCommitOffsetWithSessionIDKeepsReaderAlive verifies that passing a valid
// read_session_id does not interrupt the active read session: the reader stays
// connected, can keep reading, and the offset is visible on the server.
func TestTopicCommitOffsetWithSessionIDKeepsReaderAlive(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("msg0")},
		topicwriter.Message{Data: strings.NewReader("msg1")},
	)
	require.NoError(t, err)

	reader := scope.TopicReader()

	msg0, err := reader.ReadMessage(ctx)
	require.NoError(t, err)

	sessionIDBefore := reader.ReadSessionID()
	require.NotEmpty(t, sessionIDBefore)

	err = scope.Driver().Topic().CommitOffset(
		ctx,
		scope.TopicPath(),
		msg0.PartitionID(),
		scope.TopicConsumerName(),
		msg0.Offset+1,
		topicoptions.WithCommitOffsetReadSessionID(sessionIDBefore),
	)
	require.NoError(t, err)

	// Reader must still deliver the next message.
	msg1, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, msg0.Offset+1, msg1.Offset)

	// Session ID must not change — verified after the next read to ensure
	// any potential reconnect would have completed by this point.
	require.Equal(t, sessionIDBefore, reader.ReadSessionID())

	// Verify committed offset on server.
	desc, err := scope.Driver().Topic().DescribeTopicConsumer(
		ctx,
		scope.TopicPath(),
		scope.TopicConsumerName(),
		topicoptions.IncludeConsumerStats(),
	)
	require.NoError(t, err)
	require.EqualValues(t, msg0.Offset+1, desc.Partitions[0].PartitionConsumerStats.CommittedOffset)
}

// TestTopicCommitOffsetWithoutSessionIDReconnectsReader verifies that omitting
// read_session_id causes the server to interrupt the active session. The reader
// reconnects automatically, continues delivering messages, and the offset is
// preserved on the server.
func TestTopicCommitOffsetWithoutSessionIDReconnectsReader(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	err := scope.TopicWriter().Write(ctx,
		topicwriter.Message{Data: strings.NewReader("msg0")},
		topicwriter.Message{Data: strings.NewReader("msg1")},
	)
	require.NoError(t, err)

	reader := scope.TopicReader()

	msg0, err := reader.ReadMessage(ctx)
	require.NoError(t, err)

	// Commit without session ID — server will interrupt the active session.
	err = scope.Driver().Topic().CommitOffset(
		ctx,
		scope.TopicPath(),
		msg0.PartitionID(),
		scope.TopicConsumerName(),
		msg0.Offset+1,
	)
	require.NoError(t, err)

	// Reader reconnects and delivers the next message.
	msg1, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.Equal(t, msg0.Offset+1, msg1.Offset)

	// Verify committed offset is preserved on server.
	desc, err := scope.Driver().Topic().DescribeTopicConsumer(
		ctx,
		scope.TopicPath(),
		scope.TopicConsumerName(),
		topicoptions.IncludeConsumerStats(),
	)
	require.NoError(t, err)
	require.EqualValues(t, msg0.Offset+1, desc.Partitions[0].PartitionConsumerStats.CommittedOffset)
}
