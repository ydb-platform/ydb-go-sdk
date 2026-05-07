//go:build integration
// +build integration

package integration

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// checkStepTimeout is the per-step wait/require deadline used inside the
// scenario helpers. The actions themselves should complete within
// milliseconds; one second is generous enough for CI noise but small enough to
// fail the test fast on regressions.
const checkStepTimeout = time.Second

// TestTopicReaderOnStopPartitionSessionAfterReaderRebalance checks that
// WithReaderOnStopPartitionSession is invoked when the server stops a partition
// session after a rebalance between readers, and that a Commit done from the
// callback reaches the server before the corresponding StopPartitionSessionResponse.
func TestTopicReaderOnStopPartitionSessionAfterReaderRebalance(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	h := newOnStopPartitionSessionScenario(scope)

	h.createTopicWithPartitions(2)

	var readMessage *topicreader.Message

	h.createFirstReader(
		// Sync commit mode so that Commit(ctx, msg) inside the callback waits
		// for the server ack before returning. This guarantees the commit is
		// delivered before the SDK sends StopPartitionSessionResponse without
		// any extra flushing on the SDK side.
		topicoptions.WithReaderCommitMode(topicoptions.CommitModeSync),
		topicoptions.WithReaderOnStopPartitionSession(func(req topicoptions.StopPartitionSessionRequest) {
			h.scope.Require.NotNil(readMessage, "first reader did not read the message before stop partition callback")
			h.scope.Require.Equal(readMessage.PartitionID(), req.PartitionID)
			h.scope.Require.Greater(req.PartitionSessionID, int64(0))
			h.scope.Require.NoError(h.firstReader.commit(ctx, readMessage))
			h.onStopCalledOnce.Do(func() { close(h.onStopCalled) })
		}),
	)
	h.firstReader.waitGotAllPartitions(ctx)

	h.writeMessage(ctx)
	readMessage = h.firstReader.readMessage(ctx)

	h.createSecondReader()

	// Write a message and start a background ReadMessage on the first reader
	// so the SDK drains the queued StopPartitionSessionRequest from the
	// batcher (which lets the user callback run). The read may block forever
	// on the partition that stays with the first reader; we don't care about
	// the result and let the goroutine die when the reader is closed.
	h.writeMessage(ctx)
	h.firstReader.readInBackground(ctx)

	h.requireOnStopPartitionSessionCalled(ctx)
	h.requireReadMessageCommitted(ctx, readMessage)
}

// onStopPartitionSessionScenario drives the rebalance scenario for the
// WithReaderOnStopPartitionSession test step by step.
type onStopPartitionSessionScenario struct {
	scope *scopeT

	topicPath      string
	partitionCount int

	firstReader *scenarioReader

	onStopCalled     chan struct{}
	onStopCalledOnce sync.Once
}

func newOnStopPartitionSessionScenario(scope *scopeT) *onStopPartitionSessionScenario {
	return &onStopPartitionSessionScenario{
		scope:        scope,
		onStopCalled: make(chan struct{}),
	}
}

// createTopicWithPartitions creates a topic with the given partition count.
//
// Two or more partitions are required because YDB does not rebalance an
// actively-read single partition to a newly-connected reader (same approach
// as TestTopicPartitionsBalanced).
func (h *onStopPartitionSessionScenario) createTopicWithPartitions(partitionCount int) {
	h.scope.t.Helper()

	h.partitionCount = partitionCount
	h.topicPath = h.scope.TopicPath(
		topicoptions.CreateWithMinActivePartitions(int64(partitionCount)),
		topicoptions.CreateWithPartitionCountLimit(int64(partitionCount)),
	)
}

// writeMessage writes a single message that the first reader must read and ack
// during the stop partition callback.
func (h *onStopPartitionSessionScenario) writeMessage(ctx context.Context) {
	h.scope.t.Helper()

	h.scope.Require.NoError(h.scope.TopicWriter().Write(
		ctx,
		topicwriter.Message{Data: strings.NewReader("message")},
	))
}

// createFirstReader starts the first reader.
//
// The opts are merged with internal infrastructure options (partition-tracking
// trace) and passed to StartReader.
func (h *onStopPartitionSessionScenario) createFirstReader(opts ...topicoptions.ReaderOption) {
	h.scope.t.Helper()

	r := &scenarioReader{
		t:                h.scope.t,
		partitionCount:   h.partitionCount,
		gotAllPartitions: make(chan struct{}),
	}

	tracer := trace.Topic{
		OnReaderPartitionReadStartResponse: func(
			_ trace.TopicReaderPartitionReadStartResponseStartInfo,
		) func(trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			if r.partitions.Add(1) == int64(r.partitionCount) {
				close(r.gotAllPartitions)
			}
			return nil
		},
	}

	reader, err := h.scope.DriverWithGRPCLogging().Topic().StartReader(
		h.scope.TopicConsumerName(),
		topicoptions.ReadTopic(h.topicPath),
		append(opts, topicoptions.WithReaderTrace(tracer))...,
	)
	h.scope.Require.NoError(err)
	h.scope.t.Cleanup(func() { _ = reader.Close(context.Background()) })
	r.reader = reader
	h.firstReader = r
}

// createSecondReader starts a second reader on the same consumer so the
// server takes one partition away from the first reader.
func (h *onStopPartitionSessionScenario) createSecondReader() {
	h.scope.t.Helper()

	reader, err := h.scope.DriverWithGRPCLogging().Topic().StartReader(
		h.scope.TopicConsumerName(),
		topicoptions.ReadTopic(h.topicPath),
	)
	h.scope.Require.NoError(err)
	h.scope.t.Cleanup(func() { _ = reader.Close(context.Background()) })
}

// requireOnStopPartitionSessionCalled waits up to checkStepTimeout for the
// user callback to fire or fails the test when the deadline is reached or the
// parent context is cancelled.
func (h *onStopPartitionSessionScenario) requireOnStopPartitionSessionCalled(ctx context.Context) {
	h.scope.t.Helper()

	ctx, cancel := context.WithTimeout(ctx, checkStepTimeout)
	defer cancel()

	select {
	case <-h.onStopCalled:
	case <-ctx.Done():
		h.scope.Require.NoError(ctx.Err(), "WithReaderOnStopPartitionSession was not invoked after reader rebalance")
	}
}

// requireReadMessageCommitted waits up to checkStepTimeout for the committed
// offset of the read message to be visible on the server.
func (h *onStopPartitionSessionScenario) requireReadMessageCommitted(
	ctx context.Context,
	msg *topicreader.Message,
) {
	h.scope.t.Helper()

	h.scope.Require.NotNil(msg, "first reader did not read the message")
	if msg == nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, checkStepTimeout)
	defer cancel()

	expectedOffset := msg.Offset + 1
	partitionID := msg.PartitionID()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		desc, err := h.scope.DriverWithGRPCLogging().Topic().DescribeTopicConsumer(
			ctx,
			h.topicPath,
			h.scope.TopicConsumerName(),
			topicoptions.IncludeConsumerStats(),
		)
		if err == nil {
			for _, partition := range desc.Partitions {
				if partition.PartitionID == partitionID &&
					partition.PartitionConsumerStats.CommittedOffset == expectedOffset {
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			h.scope.Require.NoError(ctx.Err(), "message committed offset was not observed on the server")
			return
		case <-ticker.C:
		}
	}
}

// scenarioReader wraps a topic reader together with state needed by the
// rebalance scenario (partition session tracking).
type scenarioReader struct {
	t testing.TB

	reader *topicreader.Reader

	partitionCount   int
	partitions       atomic.Int64
	gotAllPartitions chan struct{}
}

// waitGotAllPartitions blocks until the reader has received every partition
// session, or fails the test when the context is cancelled.
//
// Internally it spawns a one-shot ReadMessage goroutine so the SDK pumps
// queued StartPartitionSessionRequests out of the batcher and sends
// StartPartitionSessionResponse to the server (without an active read the
// requests just sit in the batcher and the trace never fires; reader.WaitInit
// only waits for init_response and is not enough). The pump call is cancelled
// before this method returns; no data is consumed because no messages are
// written before this step.
func (r *scenarioReader) waitGotAllPartitions(ctx context.Context) {
	r.t.Helper()

	ctx, cancel := context.WithTimeout(ctx, checkStepTimeout)
	defer cancel()

	go func() {
		_, _ = r.reader.ReadMessage(ctx)
	}()

	select {
	case <-r.gotAllPartitions:
	case <-ctx.Done():
		r.t.Fatalf("reader didn't receive all partition sessions: %v", ctx.Err())
	}
}

// readMessage reads a message from the reader and returns it, or fails the
// test on read error. The wait is bounded by checkStepTimeout.
func (r *scenarioReader) readMessage(ctx context.Context) *topicreader.Message {
	r.t.Helper()

	ctx, cancel := context.WithTimeout(ctx, checkStepTimeout)
	defer cancel()

	msg, err := r.reader.ReadMessage(ctx)
	if err != nil {
		r.t.Fatalf("reader didn't read the message: %v", err)
		return nil
	}
	return msg
}

// commit acknowledges the given message via the underlying reader.
func (r *scenarioReader) commit(ctx context.Context, msg *topicreader.Message) error {
	return r.reader.Commit(ctx, msg)
}

// readInBackground starts a fire-and-forget ReadMessage in a goroutine. The
// goroutine exits when the reader is closed (registered cleanup) or the
// scenario context is cancelled. The result is intentionally ignored: the
// only purpose of the call is to make the SDK pump queued partition-session
// events out of the batcher.
func (r *scenarioReader) readInBackground(ctx context.Context) {
	go func() {
		_, _ = r.reader.ReadMessage(ctx)
	}()
}
