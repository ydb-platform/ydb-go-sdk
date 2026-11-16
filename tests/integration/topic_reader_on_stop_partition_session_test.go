//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
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
// session after a rebalance between readers, and that commits from the callback
// reach the server. Two messages are read from two partitions and both are
// committed inside the callback.
func TestTopicReaderOnStopPartitionSessionAfterReaderRebalance(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	h := newOnStopPartitionSessionScenario(scope)

	h.createTopicWithPartitions(2)

	var msg0, msg1 *topicreader.Message

	h.createReader(
		topicoptions.WithReaderOnStopPartitionSession(func(req topicoptions.StopPartitionSessionRequest) topicoptions.OnStopPartitionSessionResult {
			defer h.onStopCalledOnce.Do(func() { close(h.onStopCalled) })

			if !req.Graceful {
				return topicoptions.OnStopPartitionSessionResult{}
			}

			h.scope.Require.NotNil(msg0)
			h.scope.Require.NotNil(msg1)
			h.scope.Require.NoError(h.firstReader.commit(ctx, msg0))
			h.scope.Require.NoError(h.firstReader.commit(ctx, msg1))

			return topicoptions.OnStopPartitionSessionResult{}
		}),
	)
	h.firstReader.waitGotAllPartitions(ctx)

	h.writeMessageToPartition(ctx, 0)
	h.writeMessageToPartition(ctx, 1)

	msg0 = h.firstReader.readMessage(ctx)
	msg1 = h.firstReader.readMessage(ctx)

	h.triggerStopPartitionGraceful()

	// Write a message and start a background ReadMessage on the first reader
	// so the SDK drains the queued StopPartitionSessionRequest from the
	// batcher (which lets the user callback run). The read may block forever
	// on the partition that stays with the first reader; we don't care about
	// the result and let the goroutine die when the reader is closed.
	h.writeMessage(ctx)
	h.firstReader.readInBackground(ctx)

	h.requireOnStopPartitionSessionCalled(ctx)
	h.requireReadMessageCommitted(ctx, msg0)
	h.requireReadMessageCommitted(ctx, msg1)
}

// TestTopicReaderOnStopPartitionSessionNonGracefulWhenChangefeedDropped checks that
// dropping a changefeed emits StopPartitionSession with graceful=false and invokes
// WithReaderOnStopPartitionSession with Graceful set to false.
func TestTopicReaderOnStopPartitionSessionNonGracefulWhenChangefeedDropped(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	h := newOnStopPartitionSessionScenario(scope)

	h.createChangefeedTopic()
	h.createReader(
		topicoptions.WithReaderOnStopPartitionSession(func(req topicoptions.StopPartitionSessionRequest) topicoptions.OnStopPartitionSessionResult {
			h.scope.Require.False(req.Graceful,
				"StopPartitionSession after changefeed drop must be non-graceful, got graceful=%v", req.Graceful)
			h.onStopCalledOnce.Do(func() { close(h.onStopCalled) })

			return topicoptions.OnStopPartitionSessionResult{}
		}),
	)
	h.firstReader.waitGotAllPartitions(ctx)

	h.upsertOneRow(ctx)

	readMessage := h.firstReader.readMessage(ctx)
	h.scope.Require.NotNil(readMessage)

	h.triggerStopPartitionNotGraceful(ctx)

	// Drain the batcher so StopPartitionSessionRequest is processed and the callback runs.
	h.firstReader.readInBackground(ctx)

	h.requireOnStopPartitionSessionCalled(ctx)
}

// onStopPartitionSessionScenario drives the stop-partition integration tests step by step
// (reader rebalance and changefeed drop).
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

// writeMessageToPartition writes one message to an explicit partition using a dedicated writer.
func (h *onStopPartitionSessionScenario) writeMessageToPartition(ctx context.Context, partitionID int64) {
	h.scope.t.Helper()

	writer, err := h.scope.Driver().Topic().StartWriter(
		h.topicPath,
		topicoptions.WithWriterPartitionID(partitionID),
		topicoptions.WithWriterProducerID(fmt.Sprintf("stop-partition-test-p%d", partitionID)),
		topicoptions.WithWriterWaitServerAck(true),
	)
	h.scope.Require.NoError(err)
	defer func() { _ = writer.Close(ctx) }()

	err = writer.Write(ctx, topicwriter.Message{
		Data: strings.NewReader(fmt.Sprintf("message-%d", partitionID)),
	})
	h.scope.Require.NoError(err)
}

// createReader starts the primary reader for topicPath (regular topic or CDC).
//
// The opts are merged with internal infrastructure options (partition-tracking
// trace) and passed to StartReader.
func (h *onStopPartitionSessionScenario) createReader(opts ...topicoptions.ReaderOption) {
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

// triggerStopPartitionGraceful starts a second reader on the same consumer so the
// server takes one partition away from the first reader and sends a graceful
// StopPartitionSession for that partition.
func (h *onStopPartitionSessionScenario) triggerStopPartitionGraceful() {
	h.scope.t.Helper()

	h.createSecondReader()
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
		h.scope.Require.NoError(ctx.Err(), "WithReaderOnStopPartitionSession was not invoked in time")
	}
}

// requireReadMessageCommitted waits up to checkStepTimeout for the committed
// offset of the read message to be visible on the server.
func (h *onStopPartitionSessionScenario) requireReadMessageCommitted(
	ctx context.Context,
	msg *topicreader.Message,
) {
	h.scope.t.Helper()

	h.scope.Require.NotNil(msg, "reader did not provide the message to check committed offset")
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

// createChangefeedTopic registers a CDC changefeed on the scope table and sets topicPath
// to the CDC topic (single partition).
func (h *onStopPartitionSessionScenario) createChangefeedTopic() {
	h.scope.t.Helper()

	h.partitionCount = 1
	h.topicPath = h.scope.ChangefeedTopicPath()
}

func (h *onStopPartitionSessionScenario) upsertOneRow(ctx context.Context) {
	h.scope.t.Helper()

	prefix := fmt.Sprintf(`PRAGMA TablePathPrefix("%s");`, h.scope.Folder())
	err := h.scope.execQueryContext(ctx, prefix+fmt.Sprintf(
		`UPSERT INTO %s (id, val) VALUES (1, "x");`,
		h.scope.TableName(),
	))
	h.scope.Require.NoError(err)
}

func (h *onStopPartitionSessionScenario) dropChangefeed(ctx context.Context) {
	h.scope.t.Helper()

	prefix := fmt.Sprintf(`PRAGMA TablePathPrefix("%s");`, h.scope.Folder())
	q := prefix + fmt.Sprintf(
		`ALTER TABLE %s DROP CHANGEFEED cdc;`,
		h.scope.TableName(),
	)
	err := h.scope.execQueryContext(ctx, q)
	h.scope.Require.NoError(err)
}

// triggerStopPartitionNotGraceful drops the CDC changefeed so the server sends
// StopPartitionSession with graceful=false.
func (h *onStopPartitionSessionScenario) triggerStopPartitionNotGraceful(ctx context.Context) {
	h.scope.t.Helper()

	h.dropChangefeed(ctx)
}

// scenarioReader wraps a topic reader together with state needed by the
// scenario (partition session tracking).
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
