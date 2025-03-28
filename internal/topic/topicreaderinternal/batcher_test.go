package topicreaderinternal

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestBatcher_PushBatch(t *testing.T) {
	session1 := &topicreadercommon.PartitionSession{}
	session2 := &topicreadercommon.PartitionSession{}

	m11 := topicreadercommon.MessageWithSetCommitRangeForTest(&topicreadercommon.PublicMessage{
		WrittenAt: testTime(1),
	}, topicreadercommon.CommitRange{PartitionSession: session1})

	m12 := topicreadercommon.MessageWithSetCommitRangeForTest(&topicreadercommon.PublicMessage{
		WrittenAt: testTime(2),
	}, topicreadercommon.CommitRange{PartitionSession: session1})

	m21 := topicreadercommon.MessageWithSetCommitRangeForTest(&topicreadercommon.PublicMessage{
		WrittenAt: testTime(3),
	}, topicreadercommon.CommitRange{PartitionSession: session2})

	m22 := topicreadercommon.MessageWithSetCommitRangeForTest(&topicreadercommon.PublicMessage{
		WrittenAt: testTime(4),
	}, topicreadercommon.CommitRange{PartitionSession: session2})

	batch1 := mustNewBatch(session1, []*topicreadercommon.PublicMessage{m11, m12})
	batch2 := mustNewBatch(session2, []*topicreadercommon.PublicMessage{m21})
	batch3 := mustNewBatch(session2, []*topicreadercommon.PublicMessage{m22})

	b := newBatcher()
	require.NoError(t, b.PushBatches(batch1))
	require.NoError(t, b.PushBatches(batch2))
	require.NoError(t, b.PushBatches(batch3))

	expectedSession1 := newBatcherItemBatch(mustNewBatch(session1, []*topicreadercommon.PublicMessage{m11, m12}))
	expectedSession2 := newBatcherItemBatch(mustNewBatch(session2, []*topicreadercommon.PublicMessage{m21, m22}))

	expected := batcherMessagesMap{
		session1: batcherMessageOrderItems{expectedSession1},
		session2: batcherMessageOrderItems{expectedSession2},
	}
	require.Equal(t, expected, b.messages)
}

func TestBatcher_PushRawMessage(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		session := &topicreadercommon.PartitionSession{}
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}
		require.NoError(t, b.PushRawMessage(session, m))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemRawMessage(m)}}
		require.Equal(t, expectedMap, b.messages)
	})
	t.Run("AddRawAfterBatch", func(t *testing.T) {
		b := newBatcher()
		session := &topicreadercommon.PartitionSession{}
		batch := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}

		require.NoError(t, b.PushBatches(batch))
		require.NoError(t, b.PushRawMessage(session, m))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{
			newBatcherItemBatch(batch),
			newBatcherItemRawMessage(m),
		}}
		require.Equal(t, expectedMap, b.messages)
	})

	t.Run("AddBatchRawBatchBatch", func(t *testing.T) {
		b := newBatcher()
		session := &topicreadercommon.PartitionSession{}
		batch1 := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(2)}})
		batch3 := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(3)}})
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}

		require.NoError(t, b.PushBatches(batch1))
		require.NoError(t, b.PushRawMessage(session, m))
		require.NoError(t, b.PushBatches(batch2))
		require.NoError(t, b.PushBatches(batch3))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{
			newBatcherItemBatch(batch1),
			newBatcherItemRawMessage(m),
			newBatcherItemBatch(mustNewBatch(
				session,
				[]*topicreadercommon.PublicMessage{{WrittenAt: testTime(2)}, {WrittenAt: testTime(3)}},
			)),
		}}
		require.Equal(t, expectedMap, b.messages)
	})
}

func TestBatcher_Pop(t *testing.T) {
	t.Run("SimpleGet", func(t *testing.T) {
		ctx := context.Background()
		batch := mustNewBatch(nil, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()
		require.NoError(t, b.PushBatches(batch))

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
	})

	t.Run("SimpleOneOfTwo", func(t *testing.T) {
		ctx := context.Background()
		session1 := &topicreadercommon.PartitionSession{}
		session2 := &topicreadercommon.PartitionSession{}

		mess1 := &topicreadercommon.PublicMessage{WrittenAt: testTime(1)}
		topicreadercommon.MessageWithSetCommitRangeForTest(mess1, topicreadercommon.CommitRange{PartitionSession: session1})
		batch := mustNewBatch(
			session1,
			[]*topicreadercommon.PublicMessage{mess1},
		)

		mess2 := &topicreadercommon.PublicMessage{WrittenAt: testTime(2)}
		topicreadercommon.MessageWithSetCommitRangeForTest(mess2, topicreadercommon.CommitRange{PartitionSession: session2})
		batch2 := mustNewBatch(
			session2,
			[]*topicreadercommon.PublicMessage{mess2},
		)

		b := newBatcher()
		require.NoError(t, b.PushBatches(batch))
		require.NoError(t, b.PushBatches(batch2))

		possibleResults := []batcherMessageOrderItem{newBatcherItemBatch(batch), newBatcherItemBatch(batch2)}

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Contains(t, possibleResults, res)
		require.Len(t, b.messages, 1)

		res2, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Contains(t, possibleResults, res2)
		require.NotEqual(t, res, res2)
		require.Empty(t, b.messages)
	})

	xtest.TestManyTimesWithName(t, "GetBeforePut", func(t testing.TB) {
		ctx := context.Background()
		batch := mustNewBatch(nil, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()
		b.notifyAboutNewMessages()

		go func() {
			xtest.SpinWaitCondition(t, &b.m, func() bool {
				return len(b.hasNewMessages) == 0
			})
			_ = b.PushBatches(batch)
		}()

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
		require.Empty(t, b.messages)
	})

	t.Run("GetMaxOne", func(t *testing.T) {
		ctx := context.Background()

		m1 := &topicreadercommon.PublicMessage{WrittenAt: testTime(1)}
		m2 := &topicreadercommon.PublicMessage{WrittenAt: testTime(2)}
		batch := mustNewBatch(nil, []*topicreadercommon.PublicMessage{m1, m2})

		b := newBatcher()
		require.NoError(t, b.PushBatches(batch))

		res, err := b.Pop(ctx, batcherGetOptions{MaxCount: 1})
		require.NoError(t, err)

		expectedResult := newBatcherItemBatch(mustNewBatch(nil, []*topicreadercommon.PublicMessage{m1}))
		require.Equal(t, expectedResult, res)

		expectedMessages := batcherMessagesMap{
			nil: batcherMessageOrderItems{newBatcherItemBatch(mustNewBatch(nil, []*topicreadercommon.PublicMessage{m2}))},
		}
		require.Equal(t, expectedMessages, b.messages)
	})

	t.Run("GetFirstMessageFromSameSession", func(t *testing.T) {
		b := newBatcher()
		batch := mustNewBatch(nil, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		require.NoError(t, b.PushBatches(batch))
		require.NoError(t, b.PushRawMessage(nil, &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}))

		res, err := b.Pop(context.Background(), batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
	})

	t.Run("PreferFirstRawMessageFromDifferentSessions", func(t *testing.T) {
		session1 := &topicreadercommon.PartitionSession{}
		session2 := &topicreadercommon.PartitionSession{}

		b := newBatcher()
		m := &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}

		require.NoError(t, b.PushBatches(mustNewBatch(
			session1,
			[]*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}},
		)))
		require.NoError(t, b.PushRawMessage(session2, m))

		res, err := b.Pop(context.Background(), batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemRawMessage(m), res)
	})

	xtest.TestManyTimesWithName(t, "CloseBatcherWhilePopWait", func(t testing.TB) {
		ctx := xtest.Context(t)
		testErr := errors.New("test")

		b := newBatcher()
		b.notifyAboutNewMessages()

		require.Len(t, b.hasNewMessages, 1)

		popFinished := make(empty.Chan)
		popGoroutineStarted := make(empty.Chan)
		go func() {
			close(popGoroutineStarted)

			_, popErr := b.Pop(ctx, batcherGetOptions{MinCount: 1})
			require.ErrorIs(t, popErr, testErr)
			close(popFinished)
		}()

		xtest.WaitChannelClosed(t, popGoroutineStarted)

		// loop for wait Pop start wait message
		xtest.SpinWaitCondition(t, &b.m, func() bool {
			return len(b.hasNewMessages) == 0
		})

		require.NoError(t, b.Close(testErr))
		require.Error(t, b.Close(errors.New("second close")))

		xtest.WaitChannelClosed(t, popFinished)
	})
}

func TestBatcher_PopMinIgnored(t *testing.T) {
	t.Run("PopAfterForce", func(t *testing.T) {
		b := newBatcher()
		require.NoError(t, b.PushBatches(&topicreadercommon.PublicBatch{Messages: []*topicreadercommon.PublicMessage{
			{
				SeqNo: 1,
			},
		}}))

		b.IgnoreMinRestrictionsOnNextPop()

		ctx, cancel := xcontext.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 2})
		require.NoError(t, err)
		require.Len(t, batch.Batch.Messages, 1)
		require.False(t, b.forceIgnoreMinRestrictionsOnNextMessagesBatch)
	})

	xtest.TestManyTimesWithName(t, "ForceAfterPop", func(t testing.TB) {
		b := newBatcher()
		require.NoError(t, b.PushBatches(&topicreadercommon.PublicBatch{Messages: []*topicreadercommon.PublicMessage{
			{
				SeqNo: 1,
			},
		}}))

		var IgnoreMinRestrictionsOnNextPopDone atomic.Int64
		go func() {
			defer IgnoreMinRestrictionsOnNextPopDone.Add(1)

			xtest.SpinWaitCondition(t, &b.m, func() bool {
				return len(b.hasNewMessages) == 0
			})
			b.IgnoreMinRestrictionsOnNextPop()
		}()

		ctx, cancel := xcontext.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 2})

		require.NoError(t, err, IgnoreMinRestrictionsOnNextPopDone.Load())
		require.Len(t, batch.Batch.Messages, 1)
		require.False(t, b.forceIgnoreMinRestrictionsOnNextMessagesBatch)
	})
}

func TestBatcher_PopFlushed(t *testing.T) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(0))

	xtest.TestManyTimes(t, func(t testing.TB) {
		b := newBatcher()
		s1 := topicreadercommon.NewPartitionSession(ctx,
			"test",
			1,
			-1,
			"test",
			partitionSessionID(1),
			1,
			0,
		)
		s2 := topicreadercommon.NewPartitionSession(ctx,
			"test",
			2,
			-1,
			"test",
			partitionSessionID(1),
			1,
			0,
		)

		err := b.PushBatches(
			xtest.Must(
				topicreadercommon.NewBatch(
					s1,
					[]*topicreadercommon.PublicMessage{
						topicreadercommon.NewPublicMessageBuilder().PartitionSession(s1).Build(),
					},
				),
			),
			xtest.Must(
				topicreadercommon.NewBatch(
					s2,
					[]*topicreadercommon.PublicMessage{
						topicreadercommon.NewPublicMessageBuilder().PartitionSession(s2).Build(),
					},
				),
			),
		)
		require.NoError(t, err)

		sessions := []*topicreadercommon.PartitionSession{s1, s2}
		s := sessions[rnd.Intn(len(sessions))]
		b.FlushPartitionSession(s)

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)

		require.Same(t, s, topicreadercommon.BatchGetPartitionSession(res.Batch))
	}, xtest.StopAfter(time.Millisecond))
}

func TestBatcherConcurency(t *testing.T) {
	xtest.TestManyTimesWithName(t, "OneBatch", func(tb testing.TB) {
		b := newBatcher()

		go func() {
			_ = b.PushBatches(&topicreadercommon.PublicBatch{Messages: []*topicreadercommon.PublicMessage{{SeqNo: 1}}})
		}()

		ctx, cancel := xcontext.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 1})
		require.NoError(tb, err)
		require.Equal(tb, int64(1), batch.Batch.Messages[0].SeqNo)
	})

	xtest.TestManyTimesWithName(t, "ManyRawMessages", func(tb testing.TB) {
		const count = 10
		b := newBatcher()
		session := &topicreadercommon.PartitionSession{}

		go func() {
			for i := 0; i < count; i++ {
				_ = b.PushRawMessage(session, &rawtopicreader.StartPartitionSessionRequest{
					CommittedOffset:  rawtopiccommon.NewOffset(int64(i)),
					PartitionOffsets: rawtopiccommon.OffsetRange{},
				})
			}
		}()

		ctx, cancel := xcontext.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for i := 0; i < count; i++ {
			res, err := b.Pop(ctx, batcherGetOptions{MinCount: 1})
			require.NoError(tb, err)
			require.Equal(
				tb,
				rawtopiccommon.NewOffset(int64(i)),
				res.RawMessage.(*rawtopicreader.StartPartitionSessionRequest).CommittedOffset,
			)
		}
	})
}

func TestBatcher_Find(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		findRes := b.findNeedLock(batcherGetOptions{})
		require.False(t, findRes.Ok)
	})
	t.Run("FoundEmptyFilter", func(t *testing.T) {
		session := &topicreadercommon.PartitionSession{}
		batch := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()

		require.NoError(t, b.PushBatches(batch))

		findRes := b.findNeedLock(batcherGetOptions{})
		expectedResult := batcherResultCandidate{
			Key:         session,
			Result:      newBatcherItemBatch(batch),
			Rest:        batcherMessageOrderItems{},
			WaiterIndex: 0,
			Ok:          true,
		}
		require.Equal(t, expectedResult, findRes)
	})

	t.Run("FoundPartialBatchFilter", func(t *testing.T) {
		session := &topicreadercommon.PartitionSession{}
		batch := mustNewBatch(
			session,
			[]*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}},
		)

		b := newBatcher()

		require.NoError(t, b.PushBatches(batch))

		findRes := b.findNeedLock(batcherGetOptions{MaxCount: 1})

		expectedResult := newBatcherItemBatch(mustNewBatch(
			session,
			[]*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}},
		))
		expectedRestBatch := newBatcherItemBatch(mustNewBatch(
			session,
			[]*topicreadercommon.PublicMessage{{WrittenAt: testTime(2)}},
		))

		expectedCandidate := batcherResultCandidate{
			Key:         session,
			Result:      expectedResult,
			Rest:        batcherMessageOrderItems{expectedRestBatch},
			WaiterIndex: 0,
			Ok:          true,
		}
		require.Equal(t, expectedCandidate, findRes)
	})
}

func TestBatcher_Apply(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		session := &topicreadercommon.PartitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: batcherMessageOrderItems{newBatcherItemBatch(batch)},
		}
		b.applyNeedLock(&foundRes)

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemBatch(batch)}}
		require.Equal(t, expectedMap, b.messages)
	})

	t.Run("Delete", func(t *testing.T) {
		session := &topicreadercommon.PartitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})

		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: batcherMessageOrderItems{},
		}

		b.messages = batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemBatch(batch)}}

		b.applyNeedLock(&foundRes)

		require.Empty(t, b.messages)
	})
}

func TestBatcherGetOptions_Split(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		opts := batcherGetOptions{}
		batch := mustNewBatch(nil, []*topicreadercommon.PublicMessage{
			{WrittenAt: testTime(1)},
			{WrittenAt: testTime(2)},
		},
		)
		head, rest, ok := opts.splitBatch(batch)

		require.Equal(t, batch, head)
		require.True(t, topicreadercommon.BatchIsEmpty(rest))
		require.True(t, ok)
	})
	t.Run("MinCount", func(t *testing.T) {
		opts := batcherGetOptions{MinCount: 2}
		batch1 := mustNewBatch(nil, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []*topicreadercommon.PublicMessage{
			{WrittenAt: testTime(1)},
			{WrittenAt: testTime(2)},
		},
		)

		head, rest, ok := opts.splitBatch(batch1)
		require.True(t, topicreadercommon.BatchIsEmpty(head))
		require.True(t, topicreadercommon.BatchIsEmpty(rest))
		require.False(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, topicreadercommon.BatchIsEmpty(rest))
		require.True(t, ok)
	})
	t.Run("MaxCount", func(t *testing.T) {
		opts := batcherGetOptions{MaxCount: 2}
		batch1 := mustNewBatch(nil, []*topicreadercommon.PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []*topicreadercommon.PublicMessage{
			{WrittenAt: testTime(1)},
			{WrittenAt: testTime(2)},
		},
		)
		batch3 := mustNewBatch(
			nil,
			[]*topicreadercommon.PublicMessage{
				{WrittenAt: testTime(11)},
				{WrittenAt: testTime(12)},
				{WrittenAt: testTime(13)},
				{WrittenAt: testTime(14)},
			},
		)

		head, rest, ok := opts.splitBatch(batch1)
		require.Equal(t, batch1, head)
		require.True(t, topicreadercommon.BatchIsEmpty(rest))
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, topicreadercommon.BatchIsEmpty(rest))
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch3)
		expectedHead := mustNewBatch(
			nil,
			[]*topicreadercommon.PublicMessage{
				{WrittenAt: testTime(11)},
				{WrittenAt: testTime(12)},
			},
		)
		expectedRest := mustNewBatch(
			nil,
			[]*topicreadercommon.PublicMessage{
				{WrittenAt: testTime(13)},
				{WrittenAt: testTime(14)},
			},
		)
		require.Equal(t, expectedHead, head)
		require.Equal(t, expectedRest, rest)
		require.True(t, ok)
	})
}

func TestBatcher_Fire(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		b.notifyAboutNewMessages()
	})
}

func mustNewBatch(
	session *topicreadercommon.PartitionSession,
	messages []*topicreadercommon.PublicMessage,
) *topicreadercommon.PublicBatch {
	return xtest.Must(topicreadercommon.NewBatch(session, messages))
}

func testTime(num int) time.Time {
	return time.Date(2022, 6, 17, 0, 0, 0, num, time.UTC)
}
