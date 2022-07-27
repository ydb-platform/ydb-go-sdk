package topicreaderinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

func TestBatcher_PushBatch(t *testing.T) {
	session1 := &partitionSession{}
	session2 := &partitionSession{}

	m11 := &PublicMessage{
		WrittenAt:   testTime(1),
		commitRange: commitRange{partitionSession: session1},
	}
	m12 := &PublicMessage{
		WrittenAt:   testTime(2),
		commitRange: commitRange{partitionSession: session1},
	}
	m21 := &PublicMessage{
		WrittenAt:   testTime(3),
		commitRange: commitRange{partitionSession: session2},
	}
	m22 := &PublicMessage{
		WrittenAt:   testTime(4),
		commitRange: commitRange{partitionSession: session2},
	}

	batch1 := mustNewBatch(session1, []*PublicMessage{m11, m12})
	batch2 := mustNewBatch(session2, []*PublicMessage{m21})
	batch3 := mustNewBatch(session2, []*PublicMessage{m22})

	b := newBatcher()
	require.NoError(t, b.PushBatch(batch1))
	require.NoError(t, b.PushBatch(batch2))
	require.NoError(t, b.PushBatch(batch3))

	expectedSession1 := newBatcherItemBatch(mustNewBatch(session1, []*PublicMessage{m11, m12}))
	expectedSession2 := newBatcherItemBatch(mustNewBatch(session2, []*PublicMessage{m21, m22}))

	expected := batcherMessagesMap{
		session1: batcherMessageOrderItems{expectedSession1},
		session2: batcherMessageOrderItems{expectedSession2},
	}
	require.Equal(t, expected, b.messages)
}

func TestBatcher_PushRawMessage(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		session := &partitionSession{}
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}
		require.NoError(t, b.PushRawMessage(session, m))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemRawMessage(m)}}
		require.Equal(t, expectedMap, b.messages)
	})
	t.Run("AddRawAfterBatch", func(t *testing.T) {
		b := newBatcher()
		session := &partitionSession{}
		batch := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}

		require.NoError(t, b.PushBatch(batch))
		require.NoError(t, b.PushRawMessage(session, m))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{
			newBatcherItemBatch(batch),
			newBatcherItemRawMessage(m),
		}}
		require.Equal(t, expectedMap, b.messages)
	})

	t.Run("AddBatchRawBatchBatch", func(t *testing.T) {
		b := newBatcher()
		session := &partitionSession{}
		batch1 := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(2)}})
		batch3 := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(3)}})
		m := &rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: 1,
		}

		require.NoError(t, b.PushBatch(batch1))
		require.NoError(t, b.PushRawMessage(session, m))
		require.NoError(t, b.PushBatch(batch2))
		require.NoError(t, b.PushBatch(batch3))

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{
			newBatcherItemBatch(batch1),
			newBatcherItemRawMessage(m),
			newBatcherItemBatch(mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(2)}, {WrittenAt: testTime(3)}})),
		}}
		require.Equal(t, expectedMap, b.messages)
	})
}

func TestBatcher_Pop(t *testing.T) {
	t.Run("SimpleGet", func(t *testing.T) {
		ctx := context.Background()
		batch := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()
		require.NoError(t, b.PushBatch(batch))

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
	})

	t.Run("SimpleOneOfTwo", func(t *testing.T) {
		ctx := context.Background()
		session1 := &partitionSession{}
		session2 := &partitionSession{}
		batch := mustNewBatch(
			session1,
			[]*PublicMessage{{WrittenAt: testTime(1), commitRange: commitRange{partitionSession: session1}}},
		)
		batch2 := mustNewBatch(
			session2,
			[]*PublicMessage{{WrittenAt: testTime(2), commitRange: commitRange{partitionSession: session2}}},
		)

		b := newBatcher()
		require.NoError(t, b.PushBatch(batch))
		require.NoError(t, b.PushBatch(batch2))

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
		batch := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()

		go func() {
			xtest.SpinWaitCondition(t, &b.m, func() bool {
				return len(b.waiters) > 0
			})
			_ = b.PushBatch(batch)
		}()

		res, err := b.Pop(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
		require.Empty(t, b.messages)
	})

	t.Run("GetMaxOne", func(t *testing.T) {
		ctx := context.Background()

		m1 := &PublicMessage{WrittenAt: testTime(1)}
		m2 := &PublicMessage{WrittenAt: testTime(2)}
		batch := mustNewBatch(nil, []*PublicMessage{m1, m2})

		b := newBatcher()
		require.NoError(t, b.PushBatch(batch))

		res, err := b.Pop(ctx, batcherGetOptions{MaxCount: 1})
		require.NoError(t, err)

		expectedResult := newBatcherItemBatch(mustNewBatch(nil, []*PublicMessage{m1}))
		require.Equal(t, expectedResult, res)

		expectedMessages := batcherMessagesMap{
			nil: batcherMessageOrderItems{newBatcherItemBatch(mustNewBatch(nil, []*PublicMessage{m2}))},
		}
		require.Equal(t, expectedMessages, b.messages)
	})

	t.Run("GetFirstMessageFromSameSession", func(t *testing.T) {
		b := newBatcher()
		batch := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}})
		require.NoError(t, b.PushBatch(batch))
		require.NoError(t, b.PushRawMessage(nil, &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}))

		res, err := b.Pop(context.Background(), batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
	})

	t.Run("PreferFirstRawMessageFromDifferentSessions", func(t *testing.T) {
		session1 := &partitionSession{}
		session2 := &partitionSession{}

		b := newBatcher()
		m := &rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}

		require.NoError(t, b.PushBatch(mustNewBatch(session1, []*PublicMessage{{WrittenAt: testTime(1)}})))
		require.NoError(t, b.PushRawMessage(session2, m))

		res, err := b.Pop(context.Background(), batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemRawMessage(m), res)
	})

	xtest.TestManyTimesWithName(t, "CloseBatcherWhilePopWait", func(t testing.TB) {
		ctx := context.Background()
		testErr := errors.New("test")

		b := newBatcher()
		var err error

		popFinished := make(empty.Chan)
		go func() {
			_, err = b.Pop(ctx, batcherGetOptions{MinCount: 1})
			close(popFinished)
		}()

		// loop for wait Pop start wait message
		xtest.SpinWaitCondition(t, &b.m, func() bool {
			return len(b.waiters) > 0
		})

		require.NoError(t, b.Close(testErr))
		require.Error(t, b.Close(errors.New("second close")))

		<-popFinished
		require.ErrorIs(t, err, testErr)
	})
}

func TestBatcher_PopMinIgnored(t *testing.T) {
	t.Run("PopAfterForce", func(t *testing.T) {
		b := newBatcher()
		require.NoError(t, b.PushBatch(&PublicBatch{Messages: []*PublicMessage{
			{
				SeqNo: 1,
			},
		}}))

		b.IgnoreMinRestrictionsOnNextPop()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 2})
		require.NoError(t, err)
		require.Len(t, batch.Batch.Messages, 1)
		require.False(t, b.forceIgnoreMinRestrictionsOnNextMessagesBatch)
	})

	xtest.TestManyTimesWithName(t, "ForceAfterPop", func(t testing.TB) {
		b := newBatcher()
		require.NoError(t, b.PushBatch(&PublicBatch{Messages: []*PublicMessage{
			{
				SeqNo: 1,
			},
		}}))

		go func() {
			xtest.SpinWaitCondition(t, &b.m, func() bool {
				return len(b.waiters) > 0
			})
			b.IgnoreMinRestrictionsOnNextPop()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 2})

		require.NoError(t, err)
		require.Len(t, batch.Batch.Messages, 1)
		require.False(t, b.forceIgnoreMinRestrictionsOnNextMessagesBatch)
	})
}

func TestBatcherConcurency(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		b := newBatcher()

		go func() {
			_ = b.PushBatch(&PublicBatch{Messages: []*PublicMessage{{SeqNo: 1}}})
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		batch, err := b.Pop(ctx, batcherGetOptions{MinCount: 1})
		require.NoError(t, err)
		require.Equal(t, int64(1), batch.Batch.Messages[0].SeqNo)
	})
}

func TestBatcher_Find(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		findRes := b.findNeedLock(0, nil)
		require.False(t, findRes.Ok)
	})
	t.Run("FoundEmptyFilter", func(t *testing.T) {
		session := &partitionSession{}
		batch := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})

		b := newBatcher()

		require.NoError(t, b.PushBatch(batch))

		findRes := b.findNeedLock(0, []batcherWaiter{{}})
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
		session := &partitionSession{}
		batch := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		b := newBatcher()

		require.NoError(t, b.PushBatch(batch))

		findRes := b.findNeedLock(0, []batcherWaiter{{Options: batcherGetOptions{MaxCount: 1}}})

		expectedResult := newBatcherItemBatch(mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}}))
		expectedRestBatch := newBatcherItemBatch(mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(2)}}))

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
		session := &partitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})
		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: batcherMessageOrderItems{newBatcherItemBatch(batch)},
		}
		b.applyNeedLock(foundRes)

		expectedMap := batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemBatch(batch)}}
		require.Equal(t, expectedMap, b.messages)
	})

	t.Run("Delete", func(t *testing.T) {
		session := &partitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []*PublicMessage{{WrittenAt: testTime(1)}})

		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: batcherMessageOrderItems{},
		}

		b.messages = batcherMessagesMap{session: batcherMessageOrderItems{newBatcherItemBatch(batch)}}

		b.applyNeedLock(foundRes)

		require.Empty(t, b.messages)
	})
}

func TestBatcherGetOptions_Split(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		opts := batcherGetOptions{}
		batch := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})
		head, rest, ok := opts.splitBatch(batch)

		require.Equal(t, batch, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)
	})
	t.Run("MinCount", func(t *testing.T) {
		opts := batcherGetOptions{MinCount: 2}
		batch1 := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest, ok := opts.splitBatch(batch1)
		require.True(t, head.isEmpty())
		require.True(t, rest.isEmpty())
		require.False(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)
	})
	t.Run("MaxCount", func(t *testing.T) {
		opts := batcherGetOptions{MaxCount: 2}
		batch1 := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})
		batch3 := mustNewBatch(
			nil,
			[]*PublicMessage{
				{WrittenAt: testTime(11)},
				{WrittenAt: testTime(12)},
				{WrittenAt: testTime(13)},
				{WrittenAt: testTime(14)},
			},
		)

		head, rest, ok := opts.splitBatch(batch1)
		require.Equal(t, batch1, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch3)
		expectedHead := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(11)}, {WrittenAt: testTime(12)}})
		expectedRest := mustNewBatch(nil, []*PublicMessage{{WrittenAt: testTime(13)}, {WrittenAt: testTime(14)}})
		require.Equal(t, expectedHead, head)
		require.Equal(t, expectedRest, rest)
		require.True(t, ok)
	})
}

func TestBatcher_Fire(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		b.fireWaitersNeedLock()
	})
}

func mustNewBatch(session *partitionSession, messages []*PublicMessage) *PublicBatch {
	batch, err := newBatch(session, messages)
	if err != nil {
		panic(err)
	}
	return batch
}
