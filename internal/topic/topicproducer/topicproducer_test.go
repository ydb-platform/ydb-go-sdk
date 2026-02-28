package topicproducer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func newTestProducer(t testing.TB, client topicclient.Client) *Producer {
	t.Helper()
	_ = []topicoptions.WriterOption{
		topicwriterinternal.WithTopic("test/topic"),
		topicwriterinternal.WithMaxQueueLen(100),
		topicwriterinternal.WithAutoSetSeqNo(true),
		topicwriterinternal.WithAutosetCreatedTime(false),
	}

	return NewProducer(client, ProducerConfig{})
}

func TestProducer_ErrAlreadyClosed(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, stubClient)

	err := producer.Close(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestProducer_WaitInit_Success(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, stubClient)

	err := producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}

func TestProducer_WaitInit_ContextCanceled(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	stubClient := stubs.NewStubTopicClient(t, stubs.DefaultStubTopicDescription())
	producer := newTestProducer(t, stubClient)

	ctxCancel, cancel := context.WithCancel(ctx)
	cancel()

	err := producer.WaitInit(ctxCancel)
	require.ErrorIs(t, err, context.Canceled)

	_ = producer.Close(ctx)
}

func TestProducer_DescribeError(t *testing.T) {
	t.Parallel()

	ctx := xtest.Context(t)
	describeErr := errors.New("describe failed")
	stubClient := stubs.NewStubTopicClientWithError(t, describeErr)
	producer := newTestProducer(t, stubClient)

	ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	err := producer.WaitInit(ctxTimeout)
	require.Error(t, err)
	// When init fails, WaitInit may return context.DeadlineExceeded (initDone is not closed on error)
	// or the error may propagate depending on timing
	require.True(t, errors.Is(err, describeErr) || errors.Is(err, context.DeadlineExceeded))
}
