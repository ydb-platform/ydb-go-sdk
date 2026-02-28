package stubs

import (
	"context"
	"errors"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type stubTopicClient struct {
	describe    func(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error)
	startWriter func(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error)
}

func NewStubTopicClient(t testing.TB, desc topictypes.TopicDescription) *stubTopicClient {
	t.Helper()
	return &stubTopicClient{
		describe: func(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error) {
			return desc, nil
		},
		startWriter: func(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error) {
			// In test mode, stub factory is used - StartWriter is not called
			return nil, nil
		},
	}
}

func NewStubTopicClientWithError(t testing.TB, describeErr error) *stubTopicClient {
	t.Helper()
	return &stubTopicClient{
		describe: func(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error) {
			return topictypes.TopicDescription{}, describeErr
		},
		startWriter: nil,
	}
}

func (m *stubTopicClient) Describe(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error) {
	return m.describe(ctx, path, opts...)
}

func (m *stubTopicClient) Alter(ctx context.Context, path string, opts ...topicoptions.AlterOption) error {
	return nil
}

func (m *stubTopicClient) Create(ctx context.Context, path string, opts ...topicoptions.CreateOption) error {
	return nil
}

func (m *stubTopicClient) DescribeTopicConsumer(
	ctx context.Context, path string, consumer string, opts ...topicoptions.DescribeConsumerOption,
) (topictypes.TopicConsumerDescription, error) {
	return topictypes.TopicConsumerDescription{}, errors.New("not implemented")
}

func (m *stubTopicClient) Drop(ctx context.Context, path string, opts ...topicoptions.DropOption) error {
	return nil
}

func (m *stubTopicClient) StartListener(
	consumer string,
	handler topiclistener.EventHandler,
	readSelectors topicoptions.ReadSelectors,
	opts ...topicoptions.ListenerOption,
) (*topiclistener.TopicListener, error) {
	return nil, errors.New("not implemented")
}

func (m *stubTopicClient) StartReader(
	consumer string,
	readSelectors topicoptions.ReadSelectors,
	opts ...topicoptions.ReaderOption,
) (*topicreader.Reader, error) {
	return nil, errors.New("not implemented")
}

func (m *stubTopicClient) StartWriter(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error) {
	if m.startWriter == nil {
		return nil, errors.New("StartWriter not configured")
	}
	return m.startWriter(topicPath, opts...)
}

func (m *stubTopicClient) StartTransactionalWriter(
	txIdentifier tx.Identifier,
	topicpath string,
	opts ...topicoptions.WriterOption,
) (*topicwriter.TxWriter, error) {
	return nil, errors.New("not implemented")
}

var _ topic.Client = (*stubTopicClient)(nil)
