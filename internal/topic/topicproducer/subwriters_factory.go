package topicproducer

import (
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

var (
	_ subWritersFactory = (*baseSubWritersFactory)(nil)
	_ subWritersFactory = (*stubSubWritersFactory)(nil)
)

type baseSubWritersFactory struct {
	client topicclient.Client
}

func newBaseSubWritersFactory(client topicclient.Client) *baseSubWritersFactory {
	return &baseSubWritersFactory{
		client: client,
	}
}

func (f *baseSubWritersFactory) Create(topicPath string, opts ...topicwriterinternal.PublicWriterOption) (subWriter, error) {
	return f.client.StartWriter(topicPath, opts...)
}

type stubSubWritersFactory struct {
	stubWriterType stubs.StubWriterType
	cfg            *ProducerConfig
}

func newStubSubWritersFactory(stubWriterType stubs.StubWriterType, cfg *ProducerConfig) *stubSubWritersFactory {
	return &stubSubWritersFactory{
		stubWriterType: stubWriterType,
		cfg:            cfg,
	}
}

func (f *stubSubWritersFactory) Create(topicPath string, opts ...topicwriterinternal.PublicWriterOption) (subWriter, error) {
	switch f.stubWriterType {
	case stubs.StubWriterTypeBasic:
		return stubs.NewBasicWriter(f.cfg.OnAckReceivedCallback), nil
	default:
		return nil, errors.New("invalid stub writer type")
	}
}
