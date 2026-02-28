package topicproducer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

var (
	_ subWritersFactory = (*baseSubWritersFactory)(nil)
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
