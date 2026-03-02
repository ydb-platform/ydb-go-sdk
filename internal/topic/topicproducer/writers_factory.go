package topicproducer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	topicclient "github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

var (
	_ writersFactory = (*baseWritersFactory)(nil)
)

type baseWritersFactory struct {
	client topicclient.Client
}

func newBaseWritersFactory(client topicclient.Client) *baseWritersFactory {
	return &baseWritersFactory{
		client: client,
	}
}

func (f *baseWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	writer, err := topicwriterinternal.NewWriterReconnector(cfg)
	if err != nil {
		return nil, err
	}
	return writer, nil
}
