package topicproducer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

var _ writersFactory = (*baseWritersFactory)(nil)

type baseWritersFactory struct{}

func newBaseWritersFactory() *baseWritersFactory {
	return &baseWritersFactory{}
}

func (f *baseWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	writer, err := topicwriterinternal.NewWriterReconnector(cfg)
	if err != nil {
		return nil, err
	}

	return writer, nil
}
