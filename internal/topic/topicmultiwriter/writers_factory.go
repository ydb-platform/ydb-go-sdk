package topicmultiwriter

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
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

type transactionalWritersFactory struct {
	tx tx.Transaction
}

func newTransactionalWritersFactory(tx tx.Transaction) *transactionalWritersFactory {
	return &transactionalWritersFactory{
		tx: tx,
	}
}

func (f *transactionalWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	writer, err := topicwriterinternal.NewWriterReconnector(cfg)
	if err != nil {
		return nil, err
	}

	return topicwriterinternal.NewTopicWriterTransaction(writer, f.tx, cfg.Tracer), nil
}
