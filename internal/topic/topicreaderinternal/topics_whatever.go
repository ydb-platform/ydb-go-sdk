package topicreaderinternal

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

type BatchTxStorage interface {
	Add(transaction tx.Transaction, batch *topicreadercommon.PublicBatch) (txAlreadyExists bool)
	GetUpdateOffsetsInTransactionRequest(transaction tx.Transaction) *rawtopic.UpdateOffsetsInTransactionRequest
	GetBatches(transaction tx.Transaction) []*topicreadercommon.PublicBatch
	Clear(transaction tx.Transaction)
}
