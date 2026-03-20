package topicreaderinternal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// errNoBatches is returned when there are no batches to process for a transaction.
var errNoBatches = errors.New("no batches for transaction")

// transactionBatches stores batches for a single transaction.
// It is not thread-safe and should be accessed only through batchTxStorage methods.
type transactionBatches struct {
	batches []*topicreadercommon.PublicBatch
}

// AddBatch adds a batch to the transaction.
func (tb *transactionBatches) AddBatch(batch *topicreadercommon.PublicBatch) {
	tb.batches = append(tb.batches, batch)
}

// GetBatches returns all batches stored for this transaction.
func (tb *transactionBatches) GetBatches() []*topicreadercommon.PublicBatch {
	return tb.batches
}

// batchTxStorage stores batches associated with transactions for commit within transaction.
// It is thread-safe and allows multiple transactions to be managed concurrently.
type batchTxStorage struct {
	transactions map[string]*transactionBatches
	consumer     string
	m            xsync.Mutex
}

// newBatchTxStorage creates a new batch transaction storage with the given consumer name.
// The consumer name is used when building UpdateOffsetsInTransactionRequest.
func newBatchTxStorage(consumer string) *batchTxStorage {
	return &batchTxStorage{
		transactions: make(map[string]*transactionBatches),
		consumer:     consumer,
	}
}

// GetOrCreateTransactionBatches gets or creates a transaction batches handler for the given transaction.
// This method is thread-safe.
func (s *batchTxStorage) GetOrCreateTransactionBatches(
	transaction tx.Transaction,
) (batches *transactionBatches, created bool) {
	s.m.Lock()
	defer s.m.Unlock()

	txID := transaction.ID()
	txBatches, exists := s.transactions[txID]
	if !exists {
		txBatches = &transactionBatches{
			batches: make([]*topicreadercommon.PublicBatch, 0),
		}
		s.transactions[txID] = txBatches
		created = true
	}

	return txBatches, created
}

// GetBatches returns all batches stored for the given transaction.
// Returns an empty slice (nil) if no batches are stored for the transaction.
// This method is thread-safe.
func (s *batchTxStorage) GetBatches(transaction tx.Transaction) []*topicreadercommon.PublicBatch {
	s.m.Lock()
	defer s.m.Unlock()

	txBatches, ok := s.transactions[transaction.ID()]
	if !ok {
		return nil
	}

	return txBatches.GetBatches()
}

// GetUpdateOffsetsInTransactionRequest builds an UpdateOffsetsInTransactionRequest
// from all batches stored for the given transaction.
// The batches are converted to commit ranges, optimized (adjacent ranges are merged),
// and grouped by topic and partition.
// Returns nil, nil if no batches are stored for the transaction.
// Returns an error if session info is missing for any partition offset.
// This method is thread-safe.
func (s *batchTxStorage) GetUpdateOffsetsInTransactionRequest(
	transaction tx.Transaction,
) (*rawtopic.UpdateOffsetsInTransactionRequest, error) {
	s.m.Lock()
	txBatches, ok := s.transactions[transaction.ID()]
	s.m.Unlock()

	if !ok {
		return nil, xerrors.WithStackTrace(errNoBatches)
	}

	batches := txBatches.GetBatches()
	if len(batches) == 0 {
		return nil, xerrors.WithStackTrace(errNoBatches)
	}

	// Convert batches to CommitRanges
	commitRanges := topicreadercommon.NewCommitRangesWithCapacity(len(batches))
	for _, batch := range batches {
		commitRange := topicreadercommon.GetCommitRange(batch)
		commitRanges.AppendCommitRange(commitRange)
	}

	// Optimize ranges (merge adjacent ranges)
	commitRanges.Optimize()

	// Build sessionID -> (topic, partitionID) map for efficient lookup
	sessionInfoMap := s.buildSessionInfoMap(batches)

	// Convert to partition offsets
	partitionOffsets := commitRanges.ToPartitionsOffsets()
	if len(partitionOffsets) == 0 {
		return nil, xerrors.WithStackTrace(errNoBatches)
	}

	// Group partition offsets by topic
	topicMap, err := s.buildPartitionOffsetsMap(partitionOffsets, sessionInfoMap)
	if err != nil {
		return nil, err
	}
	if len(topicMap) == 0 {
		return nil, xerrors.WithStackTrace(errNoBatches)
	}

	// Build request
	return s.buildUpdateOffsetsRequest(transaction, topicMap), nil
}

type sessionInfo struct {
	topic       string
	partitionID int64
}

// buildSessionInfoMap creates a map from partition session ID to topic and partition ID.
func (s *batchTxStorage) buildSessionInfoMap(
	batches []*topicreadercommon.PublicBatch,
) map[rawtopicreader.PartitionSessionID]sessionInfo {
	sessionInfoMap := make(map[rawtopicreader.PartitionSessionID]sessionInfo)
	for _, batch := range batches {
		commitRange := topicreadercommon.GetCommitRange(batch)
		sessionID := commitRange.PartitionSession.StreamPartitionSessionID
		if _, exists := sessionInfoMap[sessionID]; !exists {
			sessionInfoMap[sessionID] = sessionInfo{
				topic:       commitRange.PartitionSession.Topic,
				partitionID: commitRange.PartitionSession.PartitionID,
			}
		}
	}

	return sessionInfoMap
}

// buildPartitionOffsetsMap groups partition offsets by topic.
func (s *batchTxStorage) buildPartitionOffsetsMap(
	partitionOffsets []rawtopicreader.PartitionCommitOffset,
	sessionInfoMap map[rawtopicreader.PartitionSessionID]sessionInfo,
) (map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets, error) {
	topicMap := make(map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for i := range partitionOffsets {
		po := &partitionOffsets[i]
		info, ok := sessionInfoMap[po.PartitionSessionID]
		if !ok {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("session info not found for partition session ID %d", po.PartitionSessionID),
			)
		}

		topicMap[info.topic] = append(topicMap[info.topic], rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets{
			PartitionID:      info.partitionID,
			PartitionOffsets: po.Offsets,
		})
	}

	return topicMap, nil
}

// buildUpdateOffsetsRequest creates the final UpdateOffsetsInTransactionRequest.
func (s *batchTxStorage) buildUpdateOffsetsRequest(
	transaction tx.Transaction,
	topicMap map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets,
) *rawtopic.UpdateOffsetsInTransactionRequest {
	req := &rawtopic.UpdateOffsetsInTransactionRequest{
		OperationParams: rawydb.NewRawOperationParamsFromProto(
			operation.Params(context.Background(), 0, 0, operation.ModeSync),
		),
		Tx: rawtopiccommon.TransactionIdentity{
			ID:      transaction.ID(),
			Session: transaction.SessionID(),
		},
		Consumer: s.consumer,
		Topics:   make([]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets, 0, len(topicMap)),
	}

	for path, partitions := range topicMap {
		req.Topics = append(req.Topics, rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets{
			Path:       path,
			Partitions: partitions,
		})
	}

	return req
}

// Clear removes all batches stored for the given transaction.
// After calling Clear, GetBatches and GetUpdateOffsetsInTransactionRequest
// will return empty results for this transaction.
// This method is thread-safe.
func (s *batchTxStorage) Clear(transaction tx.Transaction) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.transactions, transaction.ID())
}
