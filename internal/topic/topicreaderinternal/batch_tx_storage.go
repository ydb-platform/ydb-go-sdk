package topicreaderinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// batchTxStorage stores batches associated with transactions for commit within transaction.
// It is thread-safe and allows multiple transactions to be managed concurrently.
type batchTxStorage struct {
	batches  map[string][]*topicreadercommon.PublicBatch
	consumer string
	m        xsync.RWMutex
}

// newBatchTxStorage creates a new batch transaction storage with the given consumer name.
// The consumer name is used when building UpdateOffsetsInTransactionRequest.
func newBatchTxStorage(consumer string) *batchTxStorage {
	return &batchTxStorage{
		batches:  make(map[string][]*topicreadercommon.PublicBatch),
		consumer: consumer,
	}
}

// Add adds a batch to the transaction storage.
// It returns true if the transaction already exists (has been added before), false otherwise.
// This method is thread-safe.
func (s *batchTxStorage) Add(transaction tx.Transaction, batch *topicreadercommon.PublicBatch) (txAlreadyExists bool) {
	s.m.Lock()
	defer s.m.Unlock()

	txID := transaction.ID()
	_, exists := s.batches[txID]
	s.batches[txID] = append(s.batches[txID], batch)

	return exists
}

// GetBatches returns all batches stored for the given transaction.
// Returns an empty slice (nil) if no batches are stored for the transaction.
// This method is thread-safe.
func (s *batchTxStorage) GetBatches(transaction tx.Transaction) []*topicreadercommon.PublicBatch {
	s.m.RLock()
	defer s.m.RUnlock()

	batches, ok := s.batches[transaction.ID()]
	if !ok {
		return nil
	}

	return batches
}

// GetUpdateOffsetsInTransactionRequest builds an UpdateOffsetsInTransactionRequest
// from all batches stored for the given transaction.
// The batches are converted to commit ranges, optimized (adjacent ranges are merged),
// and grouped by topic and partition.
// Returns nil if no batches are stored for the transaction.
// This method is thread-safe.
func (s *batchTxStorage) GetUpdateOffsetsInTransactionRequest(
	transaction tx.Transaction,
) *rawtopic.UpdateOffsetsInTransactionRequest {
	s.m.RLock()
	batches, ok := s.batches[transaction.ID()]
	s.m.RUnlock()

	if !ok || len(batches) == 0 {
		return nil
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
		return nil
	}

	// Group partition offsets by topic
	topicMap := s.buildPartitionOffsetsMap(partitionOffsets, sessionInfoMap)
	if len(topicMap) == 0 {
		return nil
	}

	// Build request
	return s.buildUpdateOffsetsRequest(transaction, topicMap)
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
) map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets {
	topicMap := make(map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for i := range partitionOffsets {
		po := &partitionOffsets[i]
		info, ok := sessionInfoMap[po.PartitionSessionID]
		if !ok {
			// Skip if session info not found (should not happen in normal flow)
			continue
		}

		topicMap[info.topic] = append(topicMap[info.topic], rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets{
			PartitionID:      info.partitionID,
			PartitionOffsets: po.Offsets,
		})
	}

	return topicMap
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

	delete(s.batches, transaction.ID())
}
