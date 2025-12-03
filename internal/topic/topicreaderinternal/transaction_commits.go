package topicreaderinternal

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type TransactionCommitsStorage struct {
	commits map[string]*transactionCommits // by `tx.ID`

	m xsync.RWMutex

	reader *topicStreamReaderImpl
}

func (s *TransactionCommitsStorage) Append(tx tx.Transaction, batch *topicreadercommon.PublicBatch) {
	tc := s.getTransactionCommits(tx)
	tc.AppendBatch(batch)
}

func (s *TransactionCommitsStorage) getTransactionCommits(transaction tx.Transaction) *transactionCommits {
	s.m.Lock()
	defer s.m.Unlock()

	tc, ok := s.commits[transaction.ID()]
	if !ok {
		tc = s.newTransactionCommits(transaction)
		s.commits[transaction.ID()] = tc
	}

	return tc
}

func (s *TransactionCommitsStorage) newTransactionCommits(transaction tx.Transaction) *transactionCommits {
	ranges := topicreadercommon.NewCommitRangesWithCapacity(1)
	return &transactionCommits{
		tx:           transaction,
		commitRanges: &ranges,
		reader:       s.reader,
	}
}

// transactionCommits manages commit ranges for transactions
type transactionCommits struct {
	tx           tx.Transaction
	commitRanges *topicreadercommon.CommitRanges

	beforeCommitOnce sync.Once
	completedOnce    sync.Once

	reader *topicStreamReaderImpl
}

// AppendBatch adds a batch to the commit ranges
func (tc *transactionCommits) AppendBatch(batch *topicreadercommon.PublicBatch) {
	tc.commitRanges.Append(batch)
}

// BeforeCommitFn returns a function that will be called before transaction commit
// This function is guaranteed to be called only once per transaction using sync.Once
func (s *TransactionCommitsStorage) BeforeCommitFn(tx tx.Transaction) tx.OnTransactionBeforeCommit {
	return func(ctx context.Context) (err error) {
		s.m.RLock()
		tc, ok := s.commits[tx.ID()]
		s.m.RUnlock()

		if !ok {
			return
		}

		// Ensure this function is called only once
		var callErr error
		tc.beforeCommitOnce.Do(func() {
			callErr = tc.updateOffsetsInTransaction(ctx)
		})

		return callErr
	}
}

func (s *TransactionCommitsStorage) CompletedFn(tx tx.Transaction) tx.OnTransactionCompletedFunc {
	return func(err error) {
		s.m.RLock()
		tc, ok := s.commits[tx.ID()]
		s.m.RUnlock()

		if !ok {
			return
		}

		tc.completedOnce.Do(func() {
			defer func() {
				s.m.Lock()
				defer s.m.Unlock()
				delete(s.commits, tx.ID())
			}()

			tc.onTransactionCompleted(err)
		})
	}
}

// updateOffsetsInTransaction updates offsets in the transaction
func (tc *transactionCommits) updateOffsetsInTransaction(ctx context.Context) (err error) {
	cfg := tc.reader.cfg
	logCtx := cfg.BaseContext
	onDone := trace.TopicOnReaderUpdateOffsetsInTransaction(
		cfg.Trace,
		&logCtx,
		tc.reader.readerID,
		tc.reader.readConnectionID,
		tc.tx.SessionID(),
		tc.tx,
	)

	defer func() {
		onDone(err)
	}()

	// UpdateOffsetsInTransaction operation must be executed on the same Node where the transaction was initiated.
	// Otherwise, errors such as `Database coordinators are unavailable` may occur.
	ctx = endpoint.WithNodeID(ctx, tc.tx.NodeID())

	req := tc.commitRangesToUpdateOffsetsRequest()

	err = tc.reader.topicClient.UpdateOffsetsInTransaction(ctx, req)
	if err != nil {
		// mark error as retryable - all `UpdateOffsetsInTransaction` failures can be retried
		err = xerrors.Retryable(err)

		return xerrors.WithStackTrace(fmt.Errorf("updating offsets in transaction: %w", err))
	}

	return nil
}

// onTransactionCompleted handles transaction completion
func (tc *transactionCommits) onTransactionCompleted(err error) {
	if err != nil {
		_ = tc.reader.CloseWithError(xcontext.ValueOnly(tc.reader.cfg.BaseContext), fmt.Errorf("transaction failed: %w", err))

		return
	}

	tc.commitRanges.Optimize()

	// Update committed offsets for all partition sessions
	for i := range tc.commitRanges.Ranges {
		tc.commitRanges.Optimize()
		commitRange := &tc.commitRanges.Ranges[i]
		commitRange.PartitionSession.SetCommittedOffsetForward(commitRange.CommitOffsetEnd)
	}
}

// commitRangesToUpdateOffsetsRequest converts commit ranges to UpdateOffsetsInTransaction request
func (tc *transactionCommits) commitRangesToUpdateOffsetsRequest() *rawtopic.UpdateOffsetsInTransactionRequest {
	req := &rawtopic.UpdateOffsetsInTransactionRequest{
		OperationParams: rawydb.NewRawOperationParamsFromProto(
			operation.Params(context.Background(), 0, 0, operation.ModeSync),
		),
		Tx: rawtopiccommon.TransactionIdentity{
			ID:      tc.tx.ID(),
			Session: tc.tx.SessionID(),
		},
		Consumer: tc.reader.cfg.Consumer,
	}

	// Convert commit ranges to topics structure
	partitionOffsets := tc.commitRanges.ToPartitionsOffsets()

	// Group partition offsets by topic
	topicMap := make(map[string][]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for i := range partitionOffsets {
		po := &partitionOffsets[i]
		// Find the corresponding partition session to get topic and partition ID
		var topic string
		var partitionID int64
		for j := range tc.commitRanges.Ranges {
			if tc.commitRanges.Ranges[j].PartitionSession.StreamPartitionSessionID == po.PartitionSessionID {
				topic = tc.commitRanges.Ranges[j].PartitionSession.Topic
				partitionID = tc.commitRanges.Ranges[j].PartitionSession.PartitionID
				break
			}
		}

		topicMap[topic] = append(topicMap[topic], rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets{
			PartitionID:      partitionID,
			PartitionOffsets: po.Offsets,
		})
	}

	// Convert map to slice
	req.Topics = make([]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets, 0, len(topicMap))
	for path, partitions := range topicMap {
		req.Topics = append(req.Topics, rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets{
			Path:       path,
			Partitions: partitions,
		})
	}

	return req
}
