package topicreaderinternal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestBatchTxStorageAdd_NewTransaction(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch := createTestBatch("topic-1", 1, 10, 20, 1)

	exists := newBatchTxStorage("test-consumer").Add(tx, batch)

	assert.False(t, exists)
}

func TestBatchTxStorageAdd_ExistingTransaction(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 20, 30, 1)

	storage := newBatchTxStorage("test-consumer")

	storage.Add(tx, batch1)
	exists := storage.Add(tx, batch2)

	assert.True(t, exists)
}

func TestBatchTxStorageGetBatches_Empty(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")

	batches := newBatchTxStorage("test-consumer").GetBatches(tx)

	assert.Empty(t, batches)
}

func TestBatchTxStorageGetBatches_WithBatches(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 20, 30, 1)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)

	batches := storage.GetBatches(tx)

	require.Len(t, batches, 2)
	assert.Equal(t, batch1, batches[0])
	assert.Equal(t, batch2, batches[1])
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_Empty(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")

	req, err := newBatchTxStorage("test-consumer").GetUpdateOffsetsInTransactionRequest(tx)

	assert.NoError(t, err)
	assert.Nil(t, req)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_SingleBatch(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	_ = tx.UnLazy(context.TODO())
	batch := createTestBatch("topic-1", 1, 10, 20, 1)

	storage := newBatchTxStorage("test-consumer")

	storage.Add(tx, batch)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, "test-consumer", req.Consumer)
	assert.Equal(t, "tx-1", req.Tx.ID)
	assert.Equal(t, "session-1", req.Tx.Session)
	require.Len(t, req.Topics, 1)
	assert.Equal(t, "topic-1", req.Topics[0].Path)
	require.Len(t, req.Topics[0].Partitions, 1)
	assert.Equal(t, int64(1), req.Topics[0].Partitions[0].PartitionID)
	require.Len(t, req.Topics[0].Partitions[0].PartitionOffsets, 1)
	assert.Equal(t, rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(20), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_MultipleBatches(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 20, 30, 1)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 1)
	require.Len(t, req.Topics[0].Partitions, 1)
	require.Len(t, req.Topics[0].Partitions[0].PartitionOffsets, 1)
	assert.Equal(t, rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(30), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_MultipleTopics(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-2", 2, 30, 40, 2)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 2)
	topicMap := buildTopicMap(req.Topics)
	assert.Contains(t, topicMap, "topic-1")
	assert.Contains(t, topicMap, "topic-2")
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_MultiplePartitionsSameTopic(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 2, 30, 40, 2)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 1)
	assert.Equal(t, "topic-1", req.Topics[0].Path)
	require.Len(t, req.Topics[0].Partitions, 2)

	partitionMap := buildPartitionMap(req.Topics[0].Partitions)
	assert.Contains(t, partitionMap, int64(1))
	assert.Contains(t, partitionMap, int64(2))
	require.Len(t, partitionMap[1].PartitionOffsets, 1)
	require.Len(t, partitionMap[2].PartitionOffsets, 1)
	assert.Equal(t, rawtopiccommon.Offset(10), partitionMap[1].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(20), partitionMap[1].PartitionOffsets[0].End)
	assert.Equal(t, rawtopiccommon.Offset(30), partitionMap[2].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(40), partitionMap[2].PartitionOffsets[0].End)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_NonAdjacentBatches(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 30, 40, 1)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 1)
	require.Len(t, req.Topics[0].Partitions, 1)
	require.Len(t, req.Topics[0].Partitions[0].PartitionOffsets, 2)
	assert.Equal(t, rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(20), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
	assert.Equal(t, rawtopiccommon.Offset(30), req.Topics[0].Partitions[0].PartitionOffsets[1].Start)
	assert.Equal(t, rawtopiccommon.Offset(40), req.Topics[0].Partitions[0].PartitionOffsets[1].End)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_MultiplePartitionsMultipleTopics(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 2, 30, 40, 2)
	batch3 := createTestBatch("topic-2", 1, 50, 60, 3)
	batch4 := createTestBatch("topic-2", 2, 70, 80, 4)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)
	storage.Add(tx, batch3)
	storage.Add(tx, batch4)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 2)

	topicMap := buildTopicOffsetsMap(req.Topics)
	assert.Contains(t, topicMap, "topic-1")
	assert.Contains(t, topicMap, "topic-2")
	require.Len(t, topicMap["topic-1"].Partitions, 2)
	require.Len(t, topicMap["topic-2"].Partitions, 2)

	partition1Map := buildPartitionMap(topicMap["topic-1"].Partitions)
	assert.Contains(t, partition1Map, int64(1))
	assert.Contains(t, partition1Map, int64(2))

	partition2Map := buildPartitionMap(topicMap["topic-2"].Partitions)
	assert.Contains(t, partition2Map, int64(1))
	assert.Contains(t, partition2Map, int64(2))
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_ComplexOptimization(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 20, 30, 1)
	batch3 := createTestBatch("topic-1", 1, 30, 40, 1)
	batch4 := createTestBatch("topic-1", 1, 50, 60, 1)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)
	storage.Add(tx, batch3)
	storage.Add(tx, batch4)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 1)
	require.Len(t, req.Topics[0].Partitions, 1)
	require.Len(t, req.Topics[0].Partitions[0].PartitionOffsets, 2)
	assert.Equal(t, rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(40), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
	assert.Equal(t, rawtopiccommon.Offset(50), req.Topics[0].Partitions[0].PartitionOffsets[1].Start)
	assert.Equal(t, rawtopiccommon.Offset(60), req.Topics[0].Partitions[0].PartitionOffsets[1].End)
}

func TestBatchTxStorageGetUpdateOffsetsInTransactionRequest_MixedPartitionsAndTopics(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-1", 1, 20, 30, 1)
	batch3 := createTestBatch("topic-1", 2, 40, 50, 2)
	batch4 := createTestBatch("topic-1", 2, 60, 70, 2)
	batch5 := createTestBatch("topic-2", 1, 80, 90, 3)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch1)
	storage.Add(tx, batch2)
	storage.Add(tx, batch3)
	storage.Add(tx, batch4)
	storage.Add(tx, batch5)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)

	require.NoError(t, err)
	require.NotNil(t, req)
	require.Len(t, req.Topics, 2)

	topicMap := buildTopicOffsetsMap(req.Topics)
	assert.Contains(t, topicMap, "topic-1")
	require.Len(t, topicMap["topic-1"].Partitions, 2)
	assert.Contains(t, topicMap, "topic-2")
	require.Len(t, topicMap["topic-2"].Partitions, 1)

	partition1Map := buildPartitionMap(topicMap["topic-1"].Partitions)
	require.Len(t, partition1Map[1].PartitionOffsets, 1) // Merged
	assert.Equal(t, rawtopiccommon.Offset(10), partition1Map[1].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(30), partition1Map[1].PartitionOffsets[0].End)

	require.Len(t, partition1Map[2].PartitionOffsets, 2) // Not merged
	assert.Equal(t, rawtopiccommon.Offset(40), partition1Map[2].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(50), partition1Map[2].PartitionOffsets[0].End)
	assert.Equal(t, rawtopiccommon.Offset(60), partition1Map[2].PartitionOffsets[1].Start)
	assert.Equal(t, rawtopiccommon.Offset(70), partition1Map[2].PartitionOffsets[1].End)

	require.Len(t, topicMap["topic-2"].Partitions[0].PartitionOffsets, 1)
	assert.Equal(t, rawtopiccommon.Offset(80), topicMap["topic-2"].Partitions[0].PartitionOffsets[0].Start)
	assert.Equal(t, rawtopiccommon.Offset(90), topicMap["topic-2"].Partitions[0].PartitionOffsets[0].End)
}

func TestBatchTxStorageClear(t *testing.T) {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	batch := createTestBatch("topic-1", 1, 10, 20, 1)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx, batch)

	storage.Clear(tx)

	batches := storage.GetBatches(tx)
	assert.Empty(t, batches)

	req, err := storage.GetUpdateOffsetsInTransactionRequest(tx)
	assert.NoError(t, err)
	assert.Nil(t, req)
}

func TestBatchTxStorageMultipleTransactions(t *testing.T) {
	tx1 := newMockTransactionWrapper("session-1", "tx-1")
	_ = tx1.UnLazy(context.TODO())
	tx2 := newMockTransactionWrapper("session-2", "tx-2")
	_ = tx1.UnLazy(context.TODO())
	batch1 := createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := createTestBatch("topic-2", 2, 30, 40, 2)

	storage := newBatchTxStorage("test-consumer")
	storage.Add(tx1, batch1)
	storage.Add(tx2, batch2)

	batches1 := storage.GetBatches(tx1)
	batches2 := storage.GetBatches(tx2)

	require.Len(t, batches1, 1)
	assert.Equal(t, batch1, batches1[0])
	require.Len(t, batches2, 1)
	assert.Equal(t, batch2, batches2[0])

	storage.Clear(tx1)

	batches1 = storage.GetBatches(tx1)
	assert.Empty(t, batches1)
	batches2 = storage.GetBatches(tx2)
	require.Len(t, batches2, 1)
	assert.Equal(t, batch2, batches2[0])
}

// Helper methods for assertions

func buildTopicMap(
	topics []rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets,
) map[string]bool {
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic.Path] = true
	}

	return topicMap
}

func buildTopicOffsetsMap(
	topics []rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets,
) map[string]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets {
	topicMap := make(map[string]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets)
	for _, topic := range topics {
		topicMap[topic.Path] = topic
	}

	return topicMap
}

func buildPartitionMap(
	partitions []rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets,
) map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets {
	partitionMap := make(map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for _, partition := range partitions {
		partitionMap[partition.PartitionID] = partition
	}

	return partitionMap
}

// Helper methods for test data creation

func createTestBatch(
	topic string,
	partitionID int64,
	startOffset, endOffset int64,
	sessionID int,
) *topicreadercommon.PublicBatch {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := topicreadercommon.NewPartitionSession(
		ctx,
		topic,
		partitionID,
		1,
		"connection-1",
		rawtopicreader.PartitionSessionID(sessionID),
		int64(sessionID),
		rawtopiccommon.Offset(0),
	)

	batch, _ := topicreadercommon.NewBatch(session, nil)

	commitRange := topicreadercommon.CommitRange{
		CommitOffsetStart: rawtopiccommon.Offset(startOffset),
		CommitOffsetEnd:   rawtopiccommon.Offset(endOffset),
		PartitionSession:  session,
	}

	return topicreadercommon.BatchSetCommitRangeForTest(batch, commitRange)
}
