package topicreaderexamples

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func OwnReadProgressStorageWithStop(ctx context.Context, db *ydb.Driver) {
	workWithPartition := map[int64]*sync.Mutex{}

	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithReaderGetPartitionStartOffset(
			func(
				ctx context.Context,
				req topicoptions.GetPartitionStartOffsetRequest,
			) (
				res topicoptions.GetPartitionStartOffsetResponse,
				err error,
			) {
				workWithPartition[req.PartitionSessionID] = &sync.Mutex{}
				offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
				res.StartFrom(offset)

				// Reader will stop if return err != nil
				return res, err
			}),
		topicoptions.WithReaderOnStopPartition(func(ctx context.Context, req topicoptions.StopPartitionSession) {
			workWithPartition[req.PartitionSessionID].Lock()
			defer workWithPartition[req.PartitionSessionID].Unlock()

			unlockPartition(ctx, req.PartitionSessionID)
		}),
	)

	for {
		batch, _ := reader.ReadMessagesBatch(ctx)

		// ... race condition between user lock and stop partition
		workWithPartition[batch.PartitionSessionID()].Lock()

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
		workWithPartition[batch.PartitionSessionID()].Unlock()
	}
}
