package topicreaderexamples

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func OwnReadProgressReadEvents(ctx context.Context, db *ydb.Driver) error {
	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"))

	partitioninWork := map[int64]chan topicreader.Event{}

	for {
		event, err := reader.ReadEvent(ctx)
		if err != nil {
			return err
		}

		switch {
		case event.StartPartition != nil:
			ch := make(chan topicreader.Event, 100)
			partitioninWork[event.StartPartition.PartitionSessionID] = ch
			go partitionWorker(ctx, reader, ch)
		case event.ReadData != nil:
			partitioninWork[event.StartPartition.PartitionSessionID] <- event
			processBatch(ctx, event.ReadData.Batch)
		case event.StopPartition != nil:
			// may be doubles
			if ch, ok := partitioninWork[event.StartPartition.PartitionSessionID]; ok {
				ch <- event
				close(ch)
				delete(partitioninWork, event.StartPartition.PartitionSessionID)
			}
		}
	}
}

func partitionWorker(ctx context.Context, reader *topicreader.Reader, ch chan topicreader.Event) {
	for event := range ch {
		if event.StartPartition != nil {
			lockPartition(ctx, event.StartPartition.Topic, event.StartPartition.PartitionSessionID)
		}
		if event.ReadData != nil {
			processBatch(ctx, event.ReadData.Batch)
		}
		if event.StopPartition != nil {
			unlockPartition(ctx, event.StopPartition.PartitionSessionID)
		}
	}
}
