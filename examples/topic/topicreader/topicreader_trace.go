package topicreaderexamples

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// CommitNotify is example for receive commit notifications with async commit mode
func CommitNotify(ctx context.Context, db *ydb.Driver) {
	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderCommittedNotify: func(info trace.TopicReaderCommittedNotifyInfo) {
				// called when receive commit notify from server
				fmt.Println(info.Topic, info.PartitionID, info.CommittedOffset)
			},
		},
		),
	)

	for {
		msg, _ := reader.ReadMessage(ctx)
		processMessage(msg.Context(), msg)
	}
}

// ExplicitPartitionStartStopHandler is example for create own handler for stop partition event from server
func ExplicitPartitionStartStopHandler(ctx context.Context, db *ydb.Driver) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	reader, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),
		topicoptions.WithReaderTrace(
			trace.Topic{
				OnReaderPartitionReadStartResponse: func(
					info trace.TopicReaderPartitionReadStartResponseStartInfo,
				) func(
					trace.TopicReaderPartitionReadStartResponseDoneInfo,
				) {
					err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
					if err != nil {
						stopReader()
					}
					return nil
				},
				OnReaderPartitionReadStopResponse: func(
					info trace.TopicReaderPartitionReadStopResponseStartInfo,
				) func(
					trace.TopicReaderPartitionReadStopResponseDoneInfo,
				) {
					if info.Graceful {
						err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
						if err != nil {
							stopReader()
						}
					}
					return nil
				},
			},
		),
	)

	go func() {
		<-readContext.Done()
		_ = reader.Close(ctx)
	}()

	for {
		batch, _ := reader.ReadMessageBatch(readContext)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(
			batch.Context(),
			batch.Topic(),
			batch.PartitionID(),
			getEndOffset(batch),
		)
	}
}

// PartitionStartStopHandlerAndOwnReadProgressStorage example of complex use explicit start/stop partition handler
// and own progress storage in external system
func PartitionStartStopHandlerAndOwnReadProgressStorage(ctx context.Context, db *ydb.Driver) {
	readContext, stopReader := context.WithCancel(context.Background())
	defer stopReader()

	readStartPosition := func(
		ctx context.Context,
		req topicoptions.GetPartitionStartOffsetRequest,
	) (res topicoptions.GetPartitionStartOffsetResponse, err error) {
		offset, err := readLastOffsetFromDB(ctx, req.Topic, req.PartitionID)
		res.StartFrom(offset)

		// Reader will stop if return err != nil
		return res, err
	}

	onPartitionStart := func(
		info trace.TopicReaderPartitionReadStartResponseStartInfo,
	) func(
		trace.TopicReaderPartitionReadStartResponseDoneInfo,
	) {
		err := externalSystemLock(info.PartitionContext, info.Topic, info.PartitionID)
		if err != nil {
			stopReader()
		}
		return nil
	}

	onPartitionStop := func(
		info trace.TopicReaderPartitionReadStopResponseStartInfo,
	) func(
		trace.TopicReaderPartitionReadStopResponseDoneInfo,
	) {
		if info.Graceful {
			err := externalSystemUnlock(ctx, info.Topic, info.PartitionID)
			if err != nil {
				stopReader()
			}
		}
		return nil
	}

	r, _ := db.Topic().StartReader("consumer", topicoptions.ReadTopic("asd"),

		topicoptions.WithGetPartitionStartOffset(readStartPosition),
		topicoptions.WithReaderTrace(
			trace.Topic{
				OnReaderPartitionReadStartResponse: onPartitionStart,
				OnReaderPartitionReadStopResponse:  onPartitionStop,
			},
		),
	)
	go func() {
		<-readContext.Done()
		_ = r.Close(ctx)
	}()

	for {
		batch, _ := r.ReadMessageBatch(readContext)

		processBatch(batch.Context(), batch)
		_ = externalSystemCommit(batch.Context(), batch.Topic(), batch.PartitionID(), getEndOffset(batch))
	}
}
