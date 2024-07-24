package topicreaderexamples

import (
	"context"
	"log"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func startReader(ctx context.Context, db *ydb.Driver) (*topiclistener.TopicListener, error) {
	handler := &TopicEventsHandler{
		locks: make(map[int64]int64),
	}

	reader, err := db.Topic().StartListener("consumer", handler, topicoptions.ReadTopic("my-topic"))
	if err != nil {
		return nil, err
	}
	if err = reader.WaitInit(ctx); err != nil {
		return nil, err
	}

	err = reader.Close(ctx)

	return reader, nil
}

type TopicEventsHandler struct {
	topiclistener.BaseHandler
	listener *topiclistener.TopicListener

	m     sync.Mutex
	locks map[int64]int64 // [partitionSessionID]lockID
}

func (h *TopicEventsHandler) OnReaderCreated(req topiclistener.ReaderReady) error {
	h.listener = req.Listener
	return nil
}

func (h *TopicEventsHandler) OnReadMessages(
	ctx context.Context,
	event topiclistener.ReadMessages,
) error {
	for _, mess := range event.Batch.Messages {
		log.Println("Receive message: %v/%v/%v", mess.Topic(), mess.PartitionID(), mess.SeqNo)
	}
	_ = h.listener.Commit(ctx, event.Batch)
	return nil
}

func (h *TopicEventsHandler) OnStartPartitionSessionRequest(
	ctx context.Context,
	event topiclistener.StartPartitionSessionRequest,
) error {
	lockID, offset, err := lockPartition(ctx, event.PartitionSession.TopicPath, event.PartitionSession.PartitionID)

	h.m.Lock()
	h.locks[event.PartitionSession.SessionID] = lockID
	h.m.Unlock()

	log.Printf("Started read partition %v/%v", event.PartitionSession.TopicPath, event.PartitionSession.PartitionID)
	event.Confirm(
		topiclistener.StartPartitionSessionConfirm{}.
			WithReadOffet(offset).
			WithCommitOffset(offset),
	)
	return err
}

func (h *TopicEventsHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	event topiclistener.StopPartitionSessionRequest,
) error {
	h.m.Lock()
	lockID := h.locks[event.PartitionSessionID]
	delete(h.locks, event.PartitionSessionID)
	h.m.Unlock()

	err := unlockPartition(ctx, lockID)
	event.Confirm()
	return err
}

func lockPartition(ctx context.Context, topic string, partitionID int64) (lockID, offset int64, err error) {
	// TODO implement me
	panic("implement me")
}

func unlockPartition(ctx context.Context, lockID int64) error {
	// TODO implement me
	panic("implement me")
}
