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
	req topiclistener.ReadMessages,
) error {
	for _, mess := range req.Batch.Messages {
		log.Println("Receive message: %v/%v/%v", mess.Topic(), mess.PartitionID(), mess.SeqNo)
	}
	_ = h.listener.Commit(ctx, req.Batch)
	return nil
}

func (h *TopicEventsHandler) OnStartPartitionSessionRequest(
	ctx context.Context,
	req topiclistener.StartPartitionSessionRequest,
) (topiclistener.StartPartitionSessionResponse, error) {
	lockID, offset, err := lockPartition(ctx, req.PartitionSession.TopicPath, req.PartitionSession.PartitionID)

	h.m.Lock()
	h.locks[req.PartitionSession.SessionID] = lockID
	h.m.Unlock()

	log.Printf("Started read partition %v/%v", req.PartitionSession.TopicPath, req.PartitionSession.PartitionID)
	return topiclistener.StartPartitionSessionResponse{
		ReadOffset: &offset,
	}, err
}

func (h *TopicEventsHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	req topiclistener.StopPartitionSessionRequest,
) (topiclistener.StopPartitionSessionResponse, error) {
	h.m.Lock()
	lockID := h.locks[req.PartitionSessionID]
	delete(h.locks, req.PartitionSessionID)
	h.m.Unlock()

	err := unlockPartition(ctx, lockID)
	return topiclistener.StopPartitionSessionResponse{}, err
}

func lockPartition(ctx context.Context, topic string, partitionID int64) (lockID, offset int64, err error) {
	// TODO implement me
	panic("implement me")
}

func unlockPartition(ctx context.Context, lockID int64) error {
	// TODO implement me
	panic("implement me")
}
