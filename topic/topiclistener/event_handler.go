package topiclistener

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
)

var ErrMethodUnimplemented = topiclistenerinternal.ErrUnimplementedPublic

// EventHandler methods will be called sequentially by partition, but can be called in parallel for different partitions
type EventHandler interface {
	// topicReaderHandler needs for guarantee inherits from base struct with default implementations of new methods
	topicReaderHandler()

	topiclistenerinternal.EventHandler

	OnReaderCreated(req ReaderReady) error
}

type ReaderReady struct {
	Listener *TopicListener
}

type ReadMessages = topiclistenerinternal.PublicReadMessages

type BaseHandler struct{}

func (b BaseHandler) topicReaderHandler() {}

func (b BaseHandler) OnReaderCreated(req ReaderReady) error {
	return nil
}

func (b BaseHandler) OnStartPartitionSessionRequest(
	ctx context.Context,
	event StartPartitionSessionRequest,
) error {
	event.Confirm(StartPartitionSessionResponse{})
	return nil
}

// OnStopPartitionSessionRequest called when server want to stop send messages for the partition
// the method may be called more than once for partition session: with graceful shutdown and without
// no guarantee to call with graceful=false after graceful true
// it called with partition context if partition exists and with cancelled background context if
// not
func (b BaseHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	event StopPartitionSessionRequest,
) error {
	event.Confirm(StopPartitionSessionResponse{})
	return nil
}

func (b BaseHandler) OnReadMessages(
	ctx context.Context,
	req ReadMessages,
) error {
	return nil
}

type (
	StartPartitionSessionRequest  = topiclistenerinternal.PublicStartPartitionSessionEvent
	StartPartitionSessionResponse = topiclistenerinternal.PublicStartPartitionSessionResponse
	StopPartitionSessionRequest   = topiclistenerinternal.PublicStopPartitionSessionEvent
	StopPartitionSessionResponse  = topiclistenerinternal.PublicStopPartitionSessionResponse
)

type PartitionSession = topiclistenerinternal.PublicPartitionSession

type OffsetsRange = topiclistenerinternal.PublicOffsetsRange
