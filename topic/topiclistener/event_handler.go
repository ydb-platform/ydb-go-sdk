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

	OnReaderCreated(ctx context.Context, req ReaderReady) error
}

type ReaderReady struct {
	Reader *TopicListener
}

type ReadMessages = topiclistenerinternal.PublicReadMessages

type BaseHandler struct{}

func (b BaseHandler) topicReaderHandler() {}

func (b BaseHandler) OnReaderCreated(ctx context.Context, req ReaderReady) error {
	panic("not implemented yet")
}

func (b BaseHandler) OnStartPartitionSessionRequest(
	ctx context.Context,
	req StartPartitionSessionRequest,
) (StartPartitionSessionResponse, error) {
	return StartPartitionSessionResponse{}, ErrMethodUnimplemented
}

// OnStopPartitionSessionRequest called when server want to stop send messages for the partition
// the method may be called more than once for partition session: with graceful shutdown and without
// no guarantee to call with graceful=false after graceful true
// it called with partition context if partition exists and with cancelled background context if
// not
func (b BaseHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	req StopPartitionSessionRequest,
) (StopPartitionSessionResponse, error) {
	return StopPartitionSessionResponse{}, ErrMethodUnimplemented
}

func (b BaseHandler) OnReadMessages(
	ctx context.Context,
	req ReadMessages,
) error {
	return ErrMethodUnimplemented
}

type StartPartitionSessionRequest = topiclistenerinternal.PublicStartPartitionSessionRequest
type StartPartitionSessionResponse = topiclistenerinternal.PublicStartPartitionSessionResponse
type StopPartitionSessionRequest = topiclistenerinternal.PublicStopPartitionSessionRequest
type StopPartitionSessionResponse = topiclistenerinternal.PublicStopPartitionSessionResponse

type PartitionSession = topiclistenerinternal.PublicPartitionSession

type OffsetsRange = topiclistenerinternal.PublicOffsetsRange
