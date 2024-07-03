package topicreader

import "context"

type CallbackReader struct {
}

func (cr *CallbackReader) WaitInit() error {
	//TODO implement me
	panic("implement me")
}

func (cr *CallbackReader) Commit(ctx context.Context, batch CommitRangeGetter) error {
	//TODO implement me
	panic("implement me")
}

func (cr *CallbackReader) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

// EventHandler methods will be called sequentially by partition, but can be called in parallel for different partitions
type EventHandler interface {
	// topicReaderHandler needs for guarantee inherits from base struct with default implementations of new methods
	topicReaderHandler()

	OnReaderCreated(ctx context.Context, req ReaderReady) error
	OnStartPartitionSessionRequest(ctx context.Context, req StartPartitionSessionRequest) (StartPartitionSessionResponse, error)
	OnStopPartitionSessionRequest(ctx context.Context, req StopPartitionSessionRequest) (StopPartitionSessionResponse, error)
	OnReadMessages(ctx context.Context, req ReadMessages) error
}

type ReaderReady struct {
	Reader *CallbackReader
}

type StartPartitionSessionRequest struct {
	PartitionSession PartitionSession
	CommittedOffset  int64
	PartitionOffsets OffsetsRange
}

type StartPartitionSessionResponse struct {
	ReadOffset   *int64
	CommitOffset *int64
}

type StopPartitionSessionRequest struct {
	PartitionSessionID int64
	Graceful           bool
	CommittedOffset    int64
}

type StopPartitionSessionResponse struct {
}

type ReadMessages struct {
	PartitionSessionID int64
	Batch              *Batch
}

type BaseHandler struct {
}

func (b BaseHandler) topicReaderHandler() {}

func (b BaseHandler) OnReaderCreated(ctx context.Context, req ReaderReady) error {

}

func (b BaseHandler) OnStartPartitionSessionRequest(
	ctx context.Context,
	req StartPartitionSessionRequest,
) (StartPartitionSessionResponse, error) {
	return StartPartitionSessionResponse{}, nil
}

func (b BaseHandler) OnStopPartitionSessionRequest(
	ctx context.Context,
	req StopPartitionSessionRequest,
) (StopPartitionSessionResponse, error) {
	return StopPartitionSessionResponse{}, nil
}

func (b BaseHandler) OnReadMessages(
	ctx context.Context,
	req ReadMessages,
) error {
	return nil
}

type PartitionSession struct {
	SessionID   int64
	TopicPath   string
	PartitionID int64
}

type OffsetsRange struct {
	Start int64
	End   int64
}
