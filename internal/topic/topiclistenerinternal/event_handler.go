package topiclistenerinternal

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

var ErrUnimplementedPublic = errors.New("unimplemented event handler method")

//go:generate mockgen -source event_handler.go -destination event_handler_mock_test.go -package topiclistenerinternal -write_package_comment=false --typed

type EventHandler interface {
	OnStartPartitionSessionRequest(ctx context.Context, event PublicStartPartitionSessionEvent) error
	OnReadMessages(ctx context.Context, req PublicReadMessages) error
	OnStopPartitionSessionRequest(ctx context.Context, event PublicStopPartitionSessionEvent) error
}

type PublicReadMessages struct {
	PartitionSessionID int64
	PartitionID        int64
	Batch              *topicreader.Batch
}

type PublicStartPartitionSessionEvent struct {
	PartitionSession PublicPartitionSession
	CommittedOffset  int64
	PartitionOffsets PublicOffsetsRange
	resp             PublicStartPartitionSessionResponse
	respChan         chan PublicStartPartitionSessionResponse
}

func (e *PublicStartPartitionSessionEvent) Confirm() {
	e.respChan <- e.resp
}

func (e *PublicStartPartitionSessionEvent) SetReadOffset(offset int64) *PublicStartPartitionSessionEvent {
	e.resp.ReadOffset = &offset
	return e
}

func (e *PublicStartPartitionSessionEvent) SetCommitOffset(offset int64) *PublicStartPartitionSessionEvent {
	e.resp.CommitOffset = &offset
	return e
}

type PublicStartPartitionSessionResponse struct {
	ReadOffset   *int64
	CommitOffset *int64 ``
}

type PublicPartitionSession struct {
	SessionID   int64
	TopicPath   string
	PartitionID int64
}

type PublicOffsetsRange struct {
	Start int64
	End   int64
}

type PublicStopPartitionSessionEvent struct {
	PartitionSessionID int64
	Graceful           bool
	CommittedOffset    int64
	resp               chan PublicStopPartitionSessionResponse
}

func (e *PublicStopPartitionSessionEvent) Confirm() {
	e.resp <- PublicStopPartitionSessionResponse{}
}

type PublicStopPartitionSessionResponse struct{}
