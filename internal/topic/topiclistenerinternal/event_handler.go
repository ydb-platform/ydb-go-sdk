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
	resp             chan WithError[PublicStartPartitionSessionResponse]
}

func (e PublicStartPartitionSessionEvent) Confirm(resp PublicStartPartitionSessionResponse, err error) {
	e.resp <- WithError[PublicStartPartitionSessionResponse]{
		Error: err,
		Val:   resp,
	}
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
	resp               chan WithError[PublicStopPartitionSessionResponse]
}

func (e *PublicStopPartitionSessionEvent) Confirm(resp PublicStopPartitionSessionResponse, err error) {
	e.resp <- WithError[PublicStopPartitionSessionResponse]{
		Error: err,
		Val:   resp,
	}
}

type PublicStopPartitionSessionResponse struct{}

type WithError[S any] struct {
	Error error
	Val   S
}
