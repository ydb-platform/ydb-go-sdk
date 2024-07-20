package topiclistenerinternal

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

var ErrUnimplementedPublic = errors.New("unimplemented event handler method")

//go:generate mockgen -source event_handler.go -destination event_handler_mock_test.go -package topiclistenerinternal -write_package_comment=false --typed

type EventHandler interface {
	OnStartPartitionSessionRequest(ctx context.Context, req PublicStartPartitionSessionRequest) (PublicStartPartitionSessionResponse, error)
	OnReadMessages(ctx context.Context, req PublicReadMessages) error
	OnStopPartitionSessionRequest(ctx context.Context, req PublicStopPartitionSessionRequest) (PublicStopPartitionSessionResponse, error)
}

type PublicReadMessages struct {
	PartitionSessionID int64
	Batch              *topicreader.Batch
}

type PublicStartPartitionSessionRequest struct {
	PartitionSession PublicPartitionSession
	CommittedOffset  int64
	PartitionOffsets PublicOffsetsRange
}

type PublicStartPartitionSessionResponse struct {
	ReadOffset   *int64
	CommitOffset *int64
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

type PublicStopPartitionSessionRequest struct {
	PartitionSessionID int64
	Graceful           bool
	CommittedOffset    int64
}

type PublicStopPartitionSessionResponse struct{}
