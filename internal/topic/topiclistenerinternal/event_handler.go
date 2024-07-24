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
	resp             PublicStartPartitionSessionConfirm
	respChan         chan PublicStartPartitionSessionConfirm
}

func (e *PublicStartPartitionSessionEvent) Confirm(opts ...PublicStartPartitionSessionConfirm) {
	switch len(opts) {
	case 0:
		e.respChan <- PublicStartPartitionSessionConfirm{}
	case 1:
		e.respChan <- opts[0]
	default:
		panic("Confirm accept only zero or one confirm parameters")
	}
}

type PublicStartPartitionSessionConfirm struct {
	ReadOffset   *int64
	CommitOffset *int64 ``
}

func (c PublicStartPartitionSessionConfirm) WithReadOffet(val int64) PublicStartPartitionSessionConfirm {
	c.ReadOffset = &val
	return c
}

func (c PublicStartPartitionSessionConfirm) WithCommitOffset(val int64) PublicStartPartitionSessionConfirm {
	c.CommitOffset = &val
	return c
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
	resp               chan PublicStopPartitionSessionConfirm
}

func (e *PublicStopPartitionSessionEvent) Confirm(options ...PublicStopPartitionSessionConfirm) {
	switch len(options) {
	case 0:
		e.resp <- PublicStopPartitionSessionConfirm{}
	case 1:
		e.resp <- options[0]
	default:
		panic("Confirm accept only zero or one confirm parameters")
	}

}

type PublicStopPartitionSessionConfirm struct{}
