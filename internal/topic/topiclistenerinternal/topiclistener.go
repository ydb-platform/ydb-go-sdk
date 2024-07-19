package topiclistenerinternal

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"golang.org/x/net/context"
)

type Listener struct {
	ReadSelectors []*topicreadercommon.PublicReadSelector

	client rawtopic.Client
}

func (l *Listener) WaitInit(ctx context.Context) error {
	panic("not implemented yet")
}

func (l *Listener) Commit(ctx context.Context, offsets topicreadercommon.PublicCommitRangeGetter) {
	panic("not implemented yet")
}

func (l *Listener) CloseReason(ctx context.Context, reason error) error {
	panic("not implemented yet")
}
