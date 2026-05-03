package xquery

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

type ctxIssuesHandlerKey struct{}

type IssuesOption struct {
	Callback func([]*Ydb_Issue.IssueMessage)
}

func WithIssuesHandler(ctx context.Context, callback func([]*Ydb_Issue.IssueMessage)) context.Context {
	return context.WithValue(ctx, ctxIssuesHandlerKey{}, &IssuesOption{Callback: callback})
}

func IssuesHandlerFromContext(ctx context.Context) *IssuesOption {
	if v, ok := ctx.Value(ctxIssuesHandlerKey{}).(*IssuesOption); ok {
		return v
	}

	return nil
}
