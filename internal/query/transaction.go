package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Transaction = (*transaction)(nil)

type transaction struct {
	id string
}

func (t transaction) Execute(
	ctx context.Context, query string, opts ...query.TxExecuteOption,
) (r query.Result, err error) {
	//TODO implement me
	panic("implement me")
}

func (t transaction) CommitTx(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (t transaction) Rollback(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}
