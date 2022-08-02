package tx

import (
	"context"
	"database/sql/driver"
	"github.com/ydb-platform/ydb-go-sql/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type Tx interface {
	driver.Tx
	driver.QueryerContext
	driver.ExecerContext
}

func New(ctx context.Context, opts driver.TxOptions, s table.ClosableSession, close func()) (Tx, error) {
	isolation, control, err := isolationOrControl(opts)
	if err != nil {
		return nil, err
	}
	if isolation == nil {
		return &ro{
			s:     s,
			txc:   table.TxControl(control...),
			close: close,
		}, nil
	}
	tx, err := s.BeginTransaction(ctx, table.TxSettings(isolation))
	if err != nil {
		return nil, xerrors.Map(err)
	}
	return &rw{
		s:     s,
		tx:    tx,
		txc:   table.TxControl(append(control, table.WithTx(tx))...),
		close: close,
	}, nil
}
