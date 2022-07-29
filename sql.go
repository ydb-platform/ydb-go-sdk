package ydb

import (
	"database/sql"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var (
	ydbDriver = xsql.Driver()
)

func init() {
	sql.Register("ydb", ydbDriver)
}

type ConnectorOption = xsql.ConnectorOption

type QueryMode = xsql.QueryMode

const (
	QueryModeData      = xsql.QueryModeData
	QueryModeExplain   = xsql.QueryModeExplain
	QueryModeScan      = xsql.QueryModeScan
	QueryModeScheme    = xsql.QueryModeScheme
	QueryModeScripting = xsql.QueryModeScripting
)

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return xsql.WithDefaultQueryMode(mode)
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return xsql.WithDefaultTxControl(txControl)
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return xsql.WithDefaultDataQueryOptions(opts...)
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return xsql.WithDefaultScanQueryOptions(opts...)
}

func Connector(db Connection, opts ...ConnectorOption) (*xsql.Connector, error) {
	c, err := xsql.Open(ydbDriver, append([]ConnectorOption{xsql.WithConnection(db)}, opts...)...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}
