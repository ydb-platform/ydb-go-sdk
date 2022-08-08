package ydb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

var d = &sqlDriver{connectors: make(map[*xsql.Connector]struct{})}

func init() {
	sql.Register("ydb", d)
}

// Driver is an adapter to allow the use table client as conn.Driver instance.
type sqlDriver struct {
	connectors    map[*xsql.Connector]struct{}
	connectorsMtx sync.RWMutex
}

var (
	_ driver.Driver        = &sqlDriver{}
	_ driver.DriverContext = &sqlDriver{}
)

func (d *sqlDriver) Close() error {
	d.connectorsMtx.RLock()
	connectors := d.connectors
	d.connectorsMtx.RUnlock()
	var errs []error
	for c := range connectors {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return xerrors.NewWithIssues("ydb legacy driver close failed", errs...)
	}
	return nil
}

// Open returns a new connection to the ydb.
func (d *sqlDriver) Open(string) (driver.Conn, error) {
	return nil, xsql.ErrUnsupported
}

func (d *sqlDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	uri, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	var opts []Option
	if token := uri.Query().Get("token"); token != "" {
		opts = append(opts, WithAccessTokenCredentials(token))
	}
	db, err := Open(context.Background(), dataSourceName, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	var connectorOpts []xsql.ConnectorOption
	if queryMode := uri.Query().Get("go_default_query_mode"); queryMode != "" {
		mode := xsql.QueryModeFromString(queryMode)
		if mode == xsql.UnknownQueryMode {
			return nil, xerrors.WithStackTrace(fmt.Errorf("unknown query mode: %s", queryMode))
		}
		connectorOpts = append(connectorOpts, xsql.WithDefaultQueryMode(mode))
	}
	c, err := xsql.Open(d, db, connectorOpts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

func (d *sqlDriver) Attach(c *xsql.Connector) {
	d.connectorsMtx.Lock()
	d.connectors[c] = struct{}{}
	d.connectorsMtx.Unlock()
}

func (d *sqlDriver) Detach(c *xsql.Connector) {
	d.connectorsMtx.Lock()
	delete(d.connectors, c)
	d.connectorsMtx.Unlock()
}

type QueryMode = xsql.QueryMode

const (
	DataQueryMode      = xsql.DataQueryMode
	ExplainQueryMode   = xsql.ExplainQueryMode
	ScanQueryMode      = xsql.ScanQueryMode
	SchemeQueryMode    = xsql.SchemeQueryMode
	ScriptingQueryMode = xsql.ScriptingQueryMode
)

func WithQueryMode(ctx context.Context, mode QueryMode) context.Context {
	return xsql.WithQueryMode(ctx, mode)
}

type ConnectorOption = xsql.ConnectorOption

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
	c, err := xsql.Open(d, db)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

func Unwrap(db *sql.DB) (Connection, error) {
	c, err := xsql.Unwrap(db)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if cc, ok := c.Connection().(Connection); ok {
		return cc, nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("%+v is not a ydb.Nonnection", c.Connection()))
}
