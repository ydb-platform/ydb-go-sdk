package xsql

import (
	"database/sql/driver"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func Driver() *ydbDriver {
	return &ydbDriver{
		connectors: make(map[*Connector]struct{}),
	}
}

var (
	_ driver.Driver        = &ydbDriver{}
	_ driver.DriverContext = &ydbDriver{}
	_ io.Closer            = &ydbDriver{}
)

// Driver is an adapter to allow the use table client as conn.Driver instance.
type ydbDriver struct {
	connectors    map[*Connector]struct{}
	connectorsMtx sync.RWMutex
}

func (d *ydbDriver) Close() error {
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
func (d *ydbDriver) Open(string) (driver.Conn, error) {
	return nil, ErrUnsupported
}

func (d *ydbDriver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	c, err := Open(d,
		WithDataSourceName(dataSourceName),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return c, nil
}

func (d *ydbDriver) attachConnector(c *Connector) {
	d.connectorsMtx.Lock()
	d.connectors[c] = struct{}{}
	d.connectorsMtx.Unlock()
}

func (d *ydbDriver) detachConnector(c *Connector) {
	d.connectorsMtx.Lock()
	delete(d.connectors, c)
	d.connectorsMtx.Unlock()
}
