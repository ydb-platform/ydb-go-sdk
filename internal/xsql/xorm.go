package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type xormQueryMode int

func (t xormQueryMode) String() string {
	if s, ok := modeToString[t]; ok {
		return s
	}
	return fmt.Sprintf("unknown_mode_%d", t)
}

const xormMetadataQuery string = "xorm-metadata-query"

const (
	unknownXormMetadataQueryMode = xormQueryMode(iota)

	xormVersionQueryMode
	xormIsTableExistQueryMode
	xormIsColumnExistQueryMode
	xormGetColumnsQueryMode
	xormGetTablesQueryMode
	xormGetIndexesQueryMode
)

var (
	stringToMode = map[string]xormQueryMode{
		"version":         xormVersionQueryMode,
		"is-table-exist":  xormIsTableExistQueryMode,
		"is-column-exist": xormIsColumnExistQueryMode,
		"get-columns":     xormGetColumnsQueryMode,
		"get-tables":      xormGetTablesQueryMode,
		"get-indexes":     xormGetIndexesQueryMode,
	}

	modeToString = map[xormQueryMode]string{
		xormVersionQueryMode:       "version",
		xormIsTableExistQueryMode:  "is-table-exist",
		xormIsColumnExistQueryMode: "is-column-exist",
		xormGetColumnsQueryMode:    "get-columns",
		xormGetTablesQueryMode:     "get-tables",
		xormGetIndexesQueryMode:    "get-indexes",
	}
)

type xorm struct {
	ctx    context.Context
	conn   *conn
	params map[string]value.Value
	query  string
	args   []driver.NamedValue
}

func newXorm(ctx context.Context, c *conn, query string, args []driver.NamedValue) *xorm {
	x := &xorm{
		ctx:    ctx,
		conn:   c,
		params: make(map[string]value.Value),
		query:  query,
		args:   args,
	}
	for _, arg := range args {
		x.params[arg.Name] = arg.Value.(value.Value)
	}
	return x
}

func checkXormQueryFromContext(ctx context.Context) bool {
	if _, ok := ctx.Value(xormMetadataQuery).(string); ok {
		return true
	}
	return false
}

func xormModeFromContext(ctx context.Context) xormQueryMode {
	m := ctx.Value(xormMetadataQuery).(string)
	if r, ok := stringToMode[m]; ok {
		return r
	}
	return unknownXormMetadataQueryMode
}

func (x *xorm) getConnector() *Connector {
	return x.conn.connector
}

func (x *xorm) queryMetadata() (_ driver.Rows, err error) {
	m := xormModeFromContext(x.ctx)
	var (
		colNames []string
		res      xormResult
	)
	switch m {
	case xormVersionQueryMode:
	case xormIsTableExistQueryMode:
		colNames, res, err = x.isTableExist()
	case xormIsColumnExistQueryMode:
		colNames, res, err = x.isColumnExist()
	case xormGetColumnsQueryMode:
		colNames, res, err = x.getColumns()
	case xormGetTablesQueryMode:
		colNames, res, err = x.getTables()
	case xormGetIndexesQueryMode:
		colNames, res, err = x.getIndexes()
	default:
		return nil, fmt.Errorf("unsupported xorm metadata querymode '%s'", m)
	}
	if err != nil {
		return nil, err
	}
	return &xormRows{
		columnNames: colNames,
		xormResult:  res,
	}, nil
}

func (x *xorm) isTableExist() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	var tableName string
	if err = value.CastTo(x.params["TableName"], &tableName); err != nil {
		return nil, xormResult{}, err
	}

	exist, err := x.checkTableExist(tableName)
	if err != nil {
		return nil, xormResult{}, err
	}

	columnNames = append(columnNames, "TableName")
	res.values = make([][]any, 0)

	if exist {
		res.values = append(res.values, []any{tableName})
	}
	return
}

func (x *xorm) checkTableExist(tableName string) (bool, error) {
	schemeClient := x.getConnector().Connection().Scheme()

	var e scheme.Entry
	err := retry.Retry(x.ctx, func(ctx context.Context) (err error) {
		e, err = schemeClient.DescribePath(ctx, tableName)
		return
	}, retry.WithIdempotent(true))

	if xerrors.IsOperationError(err, Ydb.StatusIds_SCHEME_ERROR) {
		return false, nil
	}

	if err != nil {
		return false, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return e.IsTable(), nil
}

func (x *xorm) isColumnExist() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	if _, has := x.params["ColumnName"]; !has {
		return nil, xormResult{}, fmt.Errorf("column name not found")
	}

	var tableName string
	if err = value.CastTo(x.params["TableName"], &tableName); err != nil {
		return nil, xormResult{}, err
	}

	var columnName string
	if err = value.CastTo(x.params["ColumnName"], &columnName); err != nil {
		return nil, xormResult{}, err
	}

	exist, err := x.checkColumnExist(columnName, tableName)
	if err != nil {
		return nil, xormResult{}, err
	}

	columnNames = append(columnNames, "ColumnName")
	res.values = make([][]any, 0)

	if exist {
		res.values = append(res.values, []any{columnName})
	}
	return columnNames, res, nil
}

func (x *xorm) checkColumnExist(columnName, tableName string) (bool, error) {
	tableExist, err := x.checkTableExist(tableName)
	if err != nil {
		return false, err
	}
	if !tableExist {
		return false, fmt.Errorf("table `%s` not exist", tableName)
	}

	columnExist := false

	tableClient := x.getConnector().Connection().Table()
	err = tableClient.Do(x.ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return
		}
		for _, col := range desc.Columns {
			if col.Name == columnName {
				columnExist = true
				break
			}
		}
		return
	}, table.WithIdempotent())
	if err != nil {
		return false, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	return columnExist, nil
}

func (x *xorm) getColumns() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}

	var tableName string
	if err = value.CastTo(x.params["TableName"], &tableName); err != nil {
		return nil, xormResult{}, err
	}

	tableExist, err := x.checkTableExist(tableName)
	if err != nil {
		return nil, xormResult{}, err
	}
	if !tableExist {
		return nil, xormResult{}, fmt.Errorf("table `%s` not exist", tableName)
	}

	columnNames = append(columnNames, "ColumnName", "TableName", "DataType", "IsPrimaryKey")
	res.values = make([][]any, 0)

	tableClient := x.getConnector().Connection().Table()
	err = tableClient.Do(x.ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}

		isPk := make(map[string]bool)
		for _, pk := range desc.PrimaryKey {
			isPk[pk] = true
		}

		for _, col := range desc.Columns {
			res.values = append(res.values, []any{
				col.Name,
				tableName,
				col.Type.Yql(),
				isPk[col.Name],
			})
		}
		return
	}, table.WithIdempotent())
	if err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	return columnNames, res, nil
}

func (x *xorm) getTables() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["DatabaseName"]; !has {
		return nil, xormResult{}, fmt.Errorf("database name not found")
	}
	var dbName string
	if err = value.CastTo(x.params["DatabaseName"], &dbName); err != nil {
		return nil, xormResult{}, err
	}

	ignoreDirs := map[string]bool{
		".sys":        true,
		".sys_health": true,
	}

	if _, has := x.params["IgnoreDirs"]; has {
		li := make([]string, 0)
		if err = value.CastTo(x.params["IgnoreDirs"], &li); err == nil {
			for _, v := range li {
				ignoreDirs[v] = true
			}
		}
	}

	columnNames = append(columnNames, "TableName")
	res.values = make([][]any, 0)

	canEnter := func(dir string) bool {
		if _, ignore := ignoreDirs[dir]; ignore {
			return false
		}
		return true
	}

	schemeClient := x.getConnector().Connection().Scheme()

	queue := make([]string, 0)
	queue = append(queue, dbName)

	for st := 0; st < len(queue); st++ {
		curDir := queue[st]

		var e scheme.Entry
		err = retry.Retry(x.ctx, func(ctx context.Context) (err error) {
			e, err = schemeClient.DescribePath(ctx, curDir)
			return
		}, retry.WithIdempotent(true))

		if err != nil {
			return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
		}

		if e.IsTable() {
			res.values = append(res.values, []any{curDir})
			continue
		}

		var d scheme.Directory
		err = retry.Retry(x.ctx, func(ctx context.Context) (err error) {
			d, err = schemeClient.ListDirectory(ctx, curDir)
			return
		}, retry.WithIdempotent(true))

		if err != nil {
			return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
		}

		for _, child := range d.Children {
			if child.IsDirectory() || child.IsDatabase() || child.IsTable() {
				if canEnter(child.Name) {
					queue = append(queue, path.Join(curDir, child.Name))
				}
			}
		}
	}

	return columnNames, res, nil
}

func (x *xorm) getIndexes() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	var tableName string
	if err = value.CastTo(x.params["TableName"], &tableName); err != nil {
		return nil, xormResult{}, err
	}

	tableExist, err := x.checkTableExist(tableName)
	if err != nil {
		return nil, xormResult{}, err
	}
	if !tableExist {
		return nil, xormResult{}, fmt.Errorf("table `%s` not exist", tableName)
	}

	columnNames = append(columnNames, "IndexName", "Columns")
	res.values = make([][]any, 0)

	tableClient := x.getConnector().Connection().Table()
	err = tableClient.Do(x.ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for _, indexDesc := range desc.Indexes {
			res.values = append(res.values, []any{
				indexDesc.Name,
				strings.Join(indexDesc.IndexColumns, ","),
			})
		}
		return
	}, table.WithIdempotent())
	if err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return columnNames, res, nil
}

type xormResult struct {
	currentRow int
	values     [][]any
}

type xormRows struct {
	xormResult
	columnNames []string
}

var _ driver.Rows = &xormRows{}

func (xr *xormRows) Columns() []string {
	ret := make([]string, len(xr.columnNames))
	copy(ret, xr.columnNames)
	return ret
}

func (xr *xormRows) Close() error {
	return nil
}

func (xr *xormRows) Next(dst []driver.Value) error {
	if xr.currentRow >= len(xr.values) {
		return io.EOF
	}
	var (
		cur    = xr.currentRow
		values = xr.values[cur]
	)
	for i := range values {
		dst[i] = values[i]
	}
	xr.currentRow++
	return nil
}
