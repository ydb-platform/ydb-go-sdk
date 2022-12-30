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

type XormQueryMode int

func (t XormQueryMode) String() string {
	if s, ok := modeToString[t]; ok {
		return s
	}
	return fmt.Sprintf("unknown_mode_%d", t)
}

const XormMetadataQuery string = "xorm-metadata-query"

const (
	UnknownXormMetadataQueryMode = XormQueryMode(iota)

	XormVersionQueryMode
	XormIsTableExistQueryMode
	XormIsColumnExistQueryMode
	XormGetColumnsQueryMode
	XormGetTablesQueryMode
	XormGetIndexesQueryMode
)

var (
	stringToMode = map[string]XormQueryMode{
		"version":         XormVersionQueryMode,
		"is-table-exist":  XormIsTableExistQueryMode,
		"is-column-exist": XormIsColumnExistQueryMode,
		"get-columns":     XormGetColumnsQueryMode,
		"get-tables":      XormGetTablesQueryMode,
		"get-indexes":     XormGetIndexesQueryMode,
	}

	modeToString = map[XormQueryMode]string{
		XormVersionQueryMode:       "version",
		XormIsTableExistQueryMode:  "is-table-exist",
		XormIsColumnExistQueryMode: "is-column-exist",
		XormGetColumnsQueryMode:    "get-columns",
		XormGetTablesQueryMode:     "get-tables",
		XormGetIndexesQueryMode:    "get-indexes",
	}
)

type xorm struct {
	ctx    context.Context
	conn   *conn
	params map[string]any
	query  string
	args   []driver.NamedValue
}

func newXorm(ctx context.Context, c *conn, query string, args []driver.NamedValue) *xorm {
	x := &xorm{
		ctx:    ctx,
		conn:   c,
		params: make(map[string]any),
		query:  query,
		args:   args,
	}
	for _, arg := range args {
		x.params[arg.Name] = arg.Value
	}
	return x
}

func xormModeFromContext(ctx context.Context) XormQueryMode {
	m := ctx.Value(XormMetadataQuery).(string)
	if r, ok := stringToMode[m]; ok {
		return r
	}
	return UnknownXormMetadataQueryMode
}

func (x *xorm) queryMetadata() (_ driver.Rows, err error) {
	m := xormModeFromContext(x.ctx)
	var (
		colNames []string
		res      xormResult
	)
	switch m {
	case XormVersionQueryMode:
	case XormIsTableExistQueryMode:
		colNames, res, err = x.IsTableExist()
	case XormIsColumnExistQueryMode:
		colNames, res, err = x.IsColumnExist()
	case XormGetColumnsQueryMode:
	case XormGetTablesQueryMode:
		colNames, res, err = x.GetTables()
	case XormGetIndexesQueryMode:
		colNames, res, err = x.GetIndexes()
	default:
		return nil, fmt.Errorf("unsupported xorm metadata querymode '%s'", m)
	}
	if err != nil {
		return nil, err
	}
	return &xormRows{
		columnNames: colNames,
		result:      res,
	}, nil
}

func (x *xorm) IsTableExist() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	var tableName string
	if err = value.CastTo(x.params["TableName"].(value.Value), &tableName); err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	exist, err := x.isTableExist(tableName)
	if err != nil {
		return nil, xormResult{}, err
	}

	columnNames = append(columnNames, "TableName")
	res.value = make([][]any, 0)

	if exist {
		var tableName string
		_ = value.CastTo(x.params["TableName"].(value.Value), &tableName)
		res.value = append(res.value, []any{tableName})
	}
	return
}

func (x *xorm) isTableExist(tableName string) (bool, error) {
	schemeClient := x.conn.connector.Connection().Scheme()

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

func (x *xorm) IsColumnExist() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	if _, has := x.params["ColumnName"]; !has {
		return nil, xormResult{}, fmt.Errorf("column name not found")
	}

	var tableName string
	if err = value.CastTo(x.params["TableName"].(value.Value), &tableName); err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	var columnName string
	if err = value.CastTo(x.params["ColumnName"].(value.Value), &columnName); err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	exist, err := x.isColumnExist(columnName, tableName)
	if err != nil {
		return nil, xormResult{}, err
	}

	columnNames = append(columnNames, "ColumnName")
	res.value = make([][]any, 0)

	if exist {
		var columnName string
		_ = value.CastTo(x.params["ColumnName"].(value.Value), &columnName)
		res.value = append(res.value, []any{columnName})
	}
	return columnNames, res, nil
}

func (x *xorm) isColumnExist(columnName, tableName string) (bool, error) {
	tableExist, err := x.isTableExist(tableName)
	if err != nil {
		return false, err
	}
	if !tableExist {
		return false, fmt.Errorf("table `%s` not exist", tableName)
	}

	columnExist := false
	_ = columnExist

	tableClient := x.conn.connector.Connection().Table()
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
	})
	if err != nil {
		return false, err
	}

	return columnExist, nil
}

func (x *xorm) GetTables() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["DatabaseName"]; !has {
		return nil, xormResult{}, fmt.Errorf("database name not found")
	}
	var dbName string
	if err := value.CastTo(x.params["DatabaseName"].(value.Value), &dbName); err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	columnNames = append(columnNames, "TableName")
	res.value = make([][]any, 0)

	canEnter := func(dir string) bool {
		return dir != ".sys" && dir != ".sys_health"
	}

	schemeClient := x.conn.connector.Connection().Scheme()

	queue := make([]string, 0)
	queue = append(queue, dbName)

	for st := 0; st < len(queue); st++ {
		curDir := queue[st]

		var e scheme.Entry
		_ = e
		if err := retry.Retry(x.ctx, func(ctx context.Context) (err error) {
			e, err = schemeClient.DescribePath(ctx, curDir)
			return
		}, retry.WithIdempotent(true)); err != nil {
			return nil, xormResult{}, err
		}
		if e.IsTable() {
			res.value = append(res.value, []any{curDir})
			continue
		}

		var d scheme.Directory
		_ = d
		if err := retry.Retry(x.ctx, func(ctx context.Context) (err error) {
			d, err = schemeClient.ListDirectory(ctx, curDir)
			return
		}, retry.WithIdempotent(true)); err != nil {
			return nil, xormResult{}, err
		}

		for _, child := range d.Children {
			if child.IsDirectory() || child.IsTable() {
				if canEnter(child.Name) {
					queue = append(queue, path.Join(curDir, child.Name))
				}
			}
		}
	}

	return columnNames, res, nil
}

func (x *xorm) GetIndexes() (columnNames []string, res xormResult, err error) {
	if _, has := x.params["TableName"]; !has {
		return nil, xormResult{}, fmt.Errorf("table name not found")
	}
	var tableName string
	if err = value.CastTo(x.params["TableName"].(value.Value), &tableName); err != nil {
		return nil, xormResult{}, x.conn.checkClosed(xerrors.WithStackTrace(err))
	}

	tableExist, err := x.isTableExist(tableName)
	if err != nil {
		return nil, xormResult{}, err
	}
	if !tableExist {
		return nil, xormResult{}, fmt.Errorf("table `%s` not exist", tableName)
	}

	columnNames = append(columnNames, "IndexName", "Columns")
	res.value = make([][]any, 0)

	tableClient := x.conn.connector.Connection().Table()
	err = tableClient.Do(x.ctx, func(ctx context.Context, session table.Session) (err error) {
		desc, err := session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for _, indexDesc := range desc.Indexes {
			res.value = append(res.value, []any{
				indexDesc.Name,
				strings.Join(indexDesc.IndexColumns, ","),
			})
		}
		return
	})
	if err != nil {
		return nil, xormResult{}, err
	}
	return columnNames, res, nil
}

type xormResult struct {
	rentRow int
	value   [][]any
}

type xormRows struct {
	columnNames []string
	result      xormResult
}

func (xr *xormRows) Columns() []string {
	return xr.columnNames
}

func (xr *xormRows) Close() error {
	return nil
}

func (xr *xormRows) Next(dst []driver.Value) error {
	if xr.result.rentRow >= len(xr.result.value) {
		return io.EOF
	}
	for i, v := range xr.result.value[xr.result.rentRow] {
		dst[i] = v
	}
	xr.result.rentRow++
	return nil
}
