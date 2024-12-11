package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	internalTable "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	_ driver.ConnBeginTx       = (*connWrapper)(nil)
	_ driver.NamedValueChecker = (*connWrapper)(nil)
	_ driver.Pinger            = (*connWrapper)(nil)
)

type (
	connWrapper struct {
		cc        conn.Conn
		currentTx *txWrapper

		connector *Connector
		lastUsage xsync.LastUsage
	}
	singleRow struct {
		values  []sql.NamedArg
		readAll bool
	}
)

func (c *connWrapper) Ping(ctx context.Context) error {
	return c.cc.Ping(ctx)
}

func (c *connWrapper) CheckNamedValue(value *driver.NamedValue) error {
	// on this stage allows all values
	return nil
}

func (c *connWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(xerrors.AlreadyHasTx(c.currentTx.ID()))
	}

	tx, err := c.cc.BeginTx(ctx, opts)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	c.currentTx = &txWrapper{
		conn: c,
		ctx:  ctx,
		tx:   tx,
	}

	return c.currentTx, nil
}

func (c *connWrapper) Close() error {
	err := c.cc.Close()
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *connWrapper) Begin() (driver.Tx, error) {
	return nil, xerrors.WithStackTrace(errDeprecated)
}

func rowByAstPlan(ast, plan string) *singleRow {
	return &singleRow{
		values: []sql.NamedArg{
			{
				Name:  "Ast",
				Value: ast,
			},
			{
				Name:  "Plan",
				Value: plan,
			},
		},
	}
}

func (r *singleRow) Columns() (columns []string) {
	for i := range r.values {
		columns = append(columns, r.values[i].Name)
	}

	return columns
}

func (r *singleRow) Close() error {
	return nil
}

func (r *singleRow) Next(dst []driver.Value) error {
	if r.values == nil || r.readAll {
		return io.EOF
	}
	for i := range r.values {
		dst[i] = r.values[i].Value
	}
	r.readAll = true

	return nil
}

func (c *connWrapper) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *connWrapper) PrepareContext(ctx context.Context, sql string) (_ driver.Stmt, finalErr error) {
	onDone := trace.DatabaseSQLOnConnPrepare(c.connector.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*connWrapper).PrepareContext"),
		sql,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !c.cc.IsValid() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	return &stmt{
		conn:      c,
		processor: c.cc,
		ctx:       ctx,
		sql:       sql,
	}, nil
}

func (c *connWrapper) LastUsage() time.Time {
	return c.lastUsage.Get()
}

func (c *connWrapper) toYdb(sql string, args ...driver.NamedValue) (yql string, _ *params.Params, _ error) {
	queryArgs := make([]any, len(args))
	for i := range args {
		queryArgs[i] = args[i]
	}

	yql, params, err := c.connector.Bindings().ToYdb(sql, queryArgs...)
	if err != nil {
		return "", nil, xerrors.WithStackTrace(err)
	}

	return yql, &params, nil
}

func (c *connWrapper) QueryContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Rows, error) {
	done := c.lastUsage.Start()
	defer done()

	sql, params, err := c.toYdb(sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if isExplain(ctx) {
		ast, plan, err := c.cc.Explain(ctx, sql, params)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return rowByAstPlan(ast, plan), nil
	}

	if c.currentTx != nil {
		return c.currentTx.tx.Query(ctx, sql, params)
	}

	return c.cc.Query(ctx, sql, params)
}

func (c *connWrapper) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	done := c.lastUsage.Start()
	defer done()

	sql, params, err := c.toYdb(sql, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if c.currentTx != nil {
		return c.currentTx.tx.Exec(ctx, sql, params)
	}

	return c.cc.Exec(ctx, sql, params)
}

func (c *connWrapper) GetDatabaseName() string {
	return c.connector.Name()
}

func (c *connWrapper) normalizePath(tableName string) string {
	return c.connector.pathNormalizer.NormalizePath(tableName)
}

func (c *connWrapper) tableDescription(ctx context.Context, tableName string) (d options.Description, _ error) {
	d, err := retry.RetryWithResult(ctx, func(ctx context.Context) (options.Description, error) {
		return internalTable.Session(c.cc.ID(), c.connector.balancer, config.New()).DescribeTable(ctx, tableName)
	}, retry.WithIdempotent(true), retry.WithBudget(c.connector.retryBudget), retry.WithTrace(c.connector.traceRetry))
	if err != nil {
		return d, xerrors.WithStackTrace(err)
	}

	return d, nil
}

func (c *connWrapper) GetColumns(ctx context.Context, tableName string) (columns []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		columns = append(columns, d.Columns[i].Name)
	}

	return columns, nil
}

func (c *connWrapper) GetColumnType(ctx context.Context, tableName, columnName string) (dataType string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		if d.Columns[i].Name == columnName {
			return d.Columns[i].Type.Yql(), nil
		}
	}

	return "", xerrors.WithStackTrace(fmt.Errorf("column '%s' not exist in table '%s'", columnName, tableName))
}

func (c *connWrapper) GetPrimaryKeys(ctx context.Context, tableName string) ([]string, error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return d.PrimaryKey, nil
}

func (c *connWrapper) IsPrimaryKey(ctx context.Context, tableName, columnName string) (ok bool, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	for _, pkCol := range d.PrimaryKey {
		if pkCol == columnName {
			return true, nil
		}
	}

	return false, nil
}

func (c *connWrapper) Version(_ context.Context) (_ string, _ error) {
	const version = "default"

	return version, nil
}

func isSysDir(databaseName, dirAbsPath string) bool {
	for _, sysDir := range [...]string{
		path.Join(databaseName, ".sys"),
		path.Join(databaseName, ".sys_health"),
	} {
		if strings.HasPrefix(dirAbsPath, sysDir) {
			return true
		}
	}

	return false
}

func (c *connWrapper) getTables(ctx context.Context, absPath string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	if excludeSysDirs && isSysDir(c.connector.Name(), absPath) {
		return nil, nil
	}

	d, err := c.connector.Scheme().ListDirectory(ctx, absPath)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if !d.IsDirectory() && !d.IsDatabase() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("'%s' is not a folder", absPath))
	}

	for i := range d.Children {
		switch d.Children[i].Type {
		case scheme.EntryTable, scheme.EntryColumnTable:
			tables = append(tables, path.Join(absPath, d.Children[i].Name))
		case scheme.EntryDirectory, scheme.EntryDatabase:
			if recursive {
				childTables, err := c.getTables(ctx, path.Join(absPath, d.Children[i].Name), recursive, excludeSysDirs)
				if err != nil {
					return nil, xerrors.WithStackTrace(err)
				}
				tables = append(tables, childTables...)
			}
		}
	}

	return tables, nil
}

func (c *connWrapper) GetTables(ctx context.Context, folder string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	absPath := c.normalizePath(folder)

	e, err := c.connector.Scheme().DescribePath(ctx, absPath)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch e.Type {
	case scheme.EntryTable, scheme.EntryColumnTable:
		return []string{e.Name}, err
	case scheme.EntryDirectory, scheme.EntryDatabase:
		tables, err = c.getTables(ctx, absPath, recursive, excludeSysDirs)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return xslices.Transform(tables, func(tbl string) string {
			return strings.TrimPrefix(tbl, absPath+"/")
		}), nil
	default:
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("'%s' is not a table or directory (%s)", folder, e.Type.String()),
		)
	}
}

func (c *connWrapper) GetIndexes(ctx context.Context, tableName string) (indexes []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return xslices.Transform(d.Indexes, func(idx options.IndexDescription) string {
		return idx.Name
	}), nil
}

func (c *connWrapper) GetIndexColumns(ctx context.Context, tableName, indexName string) (columns []string, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	for i := range d.Indexes {
		if d.Indexes[i].Name == indexName {
			columns = append(columns, d.Indexes[i].IndexColumns...)
		}
	}

	return xslices.Uniq(columns), nil
}

func (c *connWrapper) IsColumnExists(ctx context.Context, tableName, columnName string) (columnExists bool, _ error) {
	d, err := c.tableDescription(ctx, c.normalizePath(tableName))
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	for i := range d.Columns {
		if d.Columns[i].Name == columnName {
			return true, nil
		}
	}

	return false, nil
}

func (c *connWrapper) IsTableExists(ctx context.Context, tableName string) (tableExists bool, finalErr error) {
	tableName = c.normalizePath(tableName)
	onDone := trace.DatabaseSQLOnConnIsTableExists(c.connector.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql.(*connWrapper).IsTableExists"),
		tableName,
	)
	defer func() {
		onDone(tableExists, finalErr)
	}()

	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}

	return tableExists, nil
}
