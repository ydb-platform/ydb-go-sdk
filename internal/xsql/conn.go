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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type connOption func(*conn)

func withFakeTxModes(modes ...QueryMode) connOption {
	return func(c *conn) {
		for _, m := range modes {
			c.beginTxFuncs[m] = c.beginTxFake
		}
	}
}

func withDataOpts(dataOpts ...options.ExecuteDataQueryOption) connOption {
	return func(c *conn) {
		c.dataOpts = dataOpts
	}
}

func withScanOpts(scanOpts ...options.ExecuteScanQueryOption) connOption {
	return func(c *conn) {
		c.scanOpts = scanOpts
	}
}

func withDefaultTxControl(defaultTxControl *table.TransactionControl) connOption {
	return func(c *conn) {
		c.defaultTxControl = defaultTxControl
	}
}

func withDefaultQueryMode(mode QueryMode) connOption {
	return func(c *conn) {
		c.defaultQueryMode = mode
	}
}

func withTrace(t *trace.DatabaseSQL) connOption {
	return func(c *conn) {
		c.trace = t
	}
}

type beginTxFunc func(ctx context.Context, txOptions driver.TxOptions) (currentTx, error)

type conn struct {
	openConnCtx context.Context

	connector *Connector
	trace     *trace.DatabaseSQL
	session   table.ClosableSession // Immutable and r/o usage.

	beginTxFuncs map[QueryMode]beginTxFunc

	closed           xatomic.Bool
	lastUsage        xatomic.Int64
	defaultQueryMode QueryMode

	defaultTxControl *table.TransactionControl
	dataOpts         []options.ExecuteDataQueryOption

	scanOpts []options.ExecuteScanQueryOption

	currentTx currentTx
}

func (c *conn) GetDatabaseName() string {
	return c.connector.parent.Name()
}

func (c *conn) CheckNamedValue(*driver.NamedValue) error {
	// on this stage allows all values
	return nil
}

func (c *conn) IsValid() bool {
	return c.isReady()
}

type currentTx interface {
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnPrepareContext
	table.TransactionIdentifier
}

type resultNoRows struct{}

func (resultNoRows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (resultNoRows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.Validator          = &conn{}
	_ driver.NamedValueChecker  = &conn{}

	_ driver.Result = resultNoRows{}
)

func newConn(ctx context.Context, c *Connector, s table.ClosableSession, opts ...connOption) *conn {
	cc := &conn{
		openConnCtx: ctx,
		connector:   c,
		session:     s,
	}
	cc.beginTxFuncs = map[QueryMode]beginTxFunc{
		DataQueryMode: cc.beginTx,
	}
	for _, o := range opts {
		if o != nil {
			o(cc)
		}
	}
	c.attach(cc)
	return cc
}

func (c *conn) isReady() bool {
	return c.session.Status() == table.SessionReady
}

func (c *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, finalErr error) {
	if c.currentTx != nil {
		return c.currentTx.PrepareContext(ctx, query)
	}
	onDone := trace.DatabaseSQLOnConnPrepare(c.trace, &ctx,
		stack.FunctionID(""),
		query,
	)
	defer func() {
		onDone(finalErr)
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	return &stmt{
		conn:      c,
		processor: c,
		stmtCtx:   ctx,
		query:     query,
		trace:     c.trace,
	}, nil
}

func (c *conn) sinceLastUsage() time.Duration {
	return time.Since(time.Unix(c.lastUsage.Load(), 0))
}

func (c *conn) execContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Result, finalErr error,
) {
	defer func() {
		c.lastUsage.Store(time.Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}

	var (
		m      = queryModeFromContext(ctx, c.defaultQueryMode)
		onDone = trace.DatabaseSQLOnConnExec(
			c.trace, &ctx,
			stack.FunctionID(""),
			query, m.String(), xcontext.IsIdempotent(ctx), c.sinceLastUsage(),
		)
	)
	defer func() {
		onDone(finalErr)
	}()

	switch m {
	case DataQueryMode:
		normalizedQuery, params, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		_, res, err := c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			normalizedQuery, params, c.dataQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = res.Close()
		}()
		if err = res.NextResultSetErr(ctx); !xerrors.Is(err, nil, io.EOF) {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return resultNoRows{}, nil
	case SchemeQueryMode:
		normalizedQuery, _, err := c.normalize(query)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		err = c.session.ExecuteSchemeQuery(ctx, normalizedQuery)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return resultNoRows{}, nil
	case ScriptingQueryMode:
		var (
			res    result.StreamResult
			params *table.QueryParameters
		)
		normalizedQuery, params, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err = c.connector.parent.Scripting().StreamExecute(ctx, normalizedQuery, params)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = res.Close()
		}()
		if err = res.NextResultSetErr(ctx); !xerrors.Is(err, nil, io.EOF) {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return resultNoRows{}, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	}
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}
	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, _ error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	defer func() {
		c.lastUsage.Store(time.Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}

	var (
		m      = queryModeFromContext(ctx, c.defaultQueryMode)
		onDone = trace.DatabaseSQLOnConnQuery(
			c.trace, &ctx,
			stack.FunctionID(""),
			query, m.String(), xcontext.IsIdempotent(ctx), c.sinceLastUsage(),
		)
	)
	defer func() {
		onDone(finalErr)
	}()

	switch m {
	case DataQueryMode:
		normalizedQuery, params, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		_, res, err := c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			normalizedQuery, params, c.dataQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
			result: res,
		}, nil
	case ScanQueryMode:
		normalizedQuery, params, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err := c.session.StreamExecuteScanQuery(ctx,
			normalizedQuery, params, c.scanQueryOptions(ctx)...,
		)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
			result: res,
		}, nil
	case ExplainQueryMode:
		normalizedQuery, _, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		exp, err := c.session.Explain(ctx, normalizedQuery)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &single{
			values: []sql.NamedArg{
				sql.Named("AST", exp.AST),
				sql.Named("Plan", exp.Plan),
			},
		}, nil
	case ScriptingQueryMode:
		normalizedQuery, params, err := c.normalize(query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err := c.connector.parent.Scripting().StreamExecute(ctx, normalizedQuery, params)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		if err = res.Err(); err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return &rows{
			conn:   c,
			result: res,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query mode '%s' on conn query", m)
	}
}

func (c *conn) Ping(ctx context.Context) (finalErr error) {
	onDone := trace.DatabaseSQLOnConnPing(c.trace, &ctx, stack.FunctionID(""))
	defer func() {
		onDone(finalErr)
	}()
	if !c.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if err := c.session.KeepAlive(ctx); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (c *conn) Close() (finalErr error) {
	if c.closed.CompareAndSwap(false, true) {
		c.connector.detach(c)
		onDone := trace.DatabaseSQLOnConnClose(
			c.trace, &c.openConnCtx,
			stack.FunctionID(""),
		)
		defer func() {
			onDone(finalErr)
		}()
		if c.currentTx != nil {
			_ = c.currentTx.Rollback()
		}
		err := c.session.Close(xcontext.WithoutDeadline(c.openConnCtx))
		if err != nil {
			return badconn.Map(xerrors.WithStackTrace(err))
		}
		return nil
	}
	return badconn.Map(xerrors.WithStackTrace(errConnClosedEarly))
}

func (c *conn) Prepare(string) (driver.Stmt, error) {
	return nil, errDeprecated
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}

func (c *conn) normalize(q string, args ...driver.NamedValue) (query string, _ *table.QueryParameters, _ error) {
	return c.connector.Bindings.RewriteQuery(q, func() (ii []interface{}) {
		for i := range args {
			ii = append(ii, args[i])
		}
		return ii
	}()...)
}

func (c *conn) ID() string {
	return c.session.ID()
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (_ driver.Tx, finalErr error) {
	var tx currentTx
	onDone := trace.DatabaseSQLOnConnBegin(c.trace, &ctx, stack.FunctionID(""))
	defer func() {
		onDone(tx, finalErr)
	}()

	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(
			xerrors.Retryable(
				&ErrConnAlreadyHaveTx{
					currentTx: c.currentTx.ID(),
				},
				xerrors.WithDeleteSession(),
			),
		)
	}

	m := queryModeFromContext(ctx, c.defaultQueryMode)

	beginTx, isKnown := c.beginTxFuncs[m]
	if !isKnown {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				xerrors.Retryable(
					fmt.Errorf("wrong query mode: %s", m.String()),
					xerrors.WithDeleteSession(),
					xerrors.WithName("WRONG_QUERY_MODE"),
				),
			),
		)
	}

	var err error
	tx, err = beginTx(ctx, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return tx, nil
}

func (c *conn) Version(_ context.Context) (_ string, _ error) {
	const version = "default"
	return version, nil
}

func (c *conn) IsTableExists(ctx context.Context, tableName string) (tableExists bool, finalErr error) {
	tableName = c.normalizePath(tableName)
	onDone := trace.DatabaseSQLOnConnIsTableExists(c.trace, &ctx,
		stack.FunctionID(""),
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

func (c *conn) IsColumnExists(ctx context.Context, tableName, columnName string) (columnExists bool, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return false, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for i := range desc.Columns {
			if desc.Columns[i].Name == columnName {
				columnExists = true
				break
			}
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	return columnExists, nil
}

func (c *conn) GetColumns(ctx context.Context, tableName string) (columns []string, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return nil, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for i := range desc.Columns {
			columns = append(columns, desc.Columns[i].Name)
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return columns, nil
}

func (c *conn) GetColumnType(ctx context.Context, tableName, columnName string) (dataType string, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return "", xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	columnExist, err := c.IsColumnExists(ctx, tableName, columnName)
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}
	if !columnExist {
		return "", xerrors.WithStackTrace(fmt.Errorf("column '%s' not exist in table '%s'", columnName, tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for i := range desc.Columns {
			if desc.Columns[i].Name == columnName {
				dataType = desc.Columns[i].Type.Yql()
				break
			}
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return "", xerrors.WithStackTrace(err)
	}
	return dataType, nil
}

func (c *conn) GetPrimaryKeys(ctx context.Context, tableName string) (pkCols []string, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return nil, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		pkCols = append(pkCols, desc.PrimaryKey...)
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return pkCols, nil
}

func (c *conn) IsPrimaryKey(ctx context.Context, tableName, columnName string) (ok bool, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return false, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	columnExist, err := c.IsColumnExists(ctx, tableName, columnName)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	if !columnExist {
		return false, xerrors.WithStackTrace(fmt.Errorf("column '%s' not exist in table '%s'", columnName, tableName))
	}

	pkCols, err := c.GetPrimaryKeys(ctx, tableName)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	for _, pkCol := range pkCols {
		if pkCol == columnName {
			ok = true
			break
		}
	}
	return ok, nil
}

func (c *conn) normalizePath(folderOrTable string) (absPath string) {
	return c.connector.pathNormalizer.NormalizePath(folderOrTable)
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

func (c *conn) getTables(ctx context.Context, absPath string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	if excludeSysDirs && isSysDir(c.connector.parent.Name(), absPath) {
		return nil, nil
	}

	var d scheme.Directory
	err := retry.Retry(ctx, func(ctx context.Context) (err error) {
		d, err = c.connector.parent.Scheme().ListDirectory(ctx, absPath)
		return err
	}, retry.WithIdempotent(true))
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

func (c *conn) GetTables(ctx context.Context, folder string, recursive, excludeSysDirs bool) (
	tables []string, _ error,
) {
	absPath := c.normalizePath(folder)

	var e scheme.Entry
	err := retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = c.connector.parent.Scheme().DescribePath(ctx, absPath)
		return err
	}, retry.WithIdempotent(true))
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

		for i := range tables {
			tables[i] = strings.TrimPrefix(tables[i], absPath+"/")
		}
		return tables, nil
	default:
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("'%s' is not a table or directory (%s)", folder, e.Type.String()),
		)
	}
}

func (c *conn) GetIndexes(ctx context.Context, tableName string) (indexes []string, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return nil, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for i := range desc.Indexes {
			indexes = append(indexes, desc.Indexes[i].Name)
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return indexes, nil
}

func (c *conn) GetIndexColumns(ctx context.Context, tableName, indexName string) (columns []string, _ error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsEntryExists(ctx,
		c.connector.parent.Scheme(), tableName,
		scheme.EntryTable, scheme.EntryColumnTable,
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if !tableExists {
		return nil, xerrors.WithStackTrace(fmt.Errorf("table '%s' not exist", tableName))
	}

	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		desc, err := c.session.DescribeTable(ctx, tableName)
		if err != nil {
			return err
		}
		for i := range desc.Indexes {
			if desc.Indexes[i].Name == indexName {
				columns = append(columns, desc.Indexes[i].IndexColumns...)
				return nil
			}
		}
		return xerrors.WithStackTrace(fmt.Errorf("index '%s' not found in table '%s'", indexName, tableName))
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return columns, nil
}
