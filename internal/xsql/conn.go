package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type connOption func(*conn)

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

func withTablePathPrefix(tablePathPrefix string) connOption {
	return func(c *conn) {
		c.tablePathPrefix = tablePathPrefix
	}
}

func withDefaultQueryMode(mode QueryMode) connOption {
	return func(c *conn) {
		c.defaultQueryMode = mode
	}
}

func withTrace(t trace.DatabaseSQL) connOption {
	return func(c *conn) {
		c.trace = t
	}
}

type conn struct {
	connector *Connector
	trace     trace.DatabaseSQL
	session   table.ClosableSession // Immutable and r/o usage.

	closed           uint32
	lastUsage        int64
	defaultQueryMode QueryMode
	tablePathPrefix  string

	defaultTxControl *table.TransactionControl
	dataOpts         []options.ExecuteDataQueryOption

	scanOpts []options.ExecuteScanQueryOption

	currentTx currentTx
}

func (c *conn) GetDatabaseName() string {
	return c.connector.parent.Name()
}

func (c *conn) TablePathPrefix() string {
	if c.tablePathPrefix == "" {
		return ""
	}
	return path.Join(c.connector.parent.Name(), c.tablePathPrefix)
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

func newConn(c *Connector, s table.ClosableSession, opts ...connOption) *conn {
	cc := &conn{
		connector: c,
		session:   s,
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

func (c *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	onDone := trace.DatabaseSQLOnConnPrepare(c.trace, &ctx, query)
	defer func() {
		onDone(err)
	}()
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	return &stmt{
		conn:  c,
		query: query,
		trace: c.trace,
	}, nil
}

func (c *conn) sinceLastUsage() time.Duration {
	return time.Since(time.Unix(atomic.LoadInt64(&c.lastUsage), 0))
}

func (c *conn) execContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	var (
		m      = queryModeFromContext(ctx, c.defaultQueryMode)
		onDone = trace.DatabaseSQLOnConnExec(c.trace, &ctx, query, m.String(), xcontext.IsIdempotent(ctx), c.sinceLastUsage())
	)

	defer func() {
		atomic.StoreInt64(&c.lastUsage, time.Now().Unix())
		onDone(err)
	}()
	switch m {
	case DataQueryMode:
		var (
			res    result.Result
			params *table.QueryParameters
		)
		query, params, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		_, res, err = c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			query, params, c.dataQueryOptions(ctx)...,
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
		query, _, err = c.nativeQueryAndParameters(ctx, query)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		err = c.session.ExecuteSchemeQuery(ctx, query)
		if err != nil {
			return nil, badconn.Map(xerrors.WithStackTrace(err))
		}
		return resultNoRows{}, nil
	case ScriptingQueryMode:
		var (
			res    result.StreamResult
			params *table.QueryParameters
		)
		query, params, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err = c.connector.parent.Scripting().StreamExecute(ctx, query, params)
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

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.ExecContext(ctx, query, args)
	}
	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return c.currentTx.QueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnQuery(
		c.trace,
		&ctx,
		query,
		m.String(),
		xcontext.IsIdempotent(ctx),
		c.sinceLastUsage(),
	)
	defer func() {
		atomic.StoreInt64(&c.lastUsage, time.Now().Unix())
		onDone(err)
	}()
	switch m {
	case DataQueryMode:
		var (
			res    result.Result
			params *table.QueryParameters
		)
		query, params, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		_, res, err = c.session.Execute(ctx,
			txControl(ctx, c.defaultTxControl),
			query, params, c.dataQueryOptions(ctx)...,
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
		var (
			res    result.StreamResult
			params *table.QueryParameters
		)
		query, params, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err = c.session.StreamExecuteScanQuery(ctx,
			query, params, c.scanQueryOptions(ctx)...,
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
		var exp table.DataQueryExplanation
		query, _, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		exp, err = c.session.Explain(ctx, query)
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
		var (
			res    result.StreamResult
			params *table.QueryParameters
		)
		query, params, err = c.nativeQueryAndParameters(ctx, query, args...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		res, err = c.connector.parent.Scripting().StreamExecute(ctx, query, params)
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

func (c *conn) Ping(ctx context.Context) (err error) {
	onDone := trace.DatabaseSQLOnConnPing(c.trace, &ctx)
	defer func() {
		onDone(err)
	}()
	if !c.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if err = c.session.KeepAlive(ctx); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (c *conn) Close() (err error) {
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.connector.detach(c)
		onDone := trace.DatabaseSQLOnConnClose(c.trace)
		defer func() {
			onDone(err)
		}()
		err = c.session.Close(context.Background())
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

var declareRe = regexp.MustCompile(`[dD][eE][cC][lL][aA][rR][eE]\s+\$(\S+)+\s+[aA][sS]\s+(\S+)+\s*;`)

func hasDeclare(query string) bool {
	return declareRe.MatchString(query)
}

var pragmaTablePathPrefixRe = regexp.MustCompile(`PRAGMA\s+TablePathPrefix\(\s*"[^"]+"\s*\)\s*;`)

func hasTablePathPrefix(query string) bool {
	return pragmaTablePathPrefixRe.MatchString(query)
}

func (c *conn) nativeQueryAndParameters(ctx context.Context, q string, args ...driver.NamedValue) (
	query string, _ *table.QueryParameters, _ error,
) {
	if isStrictYQL(ctx) || hasDeclare(q) || hasTablePathPrefix(q) {
		params := make([]table.ParameterOption, len(args))
		for i, arg := range args {
			switch v := arg.Value.(type) {
			case *table.QueryParameters:
				if len(args) > 1 {
					return "", nil, xerrors.WithStackTrace(fmt.Errorf("%v: %w", args, bind.ErrMultipleQueryParameters))
				}
				return q, v, nil
			case table.ParameterOption:
				params[i] = v
			default:
				param, err := bind.ToYdbParam(arg)
				if err != nil {
					return "", nil, xerrors.WithStackTrace(err)
				}
				params[i] = param
			}
		}
		return q, table.NewQueryParameters(params...), nil
	}
	return bind.Bind(q, c.TablePathPrefix(), args...)
}

func (c *conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (_ driver.Tx, err error) {
	var transaction table.Transaction
	onDone := trace.DatabaseSQLOnConnBegin(c.trace, &ctx)
	defer func() {
		onDone(transaction, err)
	}()
	m := queryModeFromContext(ctx, c.defaultQueryMode)
	if m != DataQueryMode {
		return nil, badconn.Map(xerrors.WithStackTrace(fmt.Errorf("wrong query mode: %s", m.String())))
	}
	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	if c.currentTx != nil {
		return nil, xerrors.WithStackTrace(
			fmt.Errorf("conn already have an opened currentTx: %s", c.currentTx.ID()),
		)
	}
	var txc table.TxOption
	txc, err = isolation.ToYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	transaction, err = c.session.BeginTransaction(ctx, table.TxSettings(txc))
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	c.currentTx = &tx{
		conn: c,
		ctx:  ctx,
		tx:   transaction,
	}
	return c.currentTx, nil
}

func (c *conn) Version(context.Context) (_ string, err error) {
	const version = "default"
	return version, nil
}

func (c *conn) IsTableExists(ctx context.Context, tableName string) (tableExists bool, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err = helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
	if err != nil {
		return false, xerrors.WithStackTrace(err)
	}
	return tableExists, nil
}

func (c *conn) IsColumnExists(ctx context.Context, tableName, columnName string) (columnExists bool, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
		for _, col := range desc.Columns {
			if col.Name == columnName {
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

func (c *conn) GetColumns(ctx context.Context, tableName string) (columns []string, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
		for _, col := range desc.Columns {
			columns = append(columns, col.Name)
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return columns, nil
}

func (c *conn) GetColumnType(ctx context.Context, tableName, columnName string) (dataType string, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
		for _, col := range desc.Columns {
			if col.Name == columnName {
				dataType = col.Type.Yql()
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

func (c *conn) GetPrimaryKeys(ctx context.Context, tableName string) (pkCols []string, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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

func (c *conn) IsPrimaryKey(ctx context.Context, tableName, columnName string) (ok bool, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
	switch ch := folderOrTable[0]; ch {
	case '/':
		return folderOrTable
	case '.':
		return path.Join(c.GetDatabaseName(), c.tablePathPrefix, strings.TrimLeft(folderOrTable, "."))
	default:
		return path.Join(c.GetDatabaseName(), c.tablePathPrefix, folderOrTable)
	}
}

func (c *conn) GetTables(ctx context.Context, folder string) (tables []string, err error) {
	absPath := c.normalizePath(folder)
	var e scheme.Entry
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		e, err = c.connector.parent.Scheme().DescribePath(ctx, absPath)
		return err
	}, retry.WithIdempotent(true))

	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch {
	case e.IsTable():
		_, table := path.Split(absPath)
		tables = append(tables, table)
		return tables, nil
	case e.IsDirectory():
		var d scheme.Directory
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			d, err = c.connector.parent.Scheme().ListDirectory(ctx, absPath)
			return err
		}, retry.WithIdempotent(true))

		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		for _, child := range d.Children {
			if child.IsTable() {
				tables = append(tables, child.Name)
			}
		}
		return tables, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("path '%s' should be a table or directory", folder))
	}
}

func (c *conn) GetAllTables(ctx context.Context, folder string) (tables []string, err error) {
	folder = c.normalizePath(folder)
	ignoreDirs := map[string]bool{
		".sys":        true,
		".sys_health": true,
	}

	canEnter := func(dir string) bool {
		if _, ignore := ignoreDirs[dir]; ignore {
			return false
		}
		return true
	}

	queue := make([]string, 0)
	queue = append(queue, folder)

	for st := 0; st < len(queue); st++ {
		curPath := queue[st]

		var e scheme.Entry
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			e, err = c.connector.parent.Scheme().DescribePath(ctx, curPath)
			return err
		}, retry.WithIdempotent(true))

		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		if e.IsTable() {
			switch curPath {
			case folder:
				_, table := path.Split(curPath)
				tables = append(tables, table)
			default:
				tables = append(tables, strings.TrimPrefix(curPath, folder+"/"))
			}
			continue
		}

		var d scheme.Directory
		err = retry.Retry(ctx, func(ctx context.Context) (err error) {
			d, err = c.connector.parent.Scheme().ListDirectory(ctx, curPath)
			return err
		}, retry.WithIdempotent(true))

		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		for _, child := range d.Children {
			if child.IsDirectory() || child.IsTable() {
				if canEnter(child.Name) {
					queue = append(queue, path.Join(curPath, child.Name))
				}
			}
		}
	}
	return tables, nil
}

func (c *conn) GetIndexes(ctx context.Context, tableName string) (indexes []string, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
		for _, indexDesc := range desc.Indexes {
			indexes = append(indexes, indexDesc.Name)
		}
		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return indexes, nil
}

func (c *conn) GetIndexColumns(ctx context.Context, tableName, indexName string) (columns []string, err error) {
	tableName = c.normalizePath(tableName)
	tableExists, err := helpers.IsTableExists(ctx, c.connector.parent.Scheme(), tableName)
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
		for _, indexDesc := range desc.Indexes {
			if indexDesc.Name == indexName {
				columns = append(columns, indexDesc.IndexColumns...)
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
