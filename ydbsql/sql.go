package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/options"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	table2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var (
	ErrDeprecated          = errors.New("ydbsql: deprecated")
	ErrUnsupported         = errors.New("ydbsql: not supported")
	ErrActiveTransaction   = errors.New("ydbsql: can not begin tx within active tx")
	ErrNoActiveTransaction = errors.New("ydbsql: no active tx to work with")
)

type ydbWrapper struct {
	dst *driver.Value
}

func (d *ydbWrapper) UnmarshalYDB(res types.RawValue) error {
	if res.IsOptional() {
		res.Unwrap()
	}
	if res.IsDecimal() {
		b, p, s := res.UnwrapDecimal()
		*d.dst = Decimal{
			Bytes:     b,
			Precision: p,
			Scale:     s,
		}
	} else {
		*d.dst = res.Any()
	}
	return res.Err()
}

// sqlConn is a connection to the ydb.
type sqlConn struct {
	connector *connector      // Immutable and r/o usage.
	session   *table2.Session // Immutable and r/o usage.

	idle bool

	tx  *table2.Transaction
	txc *table2.TransactionControl
}

func (c *sqlConn) takeSession(ctx context.Context) bool {
	if !c.idle {
		return true
	}
	if has, _ := c.pool().Take(ctx, c.session); !has {
		return false
	}
	c.idle = false
	return true
}

func (c *sqlConn) ResetSession(ctx context.Context) error {
	if c.idle {
		return nil
	}
	err := c.pool().Put(ctx, c.session)
	if err != nil {
		return mapBadSessionError(err)
	}
	c.idle = true
	return nil
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if !c.takeSession(ctx) {
		return nil, driver.ErrBadConn
	}
	s, err := c.session.Prepare(ctx, query)
	if err != nil {
		return nil, mapBadSessionError(err)
	}
	return &stmt{
		conn: c,
		stmt: s,
	}, nil
}

// txIsolationOrControl maps driver transaction options to ydb transaction option or query transaction control.
// This caused by ydb logic that prevents start actual transaction with OnlineReadOnly mode and ReadCommitted
// and ReadUncommitted isolation levels should use tx_control in every query request.
// It returns error on unsupported options.
func txIsolationOrControl(opts driver.TxOptions) (isolation table2.TxOption, control []table2.TxControlOption, err error) {
	level := sql.IsolationLevel(opts.Isolation)
	switch level {
	case sql.LevelDefault,
		sql.LevelSerializable,
		sql.LevelLinearizable:

		isolation = table2.WithSerializableReadWrite()
		return

	case sql.LevelReadUncommitted:
		if opts.ReadOnly {
			control = []table2.TxControlOption{
				table2.BeginTx(
					table2.WithOnlineReadOnly(
						table2.WithInconsistentReads(),
					),
				),
				table2.CommitTx(),
			}
			return
		}

	case sql.LevelReadCommitted:
		if opts.ReadOnly {
			control = []table2.TxControlOption{
				table2.BeginTx(
					table2.WithOnlineReadOnly(),
				),
				table2.CommitTx(),
			}
			return
		}
	}
	return nil, nil, fmt.Errorf(
		"ydbsql: unsupported transaction options: isolation=%s read_only=%t",
		nameIsolationLevel(level), opts.ReadOnly,
	)
}

func (c *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if !c.takeSession(ctx) {
		return nil, driver.ErrBadConn
	}
	if c.tx != nil || c.txc != nil {
		return nil, ErrActiveTransaction
	}
	isolation, control, err := txIsolationOrControl(opts)
	if err != nil {
		return nil, err
	}
	if isolation != nil {
		c.tx, err = c.session.BeginTransaction(ctx, table2.TxSettings(isolation))
		if err != nil {
			return nil, mapBadSessionError(err)
		}
		c.txc = table2.TxControl(table2.WithTx(c.tx))
	} else {
		c.txc = table2.TxControl(control...)
	}
	return c, nil
}

// Rollback implements driver.Tx interface.
// Note that it is called by driver even if a user did not called it.
func (c *sqlConn) Rollback() error {
	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		return tx.Rollback(context.Background())
	}
	return nil
}

func (c *sqlConn) Commit() (err error) {
	if c.tx == nil && c.txc == nil {
		return ErrNoActiveTransaction
	}

	tx := c.tx
	c.tx = nil
	c.txc = nil

	if tx != nil {
		_, err = tx.CommitTx(context.Background())
	}
	return
}

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	return result{}, nil
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return c.scanQueryContext(ctx, query, args)
	}
	return c.queryContext(ctx, query, args)
}

func (c *sqlConn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &rows{res: res}, nil
}

func (c *sqlConn) scanQueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.exec(ctx, &reqScanQuery{text: query}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &stream{ctx: ctx, res: res}, res.Err()
}

func (c *sqlConn) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (c *sqlConn) Ping(ctx context.Context) error {
	if !c.takeSession(ctx) {
		return driver.ErrBadConn
	}
	_, err := c.session.KeepAlive(ctx)
	return mapBadSessionError(err)
}

func (c *sqlConn) Close() error {
	ctx := context.Background()
	if !c.takeSession(ctx) {
		return driver.ErrBadConn
	}
	err := c.session.Close(ctx)
	return mapBadSessionError(err)
}

func (c *sqlConn) Prepare(string) (driver.Stmt, error) {
	return nil, ErrDeprecated
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	return nil, ErrDeprecated
}

func (c *sqlConn) exec(ctx context.Context, req processor, params *table2.QueryParameters) (res resultset.Result, err error) {
	if !c.takeSession(ctx) {
		return nil, driver.ErrBadConn
	}
	c.pool().Retry(
		ctx,
		retry.ContextRetryNoIdempotent(ctx),
		func(ctx context.Context, session *table2.Session) (err error) {
			res, err = req.process(ctx, c, params)
			return err
		},
	)
	return nil, err
}

func (c *sqlConn) txControl() *table2.TransactionControl {
	if c.txc == nil {
		return c.connector.defaultTxControl
	}
	return c.txc
}

func (c *sqlConn) dataOpts() []options.ExecuteDataQueryOption {
	return c.connector.dataOpts
}

func (c *sqlConn) scanOpts() []options.ExecuteScanQueryOption {
	return c.connector.scanOpts
}

func (c *sqlConn) pool() *table2.SessionPool {
	return &c.connector.pool
}

type processor interface {
	process(context.Context, *sqlConn, *table2.QueryParameters) (resultset.Result, error)
}

type reqStmt struct {
	stmt *table2.Statement
}

func (o *reqStmt) process(ctx context.Context, c *sqlConn, params *table2.QueryParameters) (resultset.Result, error) {
	_, res, err := o.stmt.Execute(ctx, c.txControl(), params, c.dataOpts()...)
	return res, err
}

type reqQuery struct {
	text string
}

func (o *reqQuery) process(ctx context.Context, c *sqlConn, params *table2.QueryParameters) (resultset.Result, error) {
	_, res, err := c.session.Execute(ctx, c.txControl(), o.text, params, c.dataOpts()...)
	return res, err
}

type reqScanQuery struct {
	text string
}

func (o *reqScanQuery) process(ctx context.Context, c *sqlConn, params *table2.QueryParameters) (resultset.Result, error) {
	return c.session.StreamExecuteScanQuery(ctx, o.text, params, c.scanOpts()...)
}

type TxOperationFunc func(context.Context, *sql.Tx) error

// TxDoer contains options for retrying transactions.
type TxDoer struct {
	DB      *sql.DB
	Options *sql.TxOptions
}

// Do starts a transaction and calls f with it. If f() call returns a retryable
// error, it repeats it accordingly to retry configuration that TxDoer's DB
// driver holds.
//
// Note that callers should mutate state outside of f carefully and keeping in
// mind that f could be called again even if no error returned – transaction
// commitment can be failed:
//
//   var results []int
//   ydbsql.DoTx(ctx, db, TxOperationFunc(func(ctx context.Context, tx *sql.Tx) error {
//       // Reset resulting slice to prevent duplicates when retry occurred.
//       results = results[:0]
//
//       rows, err := tx.QueryContext(...)
//       if err != nil {
//           // handle error
//       }
//       for rows.Next() {
//           results = append(results, ...)
//       }
//       return rows.Err()
//   }))
func (d TxDoer) Do(ctx context.Context, f TxOperationFunc) (err error) {
	return retry.Retry(
		ctx,
		retry.ContextRetryNoIdempotent(ctx),
		func(ctx context.Context) (err error) {
			return d.do(ctx, f)
		},
	)
}

func (d TxDoer) do(ctx context.Context, f TxOperationFunc) error {
	tx, err := d.DB.BeginTx(ctx, d.Options)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := f(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// DoTx is a shortcut for calling Do(ctx, f) on initialized TxDoer with DB
// field set to given db.
func DoTx(ctx context.Context, db *sql.DB, f TxOperationFunc) error {
	return (TxDoer{DB: db}).Do(ctx, f)
}

var isolationLevelName = [...]string{
	sql.LevelDefault:         "default",
	sql.LevelReadUncommitted: "read_uncommitted",
	sql.LevelReadCommitted:   "read_committed",
	sql.LevelWriteCommitted:  "write_committed",
	sql.LevelRepeatableRead:  "repeatable_read",
	sql.LevelSnapshot:        "snapshot",
	sql.LevelSerializable:    "serializable",
	sql.LevelLinearizable:    "linearizable",
}

func nameIsolationLevel(x sql.IsolationLevel) string {
	if int(x) < len(isolationLevelName) {
		return isolationLevelName[x]
	}
	return "unknown_isolation"
}

type stmt struct {
	conn *sqlConn
	stmt *table2.Statement
}

func (s *stmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmt) Close() error {
	return nil
}

func (s stmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, ErrDeprecated
}

func (s stmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, ErrDeprecated
}

func (s *stmt) CheckNamedValue(v *driver.NamedValue) error {
	return checkNamedValue(v)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	_, err := s.conn.exec(ctx, &reqStmt{stmt: s.stmt}, params(args))
	if err != nil {
		return nil, err
	}
	return result{}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if ContextScanQueryMode(ctx) {
		// Allow to use scanQuery only through QueryContext API.
		return s.scanQueryContext(ctx, args)
	}
	return s.queryContext(ctx, args)
}

func (s *stmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.conn.exec(ctx, &reqStmt{stmt: s.stmt}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &rows{res: res}, nil
}

func (s *stmt) scanQueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.conn.exec(ctx, &reqScanQuery{text: s.stmt.Text()}, params(args))
	if err != nil {
		return nil, err
	}
	res.NextResultSet(ctx)
	return &stream{ctx: ctx, res: res}, res.Err()
}

func checkNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		return fmt.Errorf("ydbsql: only named parameters are supported")
	}

	if valuer, ok := v.Value.(driver.Valuer); ok {
		v.Value, err = valuer.Value()
		if err != nil {
			return fmt.Errorf("ydbsql: driver.Valuer error: %w", err)
		}
	}

	switch x := v.Value.(type) {
	case types.Value:
		// OK.

	case valuer:
		// Some ydbsql level types implement valuer interface.
		// Currently it is a date/time types.
		v.Value = x.Value()

	case bool:
		v.Value = types.BoolValue(x)
	case int8:
		v.Value = types.Int8Value(x)
	case uint8:
		v.Value = types.Uint8Value(x)
	case int16:
		v.Value = types.Int16Value(x)
	case uint16:
		v.Value = types.Uint16Value(x)
	case int32:
		v.Value = types.Int32Value(x)
	case uint32:
		v.Value = types.Uint32Value(x)
	case int64:
		v.Value = types.Int64Value(x)
	case uint64:
		v.Value = types.Uint64Value(x)
	case float32:
		v.Value = types.FloatValue(x)
	case float64:
		v.Value = types.DoubleValue(x)
	case []byte:
		v.Value = types.StringValue(x)
	case string:
		v.Value = types.UTF8Value(x)
	case [16]byte:
		v.Value = types.UUIDValue(x)

	default:
		return fmt.Errorf("ydbsql: unsupported types: %T", x)
	}

	v.Name = "$" + v.Name

	return nil
}

func params(args []driver.NamedValue) *table2.QueryParameters {
	if len(args) == 0 {
		return nil
	}
	opts := make([]table2.ParameterOption, len(args))
	for i, arg := range args {
		opts[i] = table2.ValueParam(
			arg.Name,
			arg.Value.(types.Value),
		)
	}
	return table2.NewQueryParameters(opts...)
}

type rows struct {
	res resultset.Result
}

func (r *rows) Columns() []string {
	var i int
	cs := make([]string, r.res.CurrentResultSet().ColumnCount())
	r.res.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *rows) NextResultSet() error {
	if !r.res.NextResultSet(context.Background()) {
		return io.EOF
	}
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.res.HasNextResultSet()
}

func (r *rows) Next(dst []driver.Value) error {
	if !r.res.NextRow() {
		return io.EOF
	}
	for i := range dst {
		// NOTE: for queries like "SELECT * FROM xxx" order of columns is
		// undefined.
		_ = r.res.Scan(&ydbWrapper{&dst[i]})
	}
	return r.res.Err()
}

func (r *rows) Close() error {
	return r.res.Close()
}

type stream struct {
	res resultset.Result
	ctx context.Context
}

func (r *stream) Columns() []string {
	var i int
	cs := make([]string, r.res.CurrentResultSet().ColumnCount())
	r.res.CurrentResultSet().Columns(func(m options.Column) {
		cs[i] = m.Name
		i++
	})
	return cs
}

func (r *stream) Next(dst []driver.Value) error {
	if !r.res.HasNextRow() {
		if !r.res.NextResultSet(r.ctx) {
			err := r.res.Err()
			if err != nil {
				return err
			}
			return io.EOF
		}
	}
	if !r.res.NextRow() {
		return io.EOF
	}
	for i := range dst {
		// NOTE: for queries like "SELECT * FROM xxx" order of columns is
		// undefined.
		_ = r.res.Scan(&ydbWrapper{&dst[i]})
	}
	return r.res.Err()
}

func (r *stream) Close() error {
	return r.res.Close()
}

type result struct{}

func (r result) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (r result) RowsAffected() (int64, error) { return 0, ErrUnsupported }

func mapBadSessionError(err error) error {
	if errors.IsOpError(err, errors.StatusBadSession) {
		return driver.ErrBadConn
	}
	return err
}
