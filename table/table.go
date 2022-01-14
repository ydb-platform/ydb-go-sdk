package table

import (
	"bytes"
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type SessionInfo interface {
	ID() string
}

type Session interface {
	SessionInfo

	CreateTable(
		ctx context.Context,
		path string,
		opts ...options.CreateTableOption,
	) (err error)
	DescribeTable(
		ctx context.Context,
		path string,
		opts ...options.DescribeTableOption,
	) (desc options.Description, err error)
	DropTable(
		ctx context.Context,
		path string,
		opts ...options.DropTableOption,
	) (err error)
	AlterTable(
		ctx context.Context,
		path string,
		opts ...options.AlterTableOption,
	) (err error)
	CopyTable(
		ctx context.Context,
		dst, src string,
		opts ...options.CopyTableOption,
	) (err error)
	Explain(
		ctx context.Context,
		query string,
	) (exp DataQueryExplanation, err error)
	Prepare(
		ctx context.Context,
		query string,
	) (stmt Statement, err error)
	Execute(
		ctx context.Context,
		tx *TransactionControl,
		query string,
		params *QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (txr Transaction, r result.Result, err error)
	ExecuteSchemeQuery(
		ctx context.Context,
		query string,
		opts ...options.ExecuteSchemeQueryOption,
	) (err error)
	DescribeTableOptions(
		ctx context.Context,
	) (desc options.TableOptionsDescription, err error)
	StreamReadTable(
		ctx context.Context,
		path string,
		opts ...options.ReadTableOption,
	) (r result.StreamResult, err error)
	StreamExecuteScanQuery(
		ctx context.Context,
		query string,
		params *QueryParameters,
		opts ...options.ExecuteScanQueryOption,
	) (_ result.StreamResult, err error)
	BulkUpsert(
		ctx context.Context,
		table string,
		rows types.Value,
	) (err error)
	BeginTransaction(
		ctx context.Context,
		tx *TransactionSettings,
	) (x Transaction, err error)
	KeepAlive(
		ctx context.Context,
	) error
}

type TransactionSettings struct {
	settings Ydb_Table.TransactionSettings
}

func (t *TransactionSettings) Settings() *Ydb_Table.TransactionSettings {
	if t == nil {
		return nil
	}
	return &t.settings
}

// Explanation is a result of Explain calls.
type Explanation struct {
	Plan string
}

// ScriptingYQLExplanation is a result of Explain calls.
type ScriptingYQLExplanation struct {
	Explanation

	ParameterTypes map[string]types.Type
}

// DataQueryExplanation is a result of ExplainDataQuery call.
type DataQueryExplanation struct {
	Explanation

	AST string
}

// DataQuery only for tracers
type DataQuery interface {
	String() string
	ID() string
	YQL() string
}

type TransactionIdentifier interface {
	ID() string
}

type TransactionActor interface {
	TransactionIdentifier

	Execute(
		ctx context.Context,
		query string,
		params *QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (result.Result, error)
	ExecuteStatement(
		ctx context.Context,
		stmt Statement,
		params *QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (result.Result, error)
}

type Transaction interface {
	TransactionActor

	CommitTx(
		ctx context.Context,
		opts ...options.CommitTransactionOption,
	) (r result.Result, err error)
	Rollback(
		ctx context.Context,
	) (err error)
}

type Statement interface {
	Execute(
		ctx context.Context,
		tx *TransactionControl,
		params *QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (txr Transaction, r result.Result, err error)
	NumInput() int
	Text() string
}

var (
	serializableReadWrite = &Ydb_Table.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Table.SerializableModeSettings{},
	}
	staleReadOnly = &Ydb_Table.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Table.StaleModeSettings{},
	}
)

// Transaction control options
type (
	txDesc   Ydb_Table.TransactionSettings
	TxOption func(*txDesc)
)

func TxSettings(opts ...TxOption) *TransactionSettings {
	s := new(TransactionSettings)
	for _, opt := range opts {
		opt((*txDesc)(&s.settings))
	}
	return s
}

func BeginTx(opts ...TxOption) TxControlOption {
	return func(d *txControlDesc) {
		s := TxSettings(opts...)
		d.TxSelector = &Ydb_Table.TransactionControl_BeginTx{
			BeginTx: &s.settings,
		}
	}
}

func WithTx(t Transaction) TxControlOption {
	return func(d *txControlDesc) {
		d.TxSelector = &Ydb_Table.TransactionControl_TxId{
			TxId: t.ID(),
		}
	}
}

func CommitTx() TxControlOption {
	return func(d *txControlDesc) {
		d.CommitTx = true
	}
}

func WithSerializableReadWrite() TxOption {
	return func(d *txDesc) {
		d.TxMode = serializableReadWrite
	}
}

func WithStaleReadOnly() TxOption {
	return func(d *txDesc) {
		d.TxMode = staleReadOnly
	}
}

func WithOnlineReadOnly(opts ...TxOnlineReadOnlyOption) TxOption {
	return func(d *txDesc) {
		var ro txOnlineReadOnly
		for _, opt := range opts {
			opt(&ro)
		}
		d.TxMode = &Ydb_Table.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: (*Ydb_Table.OnlineModeSettings)(&ro),
		}
	}
}

type (
	txOnlineReadOnly       Ydb_Table.OnlineModeSettings
	TxOnlineReadOnlyOption func(*txOnlineReadOnly)
)

func WithInconsistentReads() TxOnlineReadOnlyOption {
	return func(d *txOnlineReadOnly) {
		d.AllowInconsistentReads = true
	}
}

type (
	txControlDesc   Ydb_Table.TransactionControl
	TxControlOption func(*txControlDesc)
)

type TransactionControl struct {
	desc Ydb_Table.TransactionControl
}

func (t *TransactionControl) Desc() *Ydb_Table.TransactionControl {
	if t == nil {
		return nil
	}
	return &t.desc
}

func TxControl(opts ...TxControlOption) *TransactionControl {
	c := new(TransactionControl)
	for _, opt := range opts {
		opt((*txControlDesc)(&c.desc))
	}
	return c
}

// QueryParameters

type (
	queryParams     map[string]*Ydb.TypedValue
	ParameterOption func(queryParams)
	QueryParameters struct {
		m queryParams
	}
)

func (q *QueryParameters) Params() queryParams {
	if q == nil {
		return nil
	}
	return q.m
}

func (q *QueryParameters) Each(it func(name string, v types.Value)) {
	if q == nil {
		return
	}
	for key, v := range q.m {
		it(key, value.FromYDB(
			v.Type,
			v.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, v types.Value) {
		buf.WriteString("((")
		buf.WriteString(name)
		buf.WriteByte(')')
		buf.WriteByte('(')
		value.WriteValueStringTo(&buf, v)
		buf.WriteString("))")
	})
	buf.WriteByte(')')
	return buf.String()
}

func NewQueryParameters(opts ...ParameterOption) *QueryParameters {
	q := &QueryParameters{
		m: make(queryParams, len(opts)),
	}
	q.Add(opts...)
	return q
}

func (q *QueryParameters) Add(opts ...ParameterOption) {
	for _, opt := range opts {
		opt(q.m)
	}
}

func ValueParam(name string, v types.Value) ParameterOption {
	return func(q queryParams) {
		q[name] = value.ToYDB(v)
	}
}
