package table

import (
	"bytes"
	"context"
	"io"
	"runtime"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/grpc/Ydb_Table_V1"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/internal/cache/lru"
)

// Client contains logic of creation of ydb table sessions.
type Client struct {
	Driver ydb.Driver
	Trace  ClientTrace

	MaxQueryCacheSize int
}

// CreateSession creates new session instance.
// Unused sessions must be destroyed.
func (c *Client) CreateSession(ctx context.Context) (s *Session, err error) {
	c.traceCreateSessionStart(ctx)
	defer func() {
		c.traceCreateSessionDone(ctx, s, err)
	}()
	var (
		req Ydb_Table.CreateSessionRequest
		res Ydb_Table.CreateSessionResult
	)
	err = c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.CreateSession, &req, &res))
	if err != nil {
		return nil, err
	}
	s = &Session{
		ID: res.SessionId,
		c:  *c,
		qcache: lru.Cache{
			MaxSize: c.MaxQueryCacheSize,
		},
	}
	runtime.SetFinalizer(s, func(s *Session) {
		go s.Close(context.Background())
	})
	return
}

// Session represents a single table API session.
//
// Session methods are not goroutine safe. Simultaneous execution of requests
// are forbidden within a single session.
//
// Note that after Session is no longer needed it should be destroyed by
// Close() call.
type Session struct {
	ID string

	c Client

	qcache lru.Cache
	qhash  queryHasher

	closed  bool
	onClose []func()
}

func (s *Session) OnClose(cb func()) {
	if s.closed {
		return
	}
	s.onClose = append(s.onClose, cb)
}

func (s *Session) Close(ctx context.Context) (err error) {
	if s.closed {
		return nil
	}
	s.closed = true
	s.c.traceDeleteSessionStart(ctx, s)
	defer func() {
		runtime.SetFinalizer(s, nil)
		for _, cb := range s.onClose {
			cb()
		}
		s.c.traceDeleteSessionDone(ctx, s, err)
	}()
	req := Ydb_Table.DeleteSessionRequest{
		SessionId: s.ID,
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.DeleteSession, &req, nil))
}

// KeepAlive keeps idle session alive.
func (s *Session) KeepAlive(ctx context.Context) (err error) {
	s.c.traceKeepAliveStart(ctx, s)
	defer func() {
		s.c.traceKeepAliveDone(ctx, s, err)
	}()
	req := Ydb_Table.KeepAliveRequest{
		SessionId: s.ID,
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.KeepAlive, &req, nil))
}

// CreateTable creates table at given path with given options.
func (s *Session) CreateTable(ctx context.Context, path string, opts ...CreateTableOption) error {
	req := Ydb_Table.CreateTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*createTableDesc)(&req))
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.CreateTable, &req, nil))
}

// DescribeTable describes table at given path.
func (s *Session) DescribeTable(ctx context.Context, path string) (desc Description, err error) {
	var res Ydb_Table.DescribeTableResult
	req := Ydb_Table.DescribeTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.DescribeTable, &req, &res))
	if err != nil {
		return desc, err
	}
	cs := make([]Column, len(res.Columns))
	for i, c := range res.Columns {
		cs[i] = Column{
			Name: c.Name,
			Type: internal.TypeFromYDB(c.Type),
		}
	}
	return Description{
		Name:       res.Self.Name,
		PrimaryKey: res.PrimaryKey,
		Columns:    cs,
	}, nil
}

// DropTable drops table at given path with given options.
func (s *Session) DropTable(ctx context.Context, path string, opts ...DropTableOption) error {
	req := Ydb_Table.DropTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*dropTableDesc)(&req))
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.DropTable, &req, nil))
}

// AlterTable modifies schema of table at given path with given options.
func (s *Session) AlterTable(ctx context.Context, path string, opts ...AlterTableOption) error {
	req := Ydb_Table.AlterTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*alterTableDesc)(&req))
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.AlterTable, &req, nil))
}

// CopyTable creates copy of table at given path.
func (s *Session) CopyTable(ctx context.Context, dst, src string, opts ...CopyTableOption) error {
	req := Ydb_Table.CopyTableRequest{
		SessionId:       s.ID,
		SourcePath:      src,
		DestinationPath: dst,
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.CopyTable, &req, nil))
}

// DataQueryExplanation is a result of ExplainDataQuery call.
type DataQueryExplanation struct {
	AST  string
	Plan string
}

// Explain explains data query represented by text.
func (s *Session) Explain(ctx context.Context, query string) (exp DataQueryExplanation, err error) {
	var res Ydb_Table.ExplainQueryResult
	req := Ydb_Table.ExplainDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.ExplainDataQuery, &req, &res))
	if err != nil {
		return
	}
	return DataQueryExplanation{
		AST:  res.QueryAst,
		Plan: res.QueryPlan,
	}, nil
}

// Statement is a prepared statement. Like a single Session, it is not safe for
// concurrent use by multiple goroutines.
type Statement struct {
	session *Session
	query   *DataQuery
	qhash   queryHash
	params  map[string]*Ydb.Type
}

// Execute executes prepared data query.
func (s *Statement) Execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	txr, r, err = s.session.executeDataQuery(ctx, tx, s.query, params, opts...)
	if ydb.IsOpError(err, ydb.StatusNotFound) {
		s.session.qcache.Remove(s.qhash)
	}
	return
}

func (s *Statement) NumInput() int {
	return len(s.params)
}

// Prepare prepares data query within session s.
func (s *Session) Prepare(
	ctx context.Context, query string,
) (
	stmt *Statement, err error,
) {
	var (
		cached bool
		q      *DataQuery
	)
	s.c.tracePrepareDataQueryStart(ctx, s, query)
	defer func() {
		s.c.tracePrepareDataQueryDone(ctx, s, query, q, cached, err)
	}()

	cacheKey := s.qhash.hash(query)
	v, cached := s.qcache.Get(cacheKey)
	if cached {
		return v.(*Statement), nil
	}

	var res Ydb_Table.PrepareQueryResult
	req := Ydb_Table.PrepareDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.PrepareDataQuery, &req, &res))
	if err != nil {
		return nil, err
	}
	q = new(DataQuery)
	q.initPrepared(res.QueryId)
	stmt = &Statement{
		session: s,
		query:   q,
		qhash:   cacheKey,
		params:  res.ParametersTypes,
	}
	s.qcache.Add(cacheKey, stmt)

	return stmt, nil
}

// Execute executes given data query represented by text.
func (s *Session) Execute(
	ctx context.Context, tx *TransactionControl,
	query string, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	q := new(DataQuery)
	q.initFromText(query)
	return s.executeDataQuery(ctx, tx, q, params, opts...)
}

// executeDataQuery executes data query. It may execute raw text query or query
// prepared before.
func (s *Session) executeDataQuery(
	ctx context.Context, tx *TransactionControl,
	query *DataQuery, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	s.c.traceExecuteDataQueryStart(ctx, s, tx, query, params)
	defer func() {
		s.c.traceExecuteDataQueryDone(ctx, s, tx, query, params, txr, r, err)
	}()
	var res Ydb_Table.ExecuteQueryResult
	req := Ydb_Table.ExecuteDataQueryRequest{
		SessionId:  s.ID,
		TxControl:  &tx.desc,
		Parameters: params.params(),
		Query:      &query.query,
	}
	for _, opt := range opts {
		opt((*executeDataQueryDesc)(&req))
	}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.ExecuteDataQuery, &req, &res))
	if err != nil {
		return txr, r, err
	}
	txr = &Transaction{
		id: res.TxMeta.Id,
		s:  s,
	}
	r = &Result{
		sets: res.ResultSets,
	}
	return txr, r, nil
}

// ExecuteSchemeQuery executes scheme query.
func (s *Session) ExecuteSchemeQuery(
	ctx context.Context, query string,
	opts ...ExecuteSchemeQueryOption,
) error {
	req := Ydb_Table.ExecuteSchemeQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	for _, opt := range opts {
		opt((*executeSchemeQueryDesc)(&req))
	}
	return s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.ExecuteSchemeQuery, &req, nil))
}

// DescribeTableOptions describes supported table options.
func (s *Session) DescribeTableOptions(ctx context.Context) (desc TableOptionsDescription, err error) {
	var res Ydb_Table.DescribeTableOptionsResult
	req := Ydb_Table.DescribeTableOptionsRequest{}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.DescribeTableOptions, &req, &res))
	if err != nil {
		return
	}
	{
		xs := make([]TableProfileDescription, len(res.TableProfilePresets))
		for i, p := range res.TableProfilePresets {
			xs[i] = TableProfileDescription{
				Name:   p.Name,
				Labels: p.Labels,

				DefaultStoragePolicy:      p.DefaultStoragePolicy,
				DefaultCompactionPolicy:   p.DefaultCompactionPolicy,
				DefaultPartitioningPolicy: p.DefaultPartitioningPolicy,
				DefaultExecutionPolicy:    p.DefaultExecutionPolicy,
				DefaultReplicationPolicy:  p.DefaultReplicationPolicy,
				DefaultCachingPolicy:      p.DefaultCachingPolicy,

				AllowedStoragePolicies:      p.AllowedStoragePolicies,
				AllowedCompactionPolicies:   p.AllowedCompactionPolicies,
				AllowedPartitioningPolicies: p.AllowedPartitioningPolicies,
				AllowedExecutionPolicies:    p.AllowedExecutionPolicies,
				AllowedReplicationPolicies:  p.AllowedReplicationPolicies,
				AllowedCachingPolicies:      p.AllowedCachingPolicies,
			}
		}
		desc.TableProfilePresets = xs
	}
	{
		xs := make([]StoragePolicyDescription, len(res.StoragePolicyPresets))
		for i, p := range res.StoragePolicyPresets {
			xs[i] = StoragePolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.StoragePolicyPresets = xs
	}
	{
		xs := make([]CompactionPolicyDescription, len(res.CompactionPolicyPresets))
		for i, p := range res.CompactionPolicyPresets {
			xs[i] = CompactionPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.CompactionPolicyPresets = xs
	}
	{
		xs := make([]PartitioningPolicyDescription, len(res.PartitioningPolicyPresets))
		for i, p := range res.PartitioningPolicyPresets {
			xs[i] = PartitioningPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.PartitioningPolicyPresets = xs
	}
	{
		xs := make([]ExecutionPolicyDescription, len(res.ExecutionPolicyPresets))
		for i, p := range res.ExecutionPolicyPresets {
			xs[i] = ExecutionPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.ExecutionPolicyPresets = xs
	}
	{
		xs := make([]ReplicationPolicyDescription, len(res.ReplicationPolicyPresets))
		for i, p := range res.ReplicationPolicyPresets {
			xs[i] = ReplicationPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.ReplicationPolicyPresets = xs
	}
	{
		xs := make([]CachingPolicyDescription, len(res.CachingPolicyPresets))
		for i, p := range res.CachingPolicyPresets {
			xs[i] = CachingPolicyDescription{
				Name:   p.Name,
				Labels: p.Labels,
			}
		}
		desc.CachingPolicyPresets = xs
	}
	return desc, nil
}

// StreamReadTable reads table at given path with given options.
func (s *Session) StreamReadTable(ctx context.Context, path string, opts ...ReadTableOption) (r *Result, err error) {
	var resp Ydb_Table.ReadTableResponse
	req := Ydb_Table.ReadTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*readTableDesc)(&req))
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	var (
		ch = make(chan *Ydb.ResultSet, 1)
		ce = new(error)
	)
	err = s.c.Driver.StreamRead(ctx, internal.WrapStreamOperation(
		Ydb_Table_V1.StreamReadTable, &req, &resp,
		func(err error) {
			if err != io.EOF {
				*ce = err
			}
			if err != nil {
				close(ch)
				return
			}
			select {
			case <-ctx.Done():
			case ch <- resp.Result.ResultSet:
			}
		},
	))
	if err != nil {
		cancel()
		return
	}
	r = &Result{
		setCh:       ch,
		setChErr:    ce,
		setChCancel: cancel,
	}
	return r, nil
}

// BeginTransaction begins new transaction within given session with given
// settings.
func (s *Session) BeginTransaction(ctx context.Context, tx *TransactionSettings) (x *Transaction, err error) {
	s.c.traceBeginTransactionStart(ctx, s)
	defer func() {
		s.c.traceBeginTransactionDone(ctx, s, x, err)
	}()
	var res Ydb_Table.BeginTransactionResult
	req := Ydb_Table.BeginTransactionRequest{
		SessionId:  s.ID,
		TxSettings: &tx.settings,
	}
	err = s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.BeginTransaction, &req, &res))
	if err != nil {
		return
	}
	return &Transaction{
		id: res.TxMeta.Id,
		s:  s,
	}, nil
}

// Transaction is a database transaction.
// Hence session methods are not goroutine safe, Transaction is not goroutine
// safe either.
type Transaction struct {
	id string
	s  *Session
	c  *TransactionControl
}

// Execute executes query represented by text within transaction tx.
func (tx *Transaction) Execute(
	ctx context.Context,
	query string, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
	return
}

// Execute executes prepared statement stmt within transaction tx.
func (tx *Transaction) ExecuteStatement(
	ctx context.Context,
	stmt *Statement, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	return
}

func (tx *Transaction) txc() *TransactionControl {
	if tx.c == nil {
		tx.c = TxControl(WithTx(tx))
	}
	return tx.c
}

// Commit commits specified active transaction.
func (tx *Transaction) Commit(ctx context.Context) (err error) {
	tx.s.c.traceCommitTransactionStart(ctx, tx)
	defer func() {
		tx.s.c.traceCommitTransactionDone(ctx, tx, err)
	}()
	req := Ydb_Table.CommitTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	}
	return tx.s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.CommitTransaction, &req, nil))
}

// Rollback performs a rollback of the specified active transaction.
func (tx *Transaction) Rollback(ctx context.Context) (err error) {
	tx.s.c.traceRollbackTransactionStart(ctx, tx)
	defer func() {
		tx.s.c.traceRollbackTransactionDone(ctx, tx, err)
	}()
	req := Ydb_Table.RollbackTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	}
	return tx.s.c.Driver.Call(ctx, internal.Wrap(Ydb_Table_V1.RollbackTransaction, &req, nil))
}

func (t *Client) traceCreateSessionStart(ctx context.Context) {
	x := CreateSessionStartInfo{
		Context: ctx,
	}
	if a := t.Trace.CreateSessionStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).CreateSessionStart; b != nil {
		b(x)
	}
}
func (t *Client) traceCreateSessionDone(ctx context.Context, s *Session, err error) {
	x := CreateSessionDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := t.Trace.CreateSessionDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).CreateSessionDone; b != nil {
		b(x)
	}
}
func (t *Client) traceKeepAliveStart(ctx context.Context, s *Session) {
	x := KeepAliveStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := t.Trace.KeepAliveStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).KeepAliveStart; b != nil {
		b(x)
	}
}
func (t *Client) traceKeepAliveDone(ctx context.Context, s *Session, err error) {
	x := KeepAliveDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := t.Trace.KeepAliveDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).KeepAliveDone; b != nil {
		b(x)
	}
}
func (t *Client) traceDeleteSessionStart(ctx context.Context, s *Session) {
	x := DeleteSessionStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := t.Trace.DeleteSessionStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).DeleteSessionStart; b != nil {
		b(x)
	}
}
func (t *Client) traceDeleteSessionDone(ctx context.Context, s *Session, err error) {
	x := DeleteSessionDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := t.Trace.DeleteSessionDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).DeleteSessionDone; b != nil {
		b(x)
	}
}
func (t *Client) tracePrepareDataQueryStart(ctx context.Context, s *Session, query string) {
	x := PrepareDataQueryStartInfo{
		Context: ctx,
		Session: s,
		Query:   query,
	}
	if a := t.Trace.PrepareDataQueryStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).PrepareDataQueryStart; b != nil {
		b(x)
	}
}

func (t *Client) tracePrepareDataQueryDone(ctx context.Context, s *Session, query string, q *DataQuery, cached bool, err error) {
	x := PrepareDataQueryDoneInfo{
		Context: ctx,
		Session: s,
		Query:   query,
		Result:  q,
		Cached:  cached,
		Error:   err,
	}
	if a := t.Trace.PrepareDataQueryDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).PrepareDataQueryDone; b != nil {
		b(x)
	}
}
func (t *Client) traceExecuteDataQueryStart(ctx context.Context, s *Session, tx *TransactionControl, query *DataQuery, params *QueryParameters) {
	x := ExecuteDataQueryStartInfo{
		Context:    ctx,
		Session:    s,
		TxID:       tx.id(),
		Query:      query,
		Parameters: params,
	}
	if a := t.Trace.ExecuteDataQueryStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).ExecuteDataQueryStart; b != nil {
		b(x)
	}
}
func (t *Client) traceExecuteDataQueryDone(ctx context.Context, s *Session, tx *TransactionControl, query *DataQuery, params *QueryParameters, txr *Transaction, r *Result, err error) {
	x := ExecuteDataQueryDoneInfo{
		Context:    ctx,
		Session:    s,
		Query:      query,
		Parameters: params,
		Result:     r,
		Error:      err,
	}
	if txr != nil {
		x.TxID = txr.id
	}
	if a := t.Trace.ExecuteDataQueryDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).ExecuteDataQueryDone; b != nil {
		b(x)
	}
}
func (t *Client) traceBeginTransactionStart(ctx context.Context, s *Session) {
	x := BeginTransactionStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := t.Trace.BeginTransactionStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).BeginTransactionStart; b != nil {
		b(x)
	}
}
func (t *Client) traceBeginTransactionDone(ctx context.Context, s *Session, tx *Transaction, err error) {
	x := BeginTransactionDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if tx != nil {
		x.TxID = tx.id
	}
	if a := t.Trace.BeginTransactionDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).BeginTransactionDone; b != nil {
		b(x)
	}
}
func (t *Client) traceCommitTransactionStart(ctx context.Context, tx *Transaction) {
	x := CommitTransactionStartInfo{
		Context: ctx,
		Session: tx.s,
		TxID:    tx.id,
	}
	if a := t.Trace.CommitTransactionStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).CommitTransactionStart; b != nil {
		b(x)
	}
}
func (t *Client) traceCommitTransactionDone(ctx context.Context, tx *Transaction, err error) {
	x := CommitTransactionDoneInfo{
		Context: ctx,
		Session: tx.s,
		Error:   err,
	}
	if tx != nil {
		x.TxID = tx.id
	}
	if a := t.Trace.CommitTransactionDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).CommitTransactionDone; b != nil {
		b(x)
	}
}
func (t *Client) traceRollbackTransactionStart(ctx context.Context, tx *Transaction) {
	x := RollbackTransactionStartInfo{
		Context: ctx,
		Session: tx.s,
		TxID:    tx.id,
	}
	if a := t.Trace.RollbackTransactionStart; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).RollbackTransactionStart; b != nil {
		b(x)
	}
}
func (t *Client) traceRollbackTransactionDone(ctx context.Context, tx *Transaction, err error) {
	x := RollbackTransactionDoneInfo{
		Context: ctx,
		Session: tx.s,
		Error:   err,
	}
	if tx != nil {
		x.TxID = tx.id
	}
	if a := t.Trace.RollbackTransactionDone; a != nil {
		a(x)
	}
	if b := ContextClientTrace(ctx).RollbackTransactionDone; b != nil {
		b(x)
	}
}

type DataQuery struct {
	query    Ydb_Table.Query
	queryID  Ydb_Table.Query_Id
	queryYQL Ydb_Table.Query_YqlText
}

func (q *DataQuery) String() string {
	var emptyID Ydb_Table.Query_Id
	if q.queryID == emptyID {
		return q.queryYQL.YqlText
	}
	return q.queryID.Id
}

func (q *DataQuery) ID() string {
	return q.queryID.Id
}

func (q *DataQuery) YQL() string {
	return q.queryYQL.YqlText
}

func (q *DataQuery) initFromText(s string) {
	q.queryID = Ydb_Table.Query_Id{} // Reset id field.
	q.queryYQL.YqlText = s
	q.query.Query = &q.queryYQL
}

func (q *DataQuery) initPrepared(id string) {
	q.queryYQL = Ydb_Table.Query_YqlText{} // Reset yql field.
	q.queryID.Id = id
	q.query.Query = &q.queryID
}

type QueryParameters struct {
	m queryParams
}

func (q *QueryParameters) params() queryParams {
	if q == nil {
		return nil
	}
	return q.m
}

func (q *QueryParameters) Each(it func(name string, value ydb.Value)) {
	if q == nil {
		return
	}
	for key, value := range q.m {
		it(key, internal.ValueFromYDB(
			internal.TypeFromYDB(value.Type),
			value.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, value ydb.Value) {
		buf.WriteString("((")
		buf.WriteString(name)
		buf.WriteByte(')')
		buf.WriteByte('(')
		internal.WriteValueStringTo(&buf, value)
		buf.WriteString("))")
	})
	buf.WriteByte(')')
	return buf.String()
}

type queryParams map[string]*Ydb.TypedValue

type ParameterOption func(queryParams)

func NewQueryParameters(opts ...ParameterOption) *QueryParameters {
	q := &QueryParameters{
		m: make(queryParams),
	}
	q.Add(opts...)
	return q
}

func (q *QueryParameters) Add(opts ...ParameterOption) {
	for _, opt := range opts {
		opt(q.m)
	}
}

func ValueParam(name string, v ydb.Value) ParameterOption {
	return func(q queryParams) {
		q[name] = internal.ValueToYDB(v)
	}
}
