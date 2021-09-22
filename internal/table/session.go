package table

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// client contains logic of creation of ydb table sessions.
type client struct {
	//trace   sessiontrace.Trace
	cluster cluster.DB
	pool    SessionProvider
}

type ClientOption func(c *client)

/*func WithClientTraceOption(sessiontrace sessiontrace.Trace) ClientOption {
	return func(c *client) {
		c.trace = sessiontrace
	}
}
*/
// CreateSession creates new session instance.
// Unused sessions must be destroyed.
func (c *client) CreateSession(ctx context.Context) (s *Session, err error) {
	/*createSessionDone := table.clientTraceOnCreateSession(c.trace, ctx)
	start := time.Now()
	defer func() {
		createSessionDone(ctx, s, s.Address(), time.Since(start), err)
	}()*/
	var (
		response *Ydb_Table.CreateSessionResponse
		result   Ydb_Table.CreateSessionResult
	)
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	var cc cluster.ClientConnInterface
	response, err = Ydb_Table_V1.NewTableServiceClient(c.cluster).CreateSession(
		cluster.WithClientConnApplier(
			ctx,
			func(c cluster.ClientConnInterface) {
				cc = c
			},
		),
		&Ydb_Table.CreateSessionRequest{},
	)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
	}
	s = &Session{
		ID:           result.SessionId,
		conn:         cc,
		tableService: Ydb_Table_V1.NewTableServiceClient(cc),
		c:            *c,
	}
	return
}

func (c *client) Retry(ctx context.Context, retryNoIdempotent bool, op RetryOperation) (err error, issues []error) {
	return c.pool.Retry(ctx, retryNoIdempotent, op)
}

// Close closes session client instance.
func (c *client) Close(ctx context.Context) (err error) {
	return c.pool.Close(ctx)
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

	conn         cluster.ClientConnInterface
	tableService Ydb_Table_V1.TableServiceClient
	c            client
	closeMux     sync.Mutex
	closed       bool
	onClose      []func()
}

func (s *Session) OnClose(cb func()) {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()
	if s.closed {
		return
	}
	s.onClose = append(s.onClose, cb)
}

func (s *Session) Close(ctx context.Context) (err error) {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	//deleteSessionDone := table.clientTraceOnDeleteSession(s.c.sessiontrace, ctx, s)
	//start := time.Now()
	defer func() {
		for _, cb := range s.onClose {
			cb()
		}
		//deleteSessionDone(ctx, s, time.Since(start), err)
	}()
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	_, err = s.tableService.DeleteSession(ctx, &Ydb_Table.DeleteSessionRequest{
		SessionId: s.ID,
	})
	return err
}

func (s *Session) Address() string {
	if s != nil && s.conn != nil {
		return s.conn.Addr().String()
	}
	return ""
}

// KeepAlive keeps idle session alive.
func (s *Session) KeepAlive(ctx context.Context) (info options.SessionInfo, err error) {
	//keepAliveDone := table.clientTraceOnKeepAlive(s.c.sessiontrace, ctx, s)
	//defer func() {
	//	keepAliveDone(ctx, s, info, err)
	//}()
	var result Ydb_Table.KeepAliveResult
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	if s == nil {
		panic("nil session")
	}
	if s.tableService == nil {
		panic("nil table service")
	}
	resp, err := s.tableService.KeepAlive(ctx, &Ydb_Table.KeepAliveRequest{
		SessionId: s.ID,
	})
	if err != nil {
		return
	}
	err = proto.Unmarshal(resp.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}
	switch result.SessionStatus {
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_READY:
		info.Status = options.SessionReady
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY:
		info.Status = options.SessionBusy
	}
	return
}

// CreateTable creates table at given path with given options.
func (s *Session) CreateTable(ctx context.Context, path string, opts ...options.CreateTableOption) (err error) {
	request := Ydb_Table.CreateTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*options.CreateTableDesc)(&request))
	}
	_, err = s.tableService.CreateTable(ctx, &request)
	return err
}

// DescribeTable describes table at given path.
func (s *Session) DescribeTable(ctx context.Context, path string, opts ...options.DescribeTableOption) (desc options.Description, err error) {
	var (
		response *Ydb_Table.DescribeTableResponse
		result   Ydb_Table.DescribeTableResult
	)
	request := Ydb_Table.DescribeTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*options.DescribeTableDesc)(&request))
	}
	response, err = s.tableService.DescribeTable(ctx, &request)
	if err != nil {
		return desc, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}

	cs := make([]options.Column, len(result.GetColumns()))
	for i, c := range result.Columns {
		cs[i] = options.Column{
			Name:   c.GetName(),
			Type:   internal.TypeFromYDB(c.GetType()),
			Family: c.GetFamily(),
		}
	}

	rs := make([]options.KeyRange, len(result.GetShardKeyBounds())+1)
	var last types.Value
	for i, b := range result.GetShardKeyBounds() {
		if last != nil {
			rs[i].From = last
		}

		bound := internal.ValueFromYDB(b.GetType(), b.GetValue())
		rs[i].To = bound

		last = bound
	}
	if last != nil {
		i := len(rs) - 1
		rs[i].From = last
	}

	var stats *options.TableStats
	if result.GetTableStats() != nil {
		resStats := result.GetTableStats()
		partStats := make([]options.PartitionStats, len(result.GetTableStats().GetPartitionStats()))
		for i, v := range result.TableStats.PartitionStats {
			partStats[i].RowsEstimate = v.GetRowsEstimate()
			partStats[i].StoreSize = v.GetStoreSize()
		}
		var creationTime, modificationTime time.Time
		if resStats.CreationTime.GetSeconds() != 0 {
			creationTime = time.Unix(resStats.GetCreationTime().GetSeconds(), int64(resStats.GetCreationTime().GetNanos()))
		}
		if resStats.ModificationTime.GetSeconds() != 0 {
			modificationTime = time.Unix(resStats.GetModificationTime().GetSeconds(), int64(resStats.GetModificationTime().GetNanos()))
		}

		stats = &options.TableStats{
			PartitionStats:   partStats,
			RowsEstimate:     resStats.GetRowsEstimate(),
			StoreSize:        resStats.GetStoreSize(),
			Partitions:       resStats.GetPartitions(),
			CreationTime:     creationTime,
			ModificationTime: modificationTime,
		}
	}

	cf := make([]options.ColumnFamily, len(result.GetColumnFamilies()))
	for i, c := range result.GetColumnFamilies() {
		cf[i] = options.NewColumnFamily(c)
	}

	attrs := make(map[string]string, len(result.GetAttributes()))
	for k, v := range result.GetAttributes() {
		attrs[k] = v
	}

	indexes := make([]options.IndexDescription, len(result.Indexes))
	for i, idx := range result.GetIndexes() {
		indexes[i] = options.IndexDescription{
			Name:         idx.GetName(),
			IndexColumns: idx.GetIndexColumns(),
			Status:       idx.GetStatus(),
		}
	}

	return options.Description{
		Name:                 result.GetSelf().GetName(),
		PrimaryKey:           result.GetPrimaryKey(),
		Columns:              cs,
		KeyRanges:            rs,
		Stats:                stats,
		ColumnFamilies:       cf,
		Attributes:           attrs,
		ReadReplicaSettings:  options.NewReadReplicasSettings(result.GetReadReplicasSettings()),
		StorageSettings:      options.NewStorageSettings(result.GetStorageSettings()),
		KeyBloomFilter:       internal.FeatureFlagFromYDB(result.GetKeyBloomFilter()),
		PartitioningSettings: options.NewPartitioningSettings(result.GetPartitioningSettings()),
		Indexes:              indexes,
		TimeToLiveSettings:   options.NewTimeToLiveSettings(result.GetTtlSettings()),
	}, nil
}

// DropTable drops table at given path with given options.
func (s *Session) DropTable(ctx context.Context, path string, opts ...options.DropTableOption) (err error) {
	request := Ydb_Table.DropTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*options.DropTableDesc)(&request))
	}
	_, err = s.tableService.DropTable(ctx, &request)
	return err
}

// AlterTable modifies schema of table at given path with given options.
func (s *Session) AlterTable(ctx context.Context, path string, opts ...options.AlterTableOption) (err error) {
	request := Ydb_Table.AlterTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*options.AlterTableDesc)(&request))
	}
	_, err = s.tableService.AlterTable(ctx, &request)
	return err
}

// CopyTable creates copy of table at given path.
func (s *Session) CopyTable(ctx context.Context, dst, src string, opts ...options.CopyTableOption) (err error) {
	request := Ydb_Table.CopyTableRequest{
		SessionId:       s.ID,
		SourcePath:      src,
		DestinationPath: dst,
	}
	for _, opt := range opts {
		opt((*options.CopyTableDesc)(&request))
	}
	_, err = s.tableService.CopyTable(ctx, &request)
	return err
}

// DataQueryExplanation is a result of ExplainDataQuery call.
type DataQueryExplanation struct {
	AST  string
	Plan string
}

// Explain explains data query represented by text.
func (s *Session) Explain(ctx context.Context, query string) (exp DataQueryExplanation, err error) {
	var (
		result   Ydb_Table.ExplainQueryResult
		response *Ydb_Table.ExplainDataQueryResponse
	)
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	response, err = s.tableService.ExplainDataQuery(ctx, &Ydb_Table.ExplainDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	})
	if err != nil {
		return
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}
	return DataQueryExplanation{
		AST:  result.QueryAst,
		Plan: result.QueryPlan,
	}, nil
}

// Statement is a prepared statement. Like a single Session, it is not safe for
// concurrent use by multiple goroutines.
type Statement struct {
	session *Session
	query   *DataQuery
	params  map[string]*Ydb.Type
}

// Execute executes prepared data query.
func (s *Statement) Execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr *Transaction, r resultset.Result, err error,
) {
	//executeDataQueryDone := table.clientTraceOnExecuteDataQuery(s.session.c.sessiontrace, ctx, s.session, tx.id(), s.query, params)
	//defer func() {
	//	executeDataQueryDone(ctx, s.session, GetTransactionID(txr), s.query, params, true, r, err)
	//}()
	return s.execute(ctx, tx, params, opts...)
}

// execute executes prepared query without any tracing.
func (s *Statement) execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr *Transaction, r resultset.Result, err error,
) {
	_, res, err := s.session.executeDataQuery(ctx, tx, s.query, params, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s.session.executeQueryResult(res)
}

func (s *Statement) NumInput() int {
	return len(s.params)
}

func (s *Statement) Text() string {
	return s.query.YQL()
}

// Prepare prepares data query within session s.
func (s *Session) Prepare(ctx context.Context, query string) (stmt *Statement, err error) {
	var (
		//cached   bool
		q        *DataQuery
		response *Ydb_Table.PrepareDataQueryResponse
		result   Ydb_Table.PrepareQueryResult
	)
	//prepareDataQueryDone := table.clientTraceOnPrepareDataQuery(s.c.sessiontrace, ctx, s, query)
	//defer func() {
	//	prepareDataQueryDone(ctx, s, query, q, cached, err)
	//}()

	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	response, err = s.tableService.PrepareDataQuery(ctx, &Ydb_Table.PrepareDataQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	})
	if err != nil {
		return
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}

	q = new(DataQuery)
	q.initPrepared(result.QueryId)
	stmt = &Statement{
		session: s,
		query:   q,
		params:  result.ParametersTypes,
	}

	return stmt, nil
}

// Execute executes given data query represented by text.
func (s *Session) Execute(
	ctx context.Context,
	tx *TransactionControl,
	query string,
	params *QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr *Transaction, r resultset.Result, err error,
) {
	q := new(DataQuery)
	q.initFromText(query)

	//executeDataQueryDone := table.clientTraceOnExecuteDataQuery(s.c.sessiontrace, ctx, s, tx.id(), q, params)
	//defer func() {
	//	executeDataQueryDone(ctx, s, GetTransactionID(txr), q, params, true, r, err)
	//}()

	request, result, err := s.executeDataQuery(ctx, tx, q, params, opts...)
	if err != nil {
		return nil, nil, err
	}
	if keepInCache(request) && result.QueryMeta != nil {
		queryID := result.QueryMeta.Id
		// Supplement q with ID for tracing.
		q.initPreparedText(query, queryID)
		// Create new DataQuery instead of q above to not store the whole query
		// string within statement.
		subq := new(DataQuery)
		subq.initPrepared(queryID)
	}

	return s.executeQueryResult(result)
}

func keepInCache(req *Ydb_Table.ExecuteDataQueryRequest) bool {
	p := req.QueryCachePolicy
	return p != nil && p.KeepInCache
}

// executeQueryResult returns Transaction and Result built from received
// result.
func (s *Session) executeQueryResult(res *Ydb_Table.ExecuteQueryResult) (*Transaction, resultset.Result, error) {
	t := &Transaction{
		id: res.GetTxMeta().GetId(),
		s:  s,
	}
	r := &scanner.Result{
		Sets:       res.GetResultSets(),
		QueryStats: res.GetQueryStats(),
	}
	return t, r, nil
}

// executeDataQuery executes data query.
func (s *Session) executeDataQuery(
	ctx context.Context, tx *TransactionControl,
	query *DataQuery, params *QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	request *Ydb_Table.ExecuteDataQueryRequest,
	result *Ydb_Table.ExecuteQueryResult,
	err error,
) {
	var (
		response *Ydb_Table.ExecuteDataQueryResponse
	)
	result = &Ydb_Table.ExecuteQueryResult{}
	request = &Ydb_Table.ExecuteDataQueryRequest{
		SessionId:  s.ID,
		TxControl:  &tx.desc,
		Parameters: params.params(),
		Query:      &query.query,
	}
	for _, opt := range opts {
		opt((*options.ExecuteDataQueryDesc)(request))
	}
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	response, err = s.tableService.ExecuteDataQuery(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), result)
	return
}

// ExecuteSchemeQuery executes scheme query.
func (s *Session) ExecuteSchemeQuery(ctx context.Context, query string, opts ...options.ExecuteSchemeQueryOption) (err error) {
	request := Ydb_Table.ExecuteSchemeQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	for _, opt := range opts {
		opt((*options.ExecuteSchemeQueryDesc)(&request))
	}
	_, err = s.tableService.ExecuteSchemeQuery(ctx, &request)
	return err
}

// DescribeTableOptions describes supported table options.
func (s *Session) DescribeTableOptions(ctx context.Context) (desc options.TableOptionsDescription, err error) {
	var (
		response *Ydb_Table.DescribeTableOptionsResponse
		result   Ydb_Table.DescribeTableOptionsResult
	)
	request := Ydb_Table.DescribeTableOptionsRequest{}
	response, err = s.tableService.DescribeTableOptions(ctx, &request)
	if err != nil {
		return
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}
	{
		xs := make([]options.TableProfileDescription, len(result.GetTableProfilePresets()))
		for i, p := range result.GetTableProfilePresets() {
			xs[i] = options.TableProfileDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),

				DefaultStoragePolicy:      p.GetDefaultStoragePolicy(),
				DefaultCompactionPolicy:   p.GetDefaultCompactionPolicy(),
				DefaultPartitioningPolicy: p.GetDefaultPartitioningPolicy(),
				DefaultExecutionPolicy:    p.GetDefaultExecutionPolicy(),
				DefaultReplicationPolicy:  p.GetDefaultReplicationPolicy(),
				DefaultCachingPolicy:      p.GetDefaultCachingPolicy(),

				AllowedStoragePolicies:      p.GetAllowedStoragePolicies(),
				AllowedCompactionPolicies:   p.GetAllowedCompactionPolicies(),
				AllowedPartitioningPolicies: p.GetAllowedPartitioningPolicies(),
				AllowedExecutionPolicies:    p.GetAllowedExecutionPolicies(),
				AllowedReplicationPolicies:  p.GetAllowedReplicationPolicies(),
				AllowedCachingPolicies:      p.GetAllowedCachingPolicies(),
			}
		}
		desc.TableProfilePresets = xs
	}
	{
		xs := make([]options.StoragePolicyDescription, len(result.GetStoragePolicyPresets()))
		for i, p := range result.GetStoragePolicyPresets() {
			xs[i] = options.StoragePolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.StoragePolicyPresets = xs
	}
	{
		xs := make([]options.CompactionPolicyDescription, len(result.GetCompactionPolicyPresets()))
		for i, p := range result.GetCompactionPolicyPresets() {
			xs[i] = options.CompactionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.CompactionPolicyPresets = xs
	}
	{
		xs := make([]options.PartitioningPolicyDescription, len(result.GetPartitioningPolicyPresets()))
		for i, p := range result.GetPartitioningPolicyPresets() {
			xs[i] = options.PartitioningPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.PartitioningPolicyPresets = xs
	}
	{
		xs := make([]options.ExecutionPolicyDescription, len(result.GetExecutionPolicyPresets()))
		for i, p := range result.GetExecutionPolicyPresets() {
			xs[i] = options.ExecutionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ExecutionPolicyPresets = xs
	}
	{
		xs := make([]options.ReplicationPolicyDescription, len(result.GetReplicationPolicyPresets()))
		for i, p := range result.GetReplicationPolicyPresets() {
			xs[i] = options.ReplicationPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ReplicationPolicyPresets = xs
	}
	{
		xs := make([]options.CachingPolicyDescription, len(result.GetCachingPolicyPresets()))
		for i, p := range result.GetCachingPolicyPresets() {
			xs[i] = options.CachingPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.CachingPolicyPresets = xs
	}
	return desc, nil
}

// StreamReadTable reads table at given path with given options.
//
// Note that given ctx controls the lifetime of the whole read, not only this
// StreamReadTable() call; that is, the time until returned result is closed
// via Close() call or fully drained by sequential NextSet() calls.
func (s *Session) StreamReadTable(ctx context.Context, path string, opts ...options.ReadTableOption) (_ resultset.Result, err error) {
	var (
		request = Ydb_Table.ReadTableRequest{
			SessionId: s.ID,
			Path:      path,
		}
		response Ydb_Table.ReadTableResponse
		client   Ydb_Table_V1.TableService_StreamReadTableClient
	)
	for _, opt := range opts {
		opt((*options.ReadTableDesc)(&request))
	}

	ctx, cancel := context.WithCancel(ctx)

	client, err = s.tableService.StreamReadTable(ctx, &request)

	//streamReadTableDone := table.clientTraceOnStreamReadTable(s.c.sessiontrace, ctx, s)
	if err != nil {
		cancel()
		//streamReadTableDone(ctx, s, nil, err)
		return nil, err
	}

	r := &scanner.Result{
		SetCh:       make(chan *Ydb.ResultSet, 1),
		SetChCancel: cancel,
	}
	go func() {
		defer func() {
			close(r.SetCh)
			cancel()
			//streamReadTableDone(ctx, s, r, err)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = client.RecvMsg(&response)
				if err != nil {
					if err != io.EOF {
						r.SetChErr = &err
					}
					return
				}
				if result := response.GetResult(); result != nil {
					if resultSet := result.GetResultSet(); resultSet != nil {
						r.SetCh <- resultSet
					}
				}
			}
		}
	}()
	return r, nil
}

// StreamExecuteScanQuery scan-reads table at given path with given options.
//
// Note that given ctx controls the lifetime of the whole read, not only this
// StreamExecuteScanQuery() call; that is, the time until returned result is closed
// via Close() call or fully drained by sequential NextSet() calls.
func (s *Session) StreamExecuteScanQuery(ctx context.Context, query string, params *QueryParameters, opts ...options.ExecuteScanQueryOption) (_ resultset.Result, err error) {
	q := new(DataQuery)
	q.initFromText(query)
	var (
		request = Ydb_Table.ExecuteScanQueryRequest{
			Query:      &q.query,
			Parameters: params.params(),
			Mode:       Ydb_Table.ExecuteScanQueryRequest_MODE_EXEC, // set default
		}
		response Ydb_Table.ExecuteScanQueryPartialResponse
		client   Ydb_Table_V1.TableService_StreamExecuteScanQueryClient
	)
	for _, opt := range opts {
		opt((*options.ExecuteScanQueryDesc)(&request))
	}

	ctx, cancel := context.WithCancel(ctx)

	client, err = s.tableService.StreamExecuteScanQuery(ctx, &request)

	//streamExecuteScanQueryDone := table.clientTraceOnStreamExecuteScanQuery(s.c.sessiontrace, ctx, s, q, params)
	if err != nil {
		cancel()
		//streamExecuteScanQueryDone(ctx, s, q, params, nil, err)
		return nil, err
	}

	r := &scanner.Result{
		SetCh:       make(chan *Ydb.ResultSet, 1),
		SetChCancel: cancel,
	}
	go func() {
		defer func() {
			close(r.SetCh)
			cancel()
			//streamExecuteScanQueryDone(ctx, s, q, params, r, err)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = client.RecvMsg(&response)
				if err != nil {
					if err != io.EOF {
						r.SetChErr = &err
					}
					return
				}
				if result := response.GetResult(); result != nil {
					if resultSet := result.GetResultSet(); resultSet != nil {
						r.SetCh <- resultSet
					}
					if stats := result.GetQueryStats(); stats != nil {
						r.QueryStats = stats
					}
				}
			}
		}
	}()
	return r, nil
}

// BulkUpsert uploads given list of ydb struct values to the table.
func (s *Session) BulkUpsert(ctx context.Context, table string, rows types.Value) (err error) {
	_, err = s.tableService.BulkUpsert(ctx, &Ydb_Table.BulkUpsertRequest{
		Table: table,
		Rows:  internal.ValueToYDB(rows),
	})
	return err
}

// BeginTransaction begins new transaction within given session with given
// settings.
func (s *Session) BeginTransaction(ctx context.Context, tx *TransactionSettings) (x *Transaction, err error) {
	//beginTransactionDone := table.clientTraceOnBeginTransaction(s.c.sessiontrace, ctx, s)
	//defer func() {
	//	beginTransactionDone(ctx, s, GetTransactionID(x), err)
	//}()
	var (
		result   Ydb_Table.BeginTransactionResult
		response *Ydb_Table.BeginTransactionResponse
	)
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	response, err = s.tableService.BeginTransaction(ctx, &Ydb_Table.BeginTransactionRequest{
		SessionId:  s.ID,
		TxSettings: &tx.settings,
	})
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}
	return &Transaction{
		id: result.GetTxMeta().GetId(),
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
	opts ...options.ExecuteDataQueryOption,
) (r resultset.Result, err error) {
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
	return
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *Transaction) ExecuteStatement(
	ctx context.Context,
	stmt *Statement, params *QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (r resultset.Result, err error) {
	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	return
}

// CommitTx commits specified active transaction.
func (tx *Transaction) CommitTx(ctx context.Context, opts ...options.CommitTransactionOption) (r resultset.Result, err error) {
	//commitTransactionDone := table.clientTraceOnCommitTransaction(tx.s.c.sessiontrace, ctx, tx.s, tx.id)
	//defer func() {
	//	commitTransactionDone(ctx, tx.s, tx.id, err)
	//}()
	var (
		request = &Ydb_Table.CommitTransactionRequest{
			SessionId: tx.s.ID,
			TxId:      tx.id,
		}
		response *Ydb_Table.CommitTransactionResponse
		result   = new(Ydb_Table.CommitTransactionResult)
	)
	for _, opt := range opts {
		opt((*options.CommitTransactionDesc)(request))
	}
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	response, err = tx.s.tableService.CommitTransaction(ctx, request)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), result)
	if err != nil {
		return nil, err
	}
	return &scanner.Result{QueryStats: result.GetQueryStats()}, nil
}

// Rollback performs a rollback of the specified active transaction.
func (tx *Transaction) Rollback(ctx context.Context) (err error) {
	//rollbackTransactionDone := table.clientTraceOnRollbackTransaction(tx.s.c.sessiontrace, ctx, tx.s, tx.id)
	//defer func() {
	//	rollbackTransactionDone(ctx, tx.s, tx.id, err)
	//}()
	if m, _ := operation.ContextOperationMode(ctx); m == operation.OperationModeUnknown {
		ctx = operation.WithOperationMode(ctx, operation.OperationModeSync)
	}
	_, err = tx.s.tableService.RollbackTransaction(ctx, &Ydb_Table.RollbackTransactionRequest{
		SessionId: tx.s.ID,
		TxId:      tx.id,
	})
	return err
}

func (tx *Transaction) txc() *TransactionControl {
	if tx.c == nil {
		tx.c = TxControl(WithTx(tx))
	}
	return tx.c
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

func (q *DataQuery) initPreparedText(s, id string) {
	q.queryYQL = Ydb_Table.Query_YqlText{} // Reset yql field.
	q.queryYQL.YqlText = s

	q.queryID = Ydb_Table.Query_Id{} // Reset id field.
	q.queryID.Id = id

	q.query.Query = &q.queryID // Prefer preared query.
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

func (q *QueryParameters) Each(it func(name string, value types.Value)) {
	if q == nil {
		return
	}
	for key, value := range q.m {
		it(key, internal.ValueFromYDB(
			value.Type,
			value.Value,
		))
	}
}

func (q *QueryParameters) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	q.Each(func(name string, value types.Value) {
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
		q[name] = internal.ValueToYDB(v)
	}
}

func GetTransactionID(txr *Transaction) string {
	if txr != nil {
		return txr.id
	}
	return ""
}

// Transaction control options
type (
	txDesc   Ydb_Table.TransactionSettings
	TxOption func(*txDesc)
)

type TransactionSettings struct {
	settings Ydb_Table.TransactionSettings
}

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

func WithTx(t *Transaction) TxControlOption {
	return func(d *txControlDesc) {
		d.TxSelector = &Ydb_Table.TransactionControl_TxId{
			TxId: t.id,
		}
	}
}

func CommitTx() TxControlOption {
	return func(d *txControlDesc) {
		d.CommitTx = true
	}
}

var (
	serializableReadWrite = &Ydb_Table.TransactionSettings_SerializableReadWrite{
		SerializableReadWrite: &Ydb_Table.SerializableModeSettings{},
	}
	staleReadOnly = &Ydb_Table.TransactionSettings_StaleReadOnly{
		StaleReadOnly: &Ydb_Table.StaleModeSettings{},
	}
)

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

type txOnlineReadOnly Ydb_Table.OnlineModeSettings

type TxOnlineReadOnlyOption func(*txOnlineReadOnly)

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

func (t *TransactionControl) id() string {
	if tx, ok := t.desc.TxSelector.(*Ydb_Table.TransactionControl_TxId); ok {
		return tx.TxId
	}
	return ""
}

func TxControl(opts ...TxControlOption) *TransactionControl {
	c := new(TransactionControl)
	for _, opt := range opts {
		opt((*txControlDesc)(&c.desc))
	}
	return c
}
