package table

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

// Client contains logic of creation of ydb table sessions.
type Client struct {
	trace   ClientTrace
	cluster ydb.Cluster
}

type ClientOption func(c *Client)

func WithClientTraceOption(trace ClientTrace) ClientOption {
	return func(c *Client) {
		c.trace = trace
	}
}

func NewClient(cluster ydb.Cluster, opts ...ClientOption) *Client {
	c := &Client{
		cluster: cluster,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CreateSession creates new session instance.
// Unused sessions must be destroyed.
func (c *Client) CreateSession(ctx context.Context) (s *Session, err error) {
	clientTraceCreateSessionDone := clientTraceOnCreateSession(ctx, c.trace, ctx)
	start := time.Now()
	defer func() {
		clientTraceCreateSessionDone(ctx, s, s.Address(), time.Since(start), err)
	}()
	var (
		response *Ydb_Table.CreateSessionResponse
		result   Ydb_Table.CreateSessionResult
	)
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	var conn ydb.ClientConnInterface
	response, err = Ydb_Table_V1.NewTableServiceClient(c.cluster).CreateSession(
		ydb.WithClientConnApplier(
			ctx,
			func(c ydb.ClientConnInterface) {
				conn = c
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
		conn:         conn,
		tableService: Ydb_Table_V1.NewTableServiceClient(conn),
		c:            *c,
	}
	return
}

// Close closes session client instance.
func (c *Client) Close() (err error) {
	return c.cluster.Close()
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

	conn         ydb.ClientConnInterface
	tableService Ydb_Table_V1.TableServiceClient
	c            Client
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
	clientTraceDeleteSessionDone := clientTraceOnDeleteSession(ctx, s.c.trace, ctx, s)
	start := time.Now()
	defer func() {
		for _, cb := range s.onClose {
			cb()
		}
		clientTraceDeleteSessionDone(ctx, s, time.Since(start), err)
	}()
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	_, err = s.tableService.DeleteSession(ctx, &Ydb_Table.DeleteSessionRequest{
		SessionId: s.ID,
	})
	return err
}

func (s *Session) Address() string {
	if s != nil && s.conn != nil {
		return s.conn.Address()
	}
	return ""
}

// KeepAlive keeps idle session alive.
func (s *Session) KeepAlive(ctx context.Context) (info SessionInfo, err error) {
	clientTraceKeepAliveDone := clientTraceOnKeepAlive(ctx, s.c.trace, ctx, s)
	defer func() {
		clientTraceKeepAliveDone(ctx, s, info, err)
	}()
	var result Ydb_Table.KeepAliveResult
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
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
		info.Status = SessionReady
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY:
		info.Status = SessionBusy
	}
	return
}

// CreateTable creates table at given path with given options.
func (s *Session) CreateTable(ctx context.Context, path string, opts ...CreateTableOption) (err error) {
	request := Ydb_Table.CreateTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*createTableDesc)(&request))
	}
	_, err = s.tableService.CreateTable(ctx, &request)
	return err
}

// DescribeTable describes table at given path.
func (s *Session) DescribeTable(ctx context.Context, path string, opts ...DescribeTableOption) (desc Description, err error) {
	var (
		response *Ydb_Table.DescribeTableResponse
		result   Ydb_Table.DescribeTableResult
	)
	request := Ydb_Table.DescribeTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*describeTableDesc)(&request))
	}
	response, err = s.tableService.DescribeTable(ctx, &request)
	if err != nil {
		return desc, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}

	cs := make([]Column, len(result.GetColumns()))
	for i, c := range result.Columns {
		cs[i] = Column{
			Name:   c.GetName(),
			Type:   internal.TypeFromYDB(c.GetType()),
			Family: c.GetFamily(),
		}
	}

	rs := make([]KeyRange, len(result.GetShardKeyBounds())+1)
	var last ydb.Value
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

	var stats *TableStats
	if result.GetTableStats() != nil {
		resStats := result.GetTableStats()
		partStats := make([]PartitionStats, len(result.GetTableStats().GetPartitionStats()))
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

		stats = &TableStats{
			PartitionStats:   partStats,
			RowsEstimate:     resStats.GetRowsEstimate(),
			StoreSize:        resStats.GetStoreSize(),
			Partitions:       resStats.GetPartitions(),
			CreationTime:     creationTime,
			ModificationTime: modificationTime,
		}
	}

	cf := make([]ColumnFamily, len(result.GetColumnFamilies()))
	for i, c := range result.GetColumnFamilies() {
		cf[i] = columnFamily(c)
	}

	attrs := make(map[string]string, len(result.GetAttributes()))
	for k, v := range result.GetAttributes() {
		attrs[k] = v
	}

	indexes := make([]IndexDescription, len(result.Indexes))
	for i, idx := range result.GetIndexes() {
		indexes[i] = IndexDescription{
			Name:         idx.GetName(),
			IndexColumns: idx.GetIndexColumns(),
			Status:       idx.GetStatus(),
		}
	}

	return Description{
		Name:                 result.GetSelf().GetName(),
		PrimaryKey:           result.GetPrimaryKey(),
		Columns:              cs,
		KeyRanges:            rs,
		Stats:                stats,
		ColumnFamilies:       cf,
		Attributes:           attrs,
		ReadReplicaSettings:  readReplicasSettings(result.GetReadReplicasSettings()),
		StorageSettings:      storageSettings(result.GetStorageSettings()),
		KeyBloomFilter:       internal.FeatureFlagFromYDB(result.GetKeyBloomFilter()),
		PartitioningSettings: partitioningSettings(result.GetPartitioningSettings()),
		Indexes:              indexes,
		TimeToLiveSettings:   timeToLiveSettings(result.GetTtlSettings()),
	}, nil
}

// DropTable drops table at given path with given options.
func (s *Session) DropTable(ctx context.Context, path string, opts ...DropTableOption) (err error) {
	request := Ydb_Table.DropTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*dropTableDesc)(&request))
	}
	_, err = s.tableService.DropTable(ctx, &request)
	return err
}

// AlterTable modifies schema of table at given path with given options.
func (s *Session) AlterTable(ctx context.Context, path string, opts ...AlterTableOption) (err error) {
	request := Ydb_Table.AlterTableRequest{
		SessionId: s.ID,
		Path:      path,
	}
	for _, opt := range opts {
		opt((*alterTableDesc)(&request))
	}
	_, err = s.tableService.AlterTable(ctx, &request)
	return err
}

// CopyTable creates copy of table at given path.
func (s *Session) CopyTable(ctx context.Context, dst, src string, opts ...CopyTableOption) (err error) {
	request := Ydb_Table.CopyTableRequest{
		SessionId:       s.ID,
		SourcePath:      src,
		DestinationPath: dst,
	}
	for _, opt := range opts {
		opt((*copyTableDesc)(&request))
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
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
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
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	clientTraceExecuteDataQueryDone := clientTraceOnExecuteDataQuery(ctx, s.session.c.trace, ctx, s.session, tx.id(), s.query, params)
	defer func() {
		clientTraceExecuteDataQueryDone(ctx, s.session, GetTransactionID(txr), s.query, params, true, r, err)
	}()
	return s.execute(ctx, tx, params, opts...)
}

// execute executes prepared query without any tracing.
func (s *Statement) execute(
	ctx context.Context, tx *TransactionControl,
	params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
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
func (s *Session) Prepare(
	ctx context.Context, query string,
) (
	stmt *Statement, err error,
) {
	var (
		cached   bool
		q        *DataQuery
		response *Ydb_Table.PrepareDataQueryResponse
		result   Ydb_Table.PrepareQueryResult
	)
	clientTracePrepareDataQueryDone := clientTraceOnPrepareDataQuery(ctx, s.c.trace, ctx, s, query)
	defer func() {
		clientTracePrepareDataQueryDone(ctx, s, query, q, cached, err)
	}()

	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
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
	opts ...ExecuteDataQueryOption,
) (
	txr *Transaction, r *Result, err error,
) {
	q := new(DataQuery)
	q.initFromText(query)

	clientTraceExecuteDataQueryDone := clientTraceOnExecuteDataQuery(ctx, s.c.trace, ctx, s, tx.id(), q, params)
	defer func() {
		clientTraceExecuteDataQueryDone(ctx, s, GetTransactionID(txr), q, params, true, r, err)
	}()

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
func (s *Session) executeQueryResult(res *Ydb_Table.ExecuteQueryResult) (*Transaction, *Result, error) {
	t := &Transaction{
		id: res.GetTxMeta().GetId(),
		s:  s,
	}
	r := &Result{
		sets:  res.GetResultSets(),
		stats: res.GetQueryStats(),
	}
	return t, r, nil
}

// executeDataQuery executes data query.
func (s *Session) executeDataQuery(
	ctx context.Context, tx *TransactionControl,
	query *DataQuery, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
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
		opt((*executeDataQueryDesc)(request))
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	response, err = s.tableService.ExecuteDataQuery(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), result)
	return
}

// ExecuteSchemeQuery executes scheme query.
func (s *Session) ExecuteSchemeQuery(
	ctx context.Context, query string,
	opts ...ExecuteSchemeQueryOption,
) (err error) {
	request := Ydb_Table.ExecuteSchemeQueryRequest{
		SessionId: s.ID,
		YqlText:   query,
	}
	for _, opt := range opts {
		opt((*executeSchemeQueryDesc)(&request))
	}
	_, err = s.tableService.ExecuteSchemeQuery(ctx, &request)
	return err
}

// DescribeTableOptions describes supported table options.
func (s *Session) DescribeTableOptions(ctx context.Context) (desc TableOptionsDescription, err error) {
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
		xs := make([]TableProfileDescription, len(result.GetTableProfilePresets()))
		for i, p := range result.GetTableProfilePresets() {
			xs[i] = TableProfileDescription{
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
		xs := make([]StoragePolicyDescription, len(result.GetStoragePolicyPresets()))
		for i, p := range result.GetStoragePolicyPresets() {
			xs[i] = StoragePolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.StoragePolicyPresets = xs
	}
	{
		xs := make([]CompactionPolicyDescription, len(result.GetCompactionPolicyPresets()))
		for i, p := range result.GetCompactionPolicyPresets() {
			xs[i] = CompactionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.CompactionPolicyPresets = xs
	}
	{
		xs := make([]PartitioningPolicyDescription, len(result.GetPartitioningPolicyPresets()))
		for i, p := range result.GetPartitioningPolicyPresets() {
			xs[i] = PartitioningPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.PartitioningPolicyPresets = xs
	}
	{
		xs := make([]ExecutionPolicyDescription, len(result.GetExecutionPolicyPresets()))
		for i, p := range result.GetExecutionPolicyPresets() {
			xs[i] = ExecutionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ExecutionPolicyPresets = xs
	}
	{
		xs := make([]ReplicationPolicyDescription, len(result.GetReplicationPolicyPresets()))
		for i, p := range result.GetReplicationPolicyPresets() {
			xs[i] = ReplicationPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ReplicationPolicyPresets = xs
	}
	{
		xs := make([]CachingPolicyDescription, len(result.GetCachingPolicyPresets()))
		for i, p := range result.GetCachingPolicyPresets() {
			xs[i] = CachingPolicyDescription{
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
func (s *Session) StreamReadTable(ctx context.Context, path string, opts ...ReadTableOption) (_ *Result, err error) {
	var (
		request = Ydb_Table.ReadTableRequest{
			SessionId: s.ID,
			Path:      path,
		}
		response Ydb_Table.ReadTableResponse
		client   Ydb_Table_V1.TableService_StreamReadTableClient
	)
	for _, opt := range opts {
		opt((*readTableDesc)(&request))
	}

	ctx, cancel := context.WithCancel(ctx)

	client, err = s.tableService.StreamReadTable(ctx, &request)

	clientTraceStreamReadTableDone := clientTraceOnStreamReadTable(ctx, s.c.trace, ctx, s)
	if err != nil {
		cancel()
		clientTraceStreamReadTableDone(ctx, s, nil, err)
		return nil, err
	}

	r := &Result{
		setCh:       make(chan *Ydb.ResultSet, 1),
		setChCancel: cancel,
	}
	go func() {
		defer func() {
			close(r.setCh)
			cancel()
			clientTraceStreamReadTableDone(ctx, s, r, err)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = client.RecvMsg(&response)
				if err != nil {
					if err != io.EOF {
						r.setChErr = &err
					}
					return
				}
				if result := response.GetResult(); result != nil {
					if resultSet := result.GetResultSet(); resultSet != nil {
						r.setCh <- resultSet
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
func (s *Session) StreamExecuteScanQuery(
	ctx context.Context,
	query string,
	params *QueryParameters,
	opts ...ExecuteScanQueryOption,
) (
	_ *Result, err error,
) {
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
		opt((*executeScanQueryDesc)(&request))
	}

	ctx, cancel := context.WithCancel(ctx)

	client, err = s.tableService.StreamExecuteScanQuery(ctx, &request)

	clientTraceStreamExecuteScanQueryDone := clientTraceOnStreamExecuteScanQuery(ctx, s.c.trace, ctx, s, q, params)
	if err != nil {
		cancel()
		clientTraceStreamExecuteScanQueryDone(ctx, s, q, params, nil, err)
		return nil, err
	}

	r := &Result{
		setCh:       make(chan *Ydb.ResultSet, 1),
		setChCancel: cancel,
	}
	go func() {
		defer func() {
			close(r.setCh)
			cancel()
			clientTraceStreamExecuteScanQueryDone(ctx, s, q, params, r, err)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = client.RecvMsg(&response)
				if err != nil {
					if err != io.EOF {
						r.setChErr = &err
					}
					return
				}
				if result := response.GetResult(); result != nil {
					if resultSet := result.GetResultSet(); resultSet != nil {
						r.setCh <- resultSet
					}
					if stats := result.GetQueryStats(); stats != nil {
						r.stats = stats
					}
				}
			}
		}
	}()
	return r, nil
}

// BulkUpsert uploads given list of ydb struct values to the table.
func (s *Session) BulkUpsert(ctx context.Context, table string, rows ydb.Value) (err error) {
	_, err = s.tableService.BulkUpsert(ctx, &Ydb_Table.BulkUpsertRequest{
		Table: table,
		Rows:  internal.ValueToYDB(rows),
	})
	return err
}

// BeginTransaction begins new transaction within given session with given
// settings.
func (s *Session) BeginTransaction(ctx context.Context, tx *TransactionSettings) (x *Transaction, err error) {
	clientTraceBeginTransactionDone := clientTraceOnBeginTransaction(ctx, s.c.trace, ctx, s)
	defer func() {
		clientTraceBeginTransactionDone(ctx, s, GetTransactionID(x), err)
	}()
	var (
		result   Ydb_Table.BeginTransactionResult
		response *Ydb_Table.BeginTransactionResponse
	)
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
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
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = tx.s.Execute(ctx, tx.txc(), query, params, opts...)
	return
}

// ExecuteStatement executes prepared statement stmt within transaction tx.
func (tx *Transaction) ExecuteStatement(
	ctx context.Context,
	stmt *Statement, params *QueryParameters,
	opts ...ExecuteDataQueryOption,
) (r *Result, err error) {
	_, r, err = stmt.Execute(ctx, tx.txc(), params, opts...)
	return
}

// CommitTx commits specified active transaction.
func (tx *Transaction) CommitTx(ctx context.Context, opts ...CommitTransactionOption) (r *Result, err error) {
	clientTraceCommitTransactionDone := clientTraceOnCommitTransaction(ctx, tx.s.c.trace, ctx, tx.s, tx.id)
	defer func() {
		clientTraceCommitTransactionDone(ctx, tx.s, tx.id, err)
	}()
	var (
		request = &Ydb_Table.CommitTransactionRequest{
			SessionId: tx.s.ID,
			TxId:      tx.id,
		}
		response *Ydb_Table.CommitTransactionResponse
		result   = new(Ydb_Table.CommitTransactionResult)
	)
	for _, opt := range opts {
		opt((*commitTransactionDesc)(request))
	}
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
	}
	response, err = tx.s.tableService.CommitTransaction(ctx, request)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), result)
	if err != nil {
		return nil, err
	}
	return &Result{stats: result.GetQueryStats()}, nil
}

// Rollback performs a rollback of the specified active transaction.
func (tx *Transaction) Rollback(ctx context.Context) (err error) {
	clientTraceRollbackTransactionDone := clientTraceOnRollbackTransaction(ctx, tx.s.c.trace, ctx, tx.s, tx.id)
	defer func() {
		clientTraceRollbackTransactionDone(ctx, tx.s, tx.id, err)
	}()
	if m, _ := ydb.ContextOperationMode(ctx); m == ydb.OperationModeUnknown {
		ctx = ydb.WithOperationMode(ctx, ydb.OperationModeSync)
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

func (q *QueryParameters) Each(it func(name string, value ydb.Value)) {
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

func ValueParam(name string, v ydb.Value) ParameterOption {
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
