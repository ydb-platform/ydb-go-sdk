package table

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/feature"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// session represents a single table API session.
//
// session methods are not goroutine safe. Simultaneous execution of requests
// are forbidden within a single session.
//
// Note that after session is no longer needed it should be destroyed by
// Close() call.
type session struct {
	id           string
	tableService Ydb_Table_V1.TableServiceClient
	config       config.Config

	status    table.SessionStatus
	statusMtx sync.RWMutex
	nodeID    uint32
	lastUsage int64

	onClose   []func(s *session)
	closeOnce sync.Once
}

func (s *session) LastUsage() time.Time {
	return time.Unix(atomic.LoadInt64(&s.lastUsage), 0)
}

func nodeID(sessionID string) (uint32, error) {
	u, err := url.Parse(sessionID)
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseUint(u.Query().Get("node_id"), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(id), err
}

func (s *session) NodeID() uint32 {
	if s == nil {
		return 0
	}
	if id := atomic.LoadUint32(&s.nodeID); id != 0 {
		return id
	}
	id, err := nodeID(s.id)
	if err != nil {
		return 0
	}
	atomic.StoreUint32(&s.nodeID, id)
	return id
}

func (s *session) Status() table.SessionStatus {
	if s == nil {
		return table.SessionStatusUnknown
	}
	s.statusMtx.RLock()
	defer s.statusMtx.RUnlock()
	return s.status
}

func (s *session) SetStatus(status table.SessionStatus) {
	s.statusMtx.Lock()
	defer s.statusMtx.Unlock()
	s.status = status
}

func (s *session) isClosed() bool {
	return s.Status() == table.SessionClosed
}

func (s *session) isClosing() bool {
	return s.Status() == table.SessionClosing
}

func newSession(ctx context.Context, cc grpc.ClientConnInterface, config config.Config, opts ...sessionBuilderOption) (
	s *session, err error,
) {
	onDone := trace.TableOnSessionNew(config.Trace(), &ctx)
	defer func() {
		onDone(s, err)
	}()
	var (
		response *Ydb_Table.CreateSessionResponse
		result   Ydb_Table.CreateSessionResult
	)
	c := Ydb_Table_V1.NewTableServiceClient(cc)
	response, err = c.CreateSession(
		ctx,
		&Ydb_Table.CreateSessionRequest{
			OperationParams: operation.Params(
				ctx,
				config.OperationTimeout(),
				config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	s = &session{
		id:        result.GetSessionId(),
		config:    config,
		status:    table.SessionReady,
		lastUsage: time.Now().Unix(),
	}

	s.tableService = Ydb_Table_V1.NewTableServiceClient(
		conn.WithBeforeFunc(
			conn.WithContextModifier(cc, func(ctx context.Context) context.Context {
				return meta.WithTrailerCallback(balancer.WithEndpoint(ctx, s), s.checkCloseHint)
			}),
			func() {
				atomic.StoreInt64(&s.lastUsage, time.Now().Unix())
			},
		),
	)

	for _, o := range opts {
		if o != nil {
			o(s)
		}
	}

	return s, nil
}

func (s *session) ID() string {
	if s == nil {
		return ""
	}
	return s.id
}

func (s *session) Close(ctx context.Context) (err error) {
	if s.isClosed() {
		return xerrors.WithStackTrace(errSessionClosed)
	}

	s.closeOnce.Do(func() {
		defer func() {
			s.SetStatus(table.SessionClosed)
		}()

		onDone := trace.TableOnSessionDelete(s.config.Trace(), &ctx, s)

		if time.Since(s.LastUsage()) < s.config.IdleThreshold() {
			_, err = s.tableService.DeleteSession(ctx,
				&Ydb_Table.DeleteSessionRequest{
					SessionId: s.id,
					OperationParams: operation.Params(ctx,
						s.config.OperationTimeout(),
						s.config.OperationCancelAfter(),
						operation.ModeSync,
					),
				},
			)
		}

		for _, onClose := range s.onClose {
			onClose(s)
		}

		onDone(err)
	})

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (s *session) checkCloseHint(md metadata.MD) {
	for header, values := range md {
		if header != meta.HeaderServerHints {
			continue
		}
		for _, hint := range values {
			if hint == meta.HintSessionClose {
				s.SetStatus(table.SessionClosing)
			}
		}
	}
}

// KeepAlive keeps idle session alive.
func (s *session) KeepAlive(ctx context.Context) (err error) {
	var (
		result Ydb_Table.KeepAliveResult
		onDone = trace.TableOnSessionKeepAlive(
			s.config.Trace(),
			&ctx,
			s,
		)
	)
	defer func() {
		onDone(err)
	}()

	resp, err := s.tableService.KeepAlive(ctx,
		&Ydb_Table.KeepAliveRequest{
			SessionId: s.id,
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return
	}
	err = resp.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return
	}
	switch result.SessionStatus {
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_READY:
		s.SetStatus(table.SessionReady)
	case Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY:
		s.SetStatus(table.SessionBusy)
	}
	return nil
}

// CreateTable creates table at given path with given options.
func (s *session) CreateTable(
	ctx context.Context,
	path string,
	opts ...options.CreateTableOption,
) (err error) {
	var (
		request = Ydb_Table.CreateTableRequest{
			SessionId: s.id,
			Path:      path,
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
		a = allocator.New()
	)
	defer a.Free()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyCreateTableOption((*options.CreateTableDesc)(&request), a)
		}
	}
	_, err = s.tableService.CreateTable(ctx, &request)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

// DescribeTable describes table at given path.
func (s *session) DescribeTable(
	ctx context.Context,
	path string,
	opts ...options.DescribeTableOption,
) (desc options.Description, err error) {
	var (
		response *Ydb_Table.DescribeTableResponse
		result   Ydb_Table.DescribeTableResult
	)
	request := Ydb_Table.DescribeTableRequest{
		SessionId: s.id,
		Path:      path,
		OperationParams: operation.Params(
			ctx,
			s.config.OperationTimeout(),
			s.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*options.DescribeTableDesc)(&request))
		}
	}
	response, err = s.tableService.DescribeTable(ctx, &request)
	if err != nil {
		return desc, xerrors.WithStackTrace(err)
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return desc, xerrors.WithStackTrace(err)
	}

	cs := make(
		[]options.Column,
		len(result.GetColumns()),
	)
	for i, c := range result.Columns {
		cs[i] = options.Column{
			Name:   c.GetName(),
			Type:   value.TypeFromYDB(c.GetType()),
			Family: c.GetFamily(),
		}
	}

	rs := make(
		[]options.KeyRange,
		len(result.GetShardKeyBounds())+1,
	)
	var last types.Value
	for i, b := range result.GetShardKeyBounds() {
		if last != nil {
			rs[i].From = last
		}

		bound := value.FromYDB(b.GetType(), b.GetValue())
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
		partStats := make(
			[]options.PartitionStats,
			len(result.GetTableStats().GetPartitionStats()),
		)
		for i, v := range result.TableStats.PartitionStats {
			partStats[i].RowsEstimate = v.GetRowsEstimate()
			partStats[i].StoreSize = v.GetStoreSize()
		}
		var creationTime, modificationTime time.Time
		if resStats.CreationTime.GetSeconds() != 0 {
			creationTime = time.Unix(
				resStats.GetCreationTime().GetSeconds(),
				int64(resStats.GetCreationTime().GetNanos()),
			)
		}
		if resStats.ModificationTime.GetSeconds() != 0 {
			modificationTime = time.Unix(
				resStats.GetModificationTime().GetSeconds(),
				int64(resStats.GetModificationTime().GetNanos()),
			)
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
		var typ options.IndexType
		switch idx.Type.(type) {
		case *Ydb_Table.TableIndexDescription_GlobalAsyncIndex:
			typ = options.IndexTypeGlobalAsync
		case *Ydb_Table.TableIndexDescription_GlobalIndex:
			typ = options.IndexTypeGlobal
		}
		indexes[i] = options.IndexDescription{
			Name:         idx.GetName(),
			IndexColumns: idx.GetIndexColumns(),
			DataColumns:  idx.GetDataColumns(),
			Status:       idx.GetStatus(),
			Type:         typ,
		}
	}

	changeFeeds := make([]options.ChangefeedDescription, len(result.Changefeeds))
	for i, proto := range result.GetChangefeeds() {
		changeFeeds[i] = options.NewChangefeedDescription(proto)
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
		KeyBloomFilter:       feature.FromYDB(result.GetKeyBloomFilter()),
		PartitioningSettings: options.NewPartitioningSettings(result.GetPartitioningSettings()),
		Indexes:              indexes,
		TimeToLiveSettings:   NewTimeToLiveSettings(result.GetTtlSettings()),
		Changefeeds:          changeFeeds,
	}, nil
}

// DropTable drops table at given path with given options.
func (s *session) DropTable(
	ctx context.Context,
	path string,
	opts ...options.DropTableOption,
) (err error) {
	request := Ydb_Table.DropTableRequest{
		SessionId: s.id,
		Path:      path,
		OperationParams: operation.Params(
			ctx,
			s.config.OperationTimeout(),
			s.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyDropTableOption((*options.DropTableDesc)(&request))
		}
	}
	_, err = s.tableService.DropTable(ctx, &request)
	return xerrors.WithStackTrace(err)
}

func (s *session) checkError(err error) {
	if err == nil {
		return
	}
	if m := retry.Check(err); m.MustDeleteSession() {
		s.SetStatus(table.SessionClosing)
	}
}

// AlterTable modifies schema of table at given path with given options.
func (s *session) AlterTable(
	ctx context.Context,
	path string,
	opts ...options.AlterTableOption,
) (err error) {
	var (
		request = Ydb_Table.AlterTableRequest{
			SessionId: s.id,
			Path:      path,
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
		a = allocator.New()
	)
	defer a.Free()
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyAlterTableOption((*options.AlterTableDesc)(&request), a)
		}
	}
	_, err = s.tableService.AlterTable(ctx, &request)
	return xerrors.WithStackTrace(err)
}

// CopyTable creates copy of table at given path.
func (s *session) CopyTable(
	ctx context.Context,
	dst, src string,
	opts ...options.CopyTableOption,
) (err error) {
	request := Ydb_Table.CopyTableRequest{
		SessionId:       s.id,
		SourcePath:      src,
		DestinationPath: dst,
		OperationParams: operation.Params(
			ctx,
			s.config.OperationTimeout(),
			s.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*options.CopyTableDesc)(&request))
		}
	}
	_, err = s.tableService.CopyTable(ctx, &request)
	return xerrors.WithStackTrace(err)
}

// Explain explains data query represented by text.
func (s *session) Explain(
	ctx context.Context,
	query string,
) (
	exp table.DataQueryExplanation,
	err error,
) {
	var (
		result   Ydb_Table.ExplainQueryResult
		response *Ydb_Table.ExplainDataQueryResponse
		onDone   = trace.TableOnSessionQueryExplain(
			s.config.Trace(),
			&ctx,
			s,
			query,
		)
	)

	defer func() {
		if err != nil {
			onDone("", "", err)
		} else {
			onDone(exp.AST, exp.AST, nil)
		}
	}()

	response, err = s.tableService.ExplainDataQuery(ctx,
		&Ydb_Table.ExplainDataQueryRequest{
			SessionId: s.id,
			YqlText:   query,
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return
	}
	return table.DataQueryExplanation{
		Explanation: table.Explanation{
			Plan: result.GetQueryPlan(),
		},
		AST: result.QueryAst,
	}, nil
}

// Prepare prepares data query within session s.
func (s *session) Prepare(ctx context.Context, queryText string) (_ table.Statement, err error) {
	var (
		stmt     *statement
		response *Ydb_Table.PrepareDataQueryResponse
		result   Ydb_Table.PrepareQueryResult
		onDone   = trace.TableOnSessionQueryPrepare(
			s.config.Trace(),
			&ctx,
			s,
			queryText,
		)
	)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(stmt.query, nil)
		}
	}()

	response, err = s.tableService.PrepareDataQuery(ctx,
		&Ydb_Table.PrepareDataQueryRequest{
			SessionId: s.id,
			YqlText:   queryText,
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return
	}

	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return
	}

	stmt = &statement{
		session: s,
		query:   queryPrepared(result.GetQueryId(), queryText),
		params:  result.ParametersTypes,
	}

	return stmt, nil
}

// Execute executes given data query represented by text.
func (s *session) Execute(
	ctx context.Context,
	txControl *table.TransactionControl,
	query string,
	params *table.QueryParameters,
	opts ...options.ExecuteDataQueryOption,
) (
	txr table.Transaction, r result.Result, err error,
) {
	var (
		a       = allocator.New()
		q       = queryFromText(query)
		request = a.TableExecuteDataQueryRequest()
	)
	defer a.Free()

	request.SessionId = s.id
	request.TxControl = txControl.Desc()
	request.Parameters = params.Params().ToYDB(a)
	request.Query = q.toYDB(a)
	request.QueryCachePolicy = a.TableQueryCachePolicy()
	request.QueryCachePolicy.KeepInCache = len(params.Params()) > 0
	request.OperationParams = operation.Params(ctx,
		s.config.OperationTimeout(),
		s.config.OperationCancelAfter(),
		operation.ModeSync,
	)

	for _, opt := range opts {
		if opt != nil {
			opt((*options.ExecuteDataQueryDesc)(request), a)
		}
	}

	onDone := trace.TableOnSessionQueryExecute(
		s.config.Trace(),
		&ctx,
		s,
		q,
		params,
		request.QueryCachePolicy.GetKeepInCache(),
	)
	defer func() {
		onDone(txr, false, r, err)
	}()

	result, err := s.executeDataQuery(ctx, a, request)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}

	return s.executeQueryResult(result, request.TxControl)
}

// executeQueryResult returns Transaction and result built from received
// result.
func (s *session) executeQueryResult(
	res *Ydb_Table.ExecuteQueryResult, txControl *Ydb_Table.TransactionControl,
) (
	table.Transaction, result.Result, error,
) {
	tx := &transaction{
		id: res.GetTxMeta().GetId(),
		s:  s,
	}
	if txControl.CommitTx {
		tx.state = txStateCommitted
	} else {
		tx.state = txStateInitialized
		tx.control = table.TxControl(table.WithTxID(tx.id))
	}
	return tx, scanner.NewUnary(
		res.GetResultSets(),
		res.GetQueryStats(),
		scanner.WithIgnoreTruncated(s.config.IgnoreTruncated()),
	), nil
}

// executeDataQuery executes data query.
func (s *session) executeDataQuery(
	ctx context.Context, a *allocator.Allocator, request *Ydb_Table.ExecuteDataQueryRequest,
) (
	_ *Ydb_Table.ExecuteQueryResult,
	err error,
) {
	var (
		result   = a.TableExecuteQueryResult()
		response *Ydb_Table.ExecuteDataQueryResponse
	)

	response, err = s.tableService.ExecuteDataQuery(ctx, request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = response.GetOperation().GetResult().UnmarshalTo(result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return result, nil
}

// ExecuteSchemeQuery executes scheme query.
func (s *session) ExecuteSchemeQuery(
	ctx context.Context,
	query string,
	opts ...options.ExecuteSchemeQueryOption,
) (err error) {
	request := Ydb_Table.ExecuteSchemeQueryRequest{
		SessionId: s.id,
		YqlText:   query,
		OperationParams: operation.Params(
			ctx,
			s.config.OperationTimeout(),
			s.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}
	for _, opt := range opts {
		if opt != nil {
			opt((*options.ExecuteSchemeQueryDesc)(&request))
		}
	}
	_, err = s.tableService.ExecuteSchemeQuery(ctx, &request)
	return xerrors.WithStackTrace(err)
}

// DescribeTableOptions describes supported table options.
func (s *session) DescribeTableOptions(ctx context.Context) (
	desc options.TableOptionsDescription,
	err error,
) {
	var (
		response *Ydb_Table.DescribeTableOptionsResponse
		result   Ydb_Table.DescribeTableOptionsResult
	)
	request := Ydb_Table.DescribeTableOptionsRequest{
		OperationParams: operation.Params(
			ctx,
			s.config.OperationTimeout(),
			s.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}
	response, err = s.tableService.DescribeTableOptions(ctx, &request)
	if err != nil {
		return
	}

	err = response.GetOperation().GetResult().UnmarshalTo(&result)
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
		xs := make(
			[]options.StoragePolicyDescription,
			len(result.GetStoragePolicyPresets()),
		)
		for i, p := range result.GetStoragePolicyPresets() {
			xs[i] = options.StoragePolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.StoragePolicyPresets = xs
	}
	{
		xs := make(
			[]options.CompactionPolicyDescription,
			len(result.GetCompactionPolicyPresets()),
		)
		for i, p := range result.GetCompactionPolicyPresets() {
			xs[i] = options.CompactionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.CompactionPolicyPresets = xs
	}
	{
		xs := make(
			[]options.PartitioningPolicyDescription,
			len(result.GetPartitioningPolicyPresets()),
		)
		for i, p := range result.GetPartitioningPolicyPresets() {
			xs[i] = options.PartitioningPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.PartitioningPolicyPresets = xs
	}
	{
		xs := make(
			[]options.ExecutionPolicyDescription,
			len(result.GetExecutionPolicyPresets()),
		)
		for i, p := range result.GetExecutionPolicyPresets() {
			xs[i] = options.ExecutionPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ExecutionPolicyPresets = xs
	}
	{
		xs := make(
			[]options.ReplicationPolicyDescription,
			len(result.GetReplicationPolicyPresets()),
		)
		for i, p := range result.GetReplicationPolicyPresets() {
			xs[i] = options.ReplicationPolicyDescription{
				Name:   p.GetName(),
				Labels: p.GetLabels(),
			}
		}
		desc.ReplicationPolicyPresets = xs
	}
	{
		xs := make(
			[]options.CachingPolicyDescription,
			len(result.GetCachingPolicyPresets()),
		)
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
// via Close() call or fully drained by sequential NextResultSet() calls.
func (s *session) StreamReadTable(
	ctx context.Context,
	path string,
	opts ...options.ReadTableOption,
) (_ result.StreamResult, err error) {
	var (
		onIntermediate = trace.TableOnSessionQueryStreamRead(s.config.Trace(), &ctx, s)
		request        = Ydb_Table.ReadTableRequest{
			SessionId: s.id,
			Path:      path,
		}
		stream Ydb_Table_V1.TableService_StreamReadTableClient
		a      = allocator.New()
	)
	defer func() {
		a.Free()
		if err != nil {
			onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
		}
	}()

	for _, opt := range opts {
		if opt != nil {
			opt((*options.ReadTableDesc)(&request), a)
		}
	}

	ctx, cancel := xcontext.WithErrCancel(ctx)

	stream, err = s.tableService.StreamReadTable(ctx, &request)

	if err != nil {
		cancel(xerrors.WithStackTrace(fmt.Errorf("ydb: stream read error: %w", err)))
		return nil, xerrors.WithStackTrace(err)
	}

	return scanner.NewStream(
		func(ctx context.Context) (
			set *Ydb.ResultSet,
			stats *Ydb_TableStats.QueryStats,
			err error,
		) {
			defer func() {
				onIntermediate(xerrors.HideEOF(err))
			}()
			select {
			case <-ctx.Done():
				return nil, nil, xerrors.WithStackTrace(ctx.Err())
			default:
				var response *Ydb_Table.ReadTableResponse
				response, err = stream.Recv()
				result := response.GetResult()
				if result == nil || err != nil {
					return nil, nil, xerrors.WithStackTrace(err)
				}
				return result.GetResultSet(), nil, nil
			}
		},
		func(err error) error {
			if err == nil {
				cancel(nil)
			} else {
				cancel(xerrors.WithStackTrace(fmt.Errorf("ydb: stream closed with: %w", err)))
			}
			onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
			return err
		},
		scanner.WithIgnoreTruncated(true), // stream read table always returns truncated flag on last result set
	), nil
}

// StreamExecuteScanQuery scan-reads table at given path with given options.
//
// Note that given ctx controls the lifetime of the whole read, not only this
// StreamExecuteScanQuery() call; that is, the time until returned result is closed
// via Close() call or fully drained by sequential NextResultSet() calls.
func (s *session) StreamExecuteScanQuery(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
	opts ...options.ExecuteScanQueryOption,
) (_ result.StreamResult, err error) {
	var (
		a              = allocator.New()
		q              = queryFromText(query)
		onIntermediate = trace.TableOnSessionQueryStreamExecute(
			s.config.Trace(),
			&ctx,
			s,
			q,
			params,
		)
		request = Ydb_Table.ExecuteScanQueryRequest{
			Query:      q.toYDB(a),
			Parameters: params.Params().ToYDB(a),
			Mode:       Ydb_Table.ExecuteScanQueryRequest_MODE_EXEC, // set default
		}
		stream Ydb_Table_V1.TableService_StreamExecuteScanQueryClient
	)
	defer func() {
		a.Free()
		if err != nil {
			onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
		}
	}()

	for _, opt := range opts {
		if opt != nil {
			opt((*options.ExecuteScanQueryDesc)(&request))
		}
	}

	ctx, cancel := context.WithCancel(ctx)

	stream, err = s.tableService.StreamExecuteScanQuery(ctx, &request)

	if err != nil {
		cancel()
		return nil, xerrors.WithStackTrace(err)
	}

	return scanner.NewStream(
		func(ctx context.Context) (
			set *Ydb.ResultSet,
			stats *Ydb_TableStats.QueryStats,
			err error,
		) {
			defer func() {
				onIntermediate(xerrors.HideEOF(err))
			}()
			select {
			case <-ctx.Done():
				return nil, nil, xerrors.WithStackTrace(ctx.Err())
			default:
				var response *Ydb_Table.ExecuteScanQueryPartialResponse
				response, err = stream.Recv()
				result := response.GetResult()
				if result == nil || err != nil {
					return nil, nil, xerrors.WithStackTrace(err)
				}
				return result.GetResultSet(), result.GetQueryStats(), nil
			}
		},
		func(err error) error {
			cancel()
			onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
			return err
		},
		scanner.WithIgnoreTruncated(s.config.IgnoreTruncated()),
		scanner.WithMarkTruncatedAsRetryable(),
	), nil
}

// BulkUpsert uploads given list of ydb struct values to the table.
func (s *session) BulkUpsert(ctx context.Context, table string, rows types.Value) (err error) {
	a := allocator.New()
	defer a.Free()
	_, err = s.tableService.BulkUpsert(ctx,
		&Ydb_Table.BulkUpsertRequest{
			Table: table,
			Rows:  value.ToYDB(rows, a),
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

// BeginTransaction begins new transaction within given session with given
// settings.
func (s *session) BeginTransaction(
	ctx context.Context,
	txSettings *table.TransactionSettings,
) (x table.Transaction, err error) {
	var (
		result   Ydb_Table.BeginTransactionResult
		response *Ydb_Table.BeginTransactionResponse
		onDone   = trace.TableOnSessionTransactionBegin(
			s.config.Trace(),
			&ctx,
			s,
		)
	)
	defer func() {
		onDone(x, err)
	}()

	response, err = s.tableService.BeginTransaction(ctx,
		&Ydb_Table.BeginTransactionRequest{
			SessionId:  s.id,
			TxSettings: txSettings.Settings(),
			OperationParams: operation.Params(
				ctx,
				s.config.OperationTimeout(),
				s.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return
	}
	return &transaction{
		id:      result.GetTxMeta().GetId(),
		state:   txStateInitialized,
		s:       s,
		control: table.TxControl(table.WithTxID(result.GetTxMeta().GetId())),
	}, nil
}
