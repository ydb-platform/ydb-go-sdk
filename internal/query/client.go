package query

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate mockgen -destination grpc_client_mock_test.go --typed -package query -write_package_comment=false github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1 QueryServiceClient,QueryService_AttachSessionClient,QueryService_ExecuteQueryClient

var (
	_ query.Client = (*Client)(nil)
	_ sessionPool  = (*poolStub)(nil)
	_ sessionPool  = (*pool.Pool[*Session, Session])(nil)
)

type (
	sessionPool interface {
		closer.Closer

		Stats() pool.Stats
		With(ctx context.Context, f func(ctx context.Context, s *Session) error, opts ...retry.Option) error
	}
	poolStub struct {
		createSession func(ctx context.Context) (*Session, error)
		InUse         atomic.Int32
	}
	Client struct {
		config             *config.Config
		queryServiceClient Ydb_Query_V1.QueryServiceClient
		pool               sessionPool

		done chan struct{}
	}
)

func fetchScriptResults(ctx context.Context,
	client Ydb_Query_V1.QueryServiceClient,
	opID string, opts ...options.FetchScriptOption,
) (*options.FetchScriptResult, error) {
	r, err := retry.RetryWithResult(ctx, func(ctx context.Context) (*options.FetchScriptResult, error) {
		request := &options.FetchScriptResultsRequest{
			FetchScriptResultsRequest: Ydb_Query.FetchScriptResultsRequest{
				OperationId: opID,
			},
		}
		for _, opt := range opts {
			if opt != nil {
				opt(request)
			}
		}

		response, err := client.FetchScriptResults(ctx, &request.FetchScriptResultsRequest)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		rs := response.GetResultSet()
		columns := rs.GetColumns()
		columnNames := make([]string, len(columns))
		columnTypes := make([]types.Type, len(columns))
		for i := range columns {
			columnNames[i] = columns[i].GetName()
			columnTypes[i] = types.TypeFromYDB(columns[i].GetType())
		}
		rows := make([]query.Row, len(rs.GetRows()))
		for i, r := range rs.GetRows() {
			rows[i] = NewRow(columns, r)
		}

		return &options.FetchScriptResult{
			ResultSetIndex: response.GetResultSetIndex(),
			ResultSet:      MaterializedResultSet(int(response.GetResultSetIndex()), columnNames, columnTypes, rows),
			NextToken:      response.GetNextFetchToken(),
		}, nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (c *Client) FetchScriptResults(ctx context.Context,
	opID string, opts ...options.FetchScriptOption,
) (*options.FetchScriptResult, error) {
	r, err := retry.RetryWithResult(ctx, func(ctx context.Context) (*options.FetchScriptResult, error) {
		r, err := fetchScriptResults(ctx, c.queryServiceClient, opID,
			append(opts, func(request *options.FetchScriptResultsRequest) {
				request.Trace = c.config.Trace()
			})...,
		)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return r, nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

type executeScriptSettings struct {
	executeSettings
	ttl             time.Duration
	operationParams *Ydb_Operations.OperationParams
}

func (s *executeScriptSettings) OperationParams() *Ydb_Operations.OperationParams {
	return s.operationParams
}

func (s *executeScriptSettings) ResultsTTL() time.Duration {
	return s.ttl
}

func executeScript(ctx context.Context, //nolint:funlen
	client Ydb_Query_V1.QueryServiceClient, request *Ydb_Query.ExecuteScriptRequest, grpcOpts ...grpc.CallOption,
) (*options.ExecuteScriptOperation, error) {
	op, err := retry.RetryWithResult(ctx, func(ctx context.Context) (*options.ExecuteScriptOperation, error) {
		response, err := client.ExecuteScript(ctx, request, grpcOpts...)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		var md Ydb_Query.ExecuteScriptMetadata
		err = response.GetMetadata().UnmarshalTo(&md)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return &options.ExecuteScriptOperation{
			ID:            response.GetId(),
			ConsumedUnits: response.GetCostInfo().GetConsumedUnits(),
			Metadata: struct {
				ID     string
				Script struct {
					Syntax options.Syntax
					Query  string
				}
				Mode           options.ExecMode
				Stats          stats.QueryStats
				ResultSetsMeta []struct {
					Columns []struct {
						Name string
						Type query.Type
					}
				}
			}{
				ID: md.GetExecutionId(),
				Script: struct {
					Syntax options.Syntax
					Query  string
				}{
					Syntax: options.Syntax(md.GetScriptContent().GetSyntax()),
					Query:  md.GetScriptContent().GetText(),
				},
				Mode:  options.ExecMode(md.GetExecMode()),
				Stats: stats.FromQueryStats(md.GetExecStats()),
				ResultSetsMeta: func() (
					resultSetsMeta []struct {
						Columns []struct {
							Name string
							Type query.Type
						}
					},
				) {
					for _, rs := range md.GetResultSetsMeta() {
						resultSetsMeta = append(resultSetsMeta, struct {
							Columns []struct {
								Name string
								Type query.Type
							}
						}{
							Columns: func() (
								columns []struct {
									Name string
									Type types.Type
								},
							) {
								for _, c := range rs.GetColumns() {
									columns = append(columns, struct {
										Name string
										Type types.Type
									}{
										Name: c.GetName(),
										Type: types.TypeFromYDB(c.GetType()),
									})
								}

								return columns
							}(),
						})
					}

					return resultSetsMeta
				}(),
			},
		}, nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return op, xerrors.WithStackTrace(err)
	}

	return op, nil
}

func (c *Client) ExecuteScript(
	ctx context.Context, q string, ttl time.Duration, opts ...options.Execute,
) (
	op *options.ExecuteScriptOperation, err error,
) {
	a := allocator.New()
	defer a.Free()

	settings := &executeScriptSettings{
		executeSettings: options.ExecuteSettings(opts...),
		ttl:             ttl,
		operationParams: operation.Params(
			ctx,
			c.config.OperationTimeout(),
			c.config.OperationCancelAfter(),
			operation.ModeSync,
		),
	}

	request, grpcOpts := executeQueryScriptRequest(a, q, settings)

	op, err = executeScript(ctx, c.queryServiceClient, request, grpcOpts...)
	if err != nil {
		return op, xerrors.WithStackTrace(err)
	}

	return op, nil
}

func (p *poolStub) Close(ctx context.Context) error {
	return nil
}

func (p *poolStub) Stats() pool.Stats {
	return pool.Stats{
		Limit: -1,
		Index: 0,
		Idle:  0,
		InUse: int(p.InUse.Load()),
	}
}

func (p *poolStub) With(
	ctx context.Context, f func(ctx context.Context, s *Session) error, opts ...retry.Option,
) error {
	p.InUse.Add(1)
	defer func() {
		p.InUse.Add(-1)
	}()

	err := retry.Retry(ctx, func(ctx context.Context) (err error) {
		s, err := p.createSession(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = s.Close(ctx)
		}()

		err = f(ctx, s)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Close(ctx context.Context) error {
	close(c.done)

	if err := c.pool.Close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func do(
	ctx context.Context,
	pool sessionPool,
	op func(ctx context.Context, s *Session) error,
	opts ...retry.Option,
) (finalErr error) {
	err := pool.With(ctx, func(ctx context.Context, s *Session) error {
		s.setStatus(statusInUse)

		err := op(ctx, s)
		if err != nil {
			if xerrors.IsOperationError(err) {
				s.setStatus(statusClosed)
			}

			return xerrors.WithStackTrace(err)
		}

		s.setStatus(statusIdle)

		return nil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Do(ctx context.Context, op query.Operation, opts ...options.DoOption) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	var (
		onDone = trace.QueryOnDo(c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Do"),
		)
		attempts = 0
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	err := do(ctx, c.pool, func(ctx context.Context, s *Session) error {
		attempts++

		return op(ctx, s)
	}, options.ParseDoOpts(c.config.Trace(), opts...).RetryOpts()...)

	return err
}

func doTx(
	ctx context.Context,
	pool sessionPool,
	op query.TxOperation,
	t *trace.Query,
	opts ...options.DoTxOption,
) (finalErr error) {
	doTxOpts := options.ParseDoTxOpts(t, opts...)

	err := do(ctx, pool, func(ctx context.Context, s *Session) (err error) {
		tx, err := s.Begin(ctx, doTxOpts.TxSettings())
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		err = op(ctx, tx)
		if err != nil {
			errRollback := tx.Rollback(ctx)
			if errRollback != nil {
				return xerrors.WithStackTrace(xerrors.Join(err, errRollback))
			}

			return xerrors.WithStackTrace(err)
		}
		err = tx.CommitTx(ctx)
		if err != nil {
			errRollback := tx.Rollback(ctx)
			if errRollback != nil {
				return xerrors.WithStackTrace(xerrors.Join(err, errRollback))
			}

			return xerrors.WithStackTrace(err)
		}

		return nil
	}, doTxOpts.RetryOpts()...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func clientQueryRow(
	ctx context.Context, pool sessionPool, q string, settings executeSettings, resultOpts ...resultOption,
) (row query.Row, finalErr error) {
	err := do(ctx, pool, func(ctx context.Context, s *Session) (err error) {
		row, err = s.queryRow(ctx, q, settings, resultOpts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, settings.RetryOpts()...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

// QueryRow is a helper which read only one row from first result set in result
func (c *Client) QueryRow(ctx context.Context, q string, opts ...options.Execute) (_ query.Row, finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQueryRow(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).QueryRow"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	row, err := clientQueryRow(ctx, c.pool, q, options.ExecuteSettings(opts...), withTrace(c.config.Trace()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func clientExec(ctx context.Context, pool sessionPool, q string, opts ...options.Execute) (finalErr error) {
	settings := options.ExecuteSettings(opts...)
	err := do(ctx, pool, func(ctx context.Context, s *Session) (err error) {
		_, r, err := execute(ctx, s.id, s.queryServiceClient, q, settings, withTrace(s.cfg.Trace()))
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		err = readAll(ctx, r)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, settings.RetryOpts()...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Exec(ctx context.Context, q string, opts ...options.Execute) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnExec(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Exec"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	err := clientExec(ctx, c.pool, q, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func clientQuery(ctx context.Context, pool sessionPool, q string, opts ...options.Execute) (
	r query.Result, err error,
) {
	settings := options.ExecuteSettings(opts...)
	err = do(ctx, pool, func(ctx context.Context, s *Session) (err error) {
		_, streamResult, err := execute(ctx, s.id, s.queryServiceClient, q,
			options.ExecuteSettings(opts...), withTrace(s.cfg.Trace()),
		)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = streamResult.Close(ctx)
		}()

		r, err = resultToMaterializedResult(ctx, streamResult)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, settings.RetryOpts()...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (c *Client) Query(ctx context.Context, q string, opts ...options.Execute) (r query.Result, err error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQuery(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Query"),
		q,
	)
	defer func() {
		onDone(err)
	}()

	r, err = clientQuery(ctx, c.pool, q, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func clientQueryResultSet(
	ctx context.Context, pool sessionPool, q string, settings executeSettings, resultOpts ...resultOption,
) (rs query.ResultSet, finalErr error) {
	err := do(ctx, pool, func(ctx context.Context, s *Session) error {
		_, r, err := execute(ctx, s.id, s.queryServiceClient, q, settings, resultOpts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		rs, err = readMaterializedResultSet(ctx, r)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, settings.RetryOpts()...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

// QueryResultSet is a helper which read all rows from first result set in result
func (c *Client) QueryResultSet(
	ctx context.Context, q string, opts ...options.Execute,
) (rs query.ResultSet, finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQueryResultSet(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).QueryResultSet"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	rs, err := clientQueryResultSet(ctx, c.pool, q, options.ExecuteSettings(opts...), withTrace(c.config.Trace()))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func (c *Client) DoTx(ctx context.Context, op query.TxOperation, opts ...options.DoTxOption) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	var (
		onDone = trace.QueryOnDoTx(c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).DoTx"),
		)
		attempts = 0
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	err := doTx(ctx, c.pool, func(ctx context.Context, tx query.TxActor) error {
		attempts++

		return op(ctx, tx)
	}, c.config.Trace(), opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func newPool(
	ctx context.Context, cfg *config.Config, createSession func(ctx context.Context) (*Session, error),
) sessionPool {
	if cfg.UseSessionPool() {
		return pool.New(ctx,
			pool.WithLimit[*Session, Session](cfg.PoolLimit()),
			pool.WithTrace[*Session, Session](poolTrace(cfg.Trace())),
			pool.WithCreateItemTimeout[*Session, Session](cfg.SessionCreateTimeout()),
			pool.WithCloseItemTimeout[*Session, Session](cfg.SessionDeleteTimeout()),
			pool.WithCreateFunc(createSession),
		)
	}

	return &poolStub{
		createSession: createSession,
	}
}

func New(ctx context.Context, balancer grpc.ClientConnInterface, cfg *config.Config) *Client {
	onDone := trace.QueryOnNew(cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.New"),
	)
	defer onDone()

	grpcClient := Ydb_Query_V1.NewQueryServiceClient(balancer)

	client := &Client{
		config:             cfg,
		queryServiceClient: grpcClient,
		done:               make(chan struct{}),
		pool: newPool(ctx, cfg, func(ctx context.Context) (_ *Session, err error) {
			var (
				createCtx    context.Context
				cancelCreate context.CancelFunc
			)
			if d := cfg.SessionCreateTimeout(); d > 0 {
				createCtx, cancelCreate = xcontext.WithTimeout(ctx, d)
			} else {
				createCtx, cancelCreate = xcontext.WithCancel(ctx)
			}
			defer cancelCreate()

			s, err := createSession(createCtx, grpcClient, cfg)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return s, nil
		}),
	}

	return client
}

func poolTrace(t *trace.Query) *pool.Trace {
	return &pool.Trace{
		OnNew: func(info *pool.NewStartInfo) func(*pool.NewDoneInfo) {
			onDone := trace.QueryOnPoolNew(t, info.Context, info.Call)

			return func(info *pool.NewDoneInfo) {
				onDone(info.Limit)
			}
		},
		OnClose: func(info *pool.CloseStartInfo) func(*pool.CloseDoneInfo) {
			onDone := trace.QueryOnClose(t, info.Context, info.Call)

			return func(info *pool.CloseDoneInfo) {
				onDone(info.Error)
			}
		},
		OnTry: func(info *pool.TryStartInfo) func(*pool.TryDoneInfo) {
			onDone := trace.QueryOnPoolTry(t, info.Context, info.Call)

			return func(info *pool.TryDoneInfo) {
				onDone(info.Error)
			}
		},
		OnWith: func(info *pool.WithStartInfo) func(*pool.WithDoneInfo) {
			onDone := trace.QueryOnPoolWith(t, info.Context, info.Call)

			return func(info *pool.WithDoneInfo) {
				onDone(info.Error, info.Attempts)
			}
		},
		OnPut: func(info *pool.PutStartInfo) func(*pool.PutDoneInfo) {
			onDone := trace.QueryOnPoolPut(t, info.Context, info.Call)

			return func(info *pool.PutDoneInfo) {
				onDone(info.Error)
			}
		},
		OnGet: func(info *pool.GetStartInfo) func(*pool.GetDoneInfo) {
			onDone := trace.QueryOnPoolGet(t, info.Context, info.Call)

			return func(info *pool.GetDoneInfo) {
				onDone(info.Error)
			}
		},
		OnChange: func(info pool.ChangeInfo) {
			trace.QueryOnPoolChange(t, info.Limit, info.Index, info.Idle, info.InUse)
		},
	}
}
