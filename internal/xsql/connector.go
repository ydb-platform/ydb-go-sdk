package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	metaHeaders "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ConnectorOption interface {
	Apply(c *Connector) error
}

type defaultQueryModeConnectorOption QueryMode

func (mode defaultQueryModeConnectorOption) Apply(c *Connector) error {
	c.defaultQueryMode = QueryMode(mode)
	return nil
}

type QueryBindConnectorOption interface {
	ConnectorOption
	bind.Bind
}

type queryBindConnectorOption struct {
	bind.Bind
}

func (o queryBindConnectorOption) Apply(c *Connector) error {
	c.Bindings = bind.Sort(append(c.Bindings, o.Bind))
	return nil
}

type tablePathPrefixConnectorOption struct {
	bind.TablePathPrefix
}

func (o tablePathPrefixConnectorOption) Apply(c *Connector) error {
	c.Bindings = bind.Sort(append(c.Bindings, o.TablePathPrefix))
	c.pathNormalizer = o.TablePathPrefix
	return nil
}

func WithQueryBind(bind bind.Bind) QueryBindConnectorOption {
	return queryBindConnectorOption{Bind: bind}
}

func WithTablePathPrefix(tablePathPrefix string) QueryBindConnectorOption {
	return tablePathPrefixConnectorOption{TablePathPrefix: bind.TablePathPrefix(tablePathPrefix)}
}

func WithDefaultQueryMode(mode QueryMode) ConnectorOption {
	return defaultQueryModeConnectorOption(mode)
}

type defaultTxControlOption struct {
	txControl *table.TransactionControl
}

func (opt defaultTxControlOption) Apply(c *Connector) error {
	c.defaultTxControl = opt.txControl
	return nil
}

func WithDefaultTxControl(txControl *table.TransactionControl) ConnectorOption {
	return defaultTxControlOption{txControl}
}

type defaultDataQueryOptionsConnectorOption []options.ExecuteDataQueryOption

func (opts defaultDataQueryOptionsConnectorOption) Apply(c *Connector) error {
	c.defaultDataQueryOpts = append(c.defaultDataQueryOpts, opts...)
	return nil
}

func WithDefaultDataQueryOptions(opts ...options.ExecuteDataQueryOption) ConnectorOption {
	return defaultDataQueryOptionsConnectorOption(opts)
}

type defaultScanQueryOptionsConnectorOption []options.ExecuteScanQueryOption

func (opts defaultScanQueryOptionsConnectorOption) Apply(c *Connector) error {
	c.defaultScanQueryOpts = append(c.defaultScanQueryOpts, opts...)
	return nil
}

func WithDefaultScanQueryOptions(opts ...options.ExecuteScanQueryOption) ConnectorOption {
	return defaultScanQueryOptionsConnectorOption(opts)
}

type traceConnectorOption struct {
	t    *trace.DatabaseSQL
	opts []trace.DatabaseSQLComposeOption
}

func (option traceConnectorOption) Apply(c *Connector) error {
	c.trace = c.trace.Compose(option.t, option.opts...)
	return nil
}

func WithTrace(t *trace.DatabaseSQL, opts ...trace.DatabaseSQLComposeOption) ConnectorOption {
	return traceConnectorOption{t, opts}
}

type disableServerBalancerConnectorOption struct{}

func (d disableServerBalancerConnectorOption) Apply(c *Connector) error {
	c.disableServerBalancer = true
	return nil
}

func WithDisableServerBalancer() ConnectorOption {
	return disableServerBalancerConnectorOption{}
}

type idleThresholdConnectorOption time.Duration

func (idleThreshold idleThresholdConnectorOption) Apply(c *Connector) error {
	c.idleThreshold = time.Duration(idleThreshold)
	return nil
}

func WithIdleThreshold(idleThreshold time.Duration) ConnectorOption {
	return idleThresholdConnectorOption(idleThreshold)
}

type onCloseConnectorOption func(connector *Connector)

func (f onCloseConnectorOption) Apply(c *Connector) error {
	c.onClose = append(c.onClose, f)
	return nil
}

func WithOnClose(f func(connector *Connector)) ConnectorOption {
	return onCloseConnectorOption(f)
}

type traceRetryConnectorOption struct {
	t *trace.Retry
}

func (t traceRetryConnectorOption) Apply(c *Connector) error {
	c.traceRetry = t.t
	return nil
}

func WithTraceRetry(t *trace.Retry) ConnectorOption {
	return traceRetryConnectorOption{t: t}
}

type fakeTxConnectorOption QueryMode

func (m fakeTxConnectorOption) Apply(c *Connector) error {
	c.fakeTxModes = append(c.fakeTxModes, QueryMode(m))
	return nil
}

// WithFakeTx returns a copy of context with given QueryMode
func WithFakeTx(m QueryMode) ConnectorOption {
	return fakeTxConnectorOption(m)
}

type ydbDriver interface {
	Name() string
	Table() table.Client
	Scripting() scripting.Client
	Scheme() scheme.Client
}

func Open(parent ydbDriver, opts ...ConnectorOption) (_ *Connector, err error) {
	c := &Connector{
		parent:           parent,
		clock:            clockwork.NewRealClock(),
		conns:            make(map[*conn]struct{}),
		defaultTxControl: table.DefaultTxControl(),
		defaultQueryMode: DefaultQueryMode,
		pathNormalizer:   bind.TablePathPrefix(parent.Name()),
		trace:            &trace.DatabaseSQL{},
	}
	for _, opt := range opts {
		if opt != nil {
			if err = opt.Apply(c); err != nil {
				return nil, err
			}
		}
	}
	if c.idleThreshold > 0 {
		c.idleStopper = c.idleCloser()
	}
	return c, nil
}

type pathNormalizer interface {
	NormalizePath(folderOrTable string) string
}

// Connector is a producer of database/sql connections
type Connector struct {
	parent ydbDriver

	clock clockwork.Clock

	Bindings       bind.Bindings
	pathNormalizer pathNormalizer

	fakeTxModes []QueryMode

	onClose []func(connector *Connector)

	conns    map[*conn]struct{}
	connsMtx sync.RWMutex

	idleStopper func()

	defaultTxControl      *table.TransactionControl
	defaultQueryMode      QueryMode
	defaultDataQueryOpts  []options.ExecuteDataQueryOption
	defaultScanQueryOpts  []options.ExecuteScanQueryOption
	disableServerBalancer bool
	idleThreshold         time.Duration

	trace      *trace.DatabaseSQL
	traceRetry *trace.Retry
}

var (
	_ driver.Connector = &Connector{}
	_ io.Closer        = &Connector{}
)

func (c *Connector) idleCloser() (idleStopper func()) {
	var ctx context.Context
	ctx, idleStopper = xcontext.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.clock.After(c.idleThreshold):
				c.connsMtx.RLock()
				conns := make([]*conn, 0, len(c.conns))
				for cc := range c.conns {
					conns = append(conns, cc)
				}
				c.connsMtx.RUnlock()
				for _, cc := range conns {
					if cc.sinceLastUsage() > c.idleThreshold {
						cc.session.Close(context.Background())
					}
				}
			}
		}
	}()
	return idleStopper
}

func (c *Connector) Close() (err error) {
	defer func() {
		for _, onClose := range c.onClose {
			onClose(c)
		}
	}()
	if c.idleStopper != nil {
		c.idleStopper()
	}
	return nil
}

func (c *Connector) attach(cc *conn) {
	c.connsMtx.Lock()
	defer c.connsMtx.Unlock()
	c.conns[cc] = struct{}{}
}

func (c *Connector) detach(cc *conn) {
	c.connsMtx.Lock()
	defer c.connsMtx.Unlock()
	delete(c.conns, cc)
}

func (c *Connector) Connect(ctx context.Context) (_ driver.Conn, err error) {
	var (
		onDone = trace.DatabaseSQLOnConnectorConnect(
			c.trace, &ctx,
			stack.FunctionID(""),
		)
		session table.ClosableSession
	)
	defer func() {
		onDone(err, session)
	}()
	if !c.disableServerBalancer {
		ctx = meta.WithAllowFeatures(ctx, metaHeaders.HintSessionBalancer)
	}
	session, err = c.parent.Table().CreateSession(ctx) //nolint
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return newConn(ctx, c, session, withDefaultTxControl(c.defaultTxControl),
		withDefaultQueryMode(c.defaultQueryMode),
		withDataOpts(c.defaultDataQueryOpts...),
		withScanOpts(c.defaultScanQueryOpts...),
		withTrace(c.trace),
		withFakeTxModes(c.fakeTxModes...),
	), nil
}

func (c *Connector) Driver() driver.Driver {
	return &driverWrapper{c: c}
}

type driverWrapper struct {
	c *Connector
}

func (d *driverWrapper) TraceRetry() *trace.Retry {
	return d.c.traceRetry
}

func (d *driverWrapper) Open(_ string) (driver.Conn, error) {
	return nil, ErrUnsupported
}
