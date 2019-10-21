/*
Package ydbtest provides tools for stubbing ydb server up.
*/
package ydbtest

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/grpc"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/grpc/Ydb_Discovery_V1"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Discovery"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Operations"
)

type Handler func(ctx context.Context, req RequestParser) (res interface{}, err error)

type Handlers map[ydb.Method]Handler

type RequestParser func(interface{})
type ResponseMapper func(RequestParser) proto.Message

func Ident(res proto.Message) ResponseMapper {
	return func(RequestParser) proto.Message {
		return res
	}
}

func operationResponse(r proto.Message, id string, status Ydb.StatusIds_StatusCode) interface{} {
	res, err := proto.Marshal(r)
	if err != nil {
		panic(err)
	}
	return &Ydb_Operations.GetOperationResponse{
		Operation: &Ydb_Operations.Operation{
			Id:     id,
			Ready:  true,
			Status: status,
			Result: &any.Any{
				Value: res,
			},
		},
	}
}

func SuccessHandler(f ResponseMapper) Handler {
	var id int32
	return func(_ context.Context, req RequestParser) (_ interface{}, err error) {
		x := atomic.AddInt32(&id, 1)
		r := f(req)
		return operationResponse(
			r, strconv.Itoa(int(x)),
			Ydb.StatusIds_SUCCESS,
		), nil
	}
}

type YDB struct {
	Database string
	Handlers Handlers
	T        testing.TB

	once  sync.Once
	descs []grpc.ServiceDesc

	mu        sync.Mutex
	endpoints map[ydb.Endpoint]*Endpoint
}

func (s *YDB) init() {
	s.once.Do(func() {
		m := make(map[string][]grpc.MethodDesc)
		for x, handler := range s.Handlers {
			service := x.Service()
			m[service] = append(m[service], grpc.MethodDesc{
				MethodName: x.Name(),
				Handler: func(
					_ interface{}, ctx context.Context,
					dec func(interface{}) error, _ grpc.UnaryServerInterceptor,
				) (interface{}, error) {
					return handler(ctx, func(x interface{}) {
						if err := dec(x); err != nil {
							s.T.Fatalf("decode request error: %v", err)
						}
					})
				},
			})
		}
		for service, methods := range m {
			s.descs = append(s.descs, grpc.ServiceDesc{
				ServiceName: service,
				Methods:     methods,
				HandlerType: (*stubHandler)(nil),
			})
		}

		s.endpoints = make(map[ydb.Endpoint]*Endpoint)
	})
}

type Endpoint struct {
	db     *YDB
	id     ydb.Endpoint
	server *grpc.Server
	proxy  *proxyListener

	C <-chan net.Conn
}

// StartEndpoint starts to provide a YDB service on some local address.
func (s *YDB) StartEndpoint() *Endpoint {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		s.T.Fatal(err)
	}
	e := ydb.Endpoint{
		Addr: "localhost",
		Port: ln.Addr().(*net.TCPAddr).Port,
	}

	srv := grpc.NewServer()
	for _, desc := range s.descs {
		srv.RegisterService(&desc, stubHandler(1))
	}
	conns := make(chan net.Conn, 1)
	proxy := newProxyListener(ln, conns)
	go func() {
		if err := srv.Serve(proxy); err != nil {
			s.T.Fatal(err)
		}
	}()

	x := &Endpoint{
		C:      conns,
		db:     s,
		id:     e,
		server: srv,
		proxy:  proxy,
	}
	s.endpoints[e] = x

	return x
}

func (e *Endpoint) Close() {
	e.db.mu.Lock()
	defer e.db.mu.Unlock()

	_, has := e.db.endpoints[e.id]
	if !has {
		e.db.T.Fatalf("endpoint not exists: %v", e.id)
	}
	delete(e.db.endpoints, e.id)
	e.server.Stop()
	_ = e.proxy.Close()
}

type Balancer struct {
	ln     net.Listener
	server *grpc.Server
}

func (b *Balancer) Addr() net.Addr {
	return b.ln.Addr()
}

func (b *Balancer) Close() error {
	b.server.Stop()
	return b.ln.Close()
}

// StartBalancer starts to provide a discovery service on some local address.
func (s *YDB) StartBalancer() *Balancer {
	s.init()

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		s.T.Fatal(err)
	}
	srv := grpc.NewServer()
	m := ydb.Method(Ydb_Discovery_V1.ListEndpoints)

	service, method := m.Split()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: service,
		Methods: []grpc.MethodDesc{
			{
				MethodName: method,
				Handler: func(
					_ interface{}, ctx context.Context,
					dec func(interface{}) error, _ grpc.UnaryServerInterceptor,
				) (interface{}, error) {
					req := new(Ydb_Discovery.ListEndpointsRequest)
					if err := dec(req); err != nil {
						return nil, err
					}
					res, err := s.listEndpoints(req.Database)
					if err != nil {
						return nil, err
					}
					return operationResponse(
						res, "0", Ydb.StatusIds_SUCCESS,
					), nil
				},
			},
		},
		HandlerType: (*stubHandler)(nil),
	}, stubHandler(1))
	go func() {
		if err := srv.Serve(ln); err != nil {
			s.T.Fatal(err)
		}
	}()

	return &Balancer{
		ln:     ln,
		server: srv,
	}
}

func (s *YDB) listEndpoints(db string) (resp *Ydb_Discovery.ListEndpointsResult, err error) {
	if db != s.Database {
		s.T.Errorf(
			"discovery request for %q database; have %q",
			db, s.Database,
		)
		return nil, fmt.Errorf("ydbtest: unknown database: %q", db)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	es := make([]*Ydb_Discovery.EndpointInfo, 0, len(s.endpoints))
	for e := range s.endpoints {
		es = append(es, &Ydb_Discovery.EndpointInfo{
			Address:    e.Addr,
			Port:       uint32(e.Port),
			LoadFactor: e.LoadFactor,
		})
	}
	return &Ydb_Discovery.ListEndpointsResult{
		Endpoints: es,
	}, nil
}

type stubHandler interface{}

type proxyListener struct {
	net.Listener
	ticket chan struct{}
	conns  chan<- net.Conn
}

func newProxyListener(ln net.Listener, conns chan<- net.Conn) *proxyListener {
	return &proxyListener{
		Listener: ln,
		conns:    conns,
	}
}

func (p *proxyListener) Accept() (net.Conn, error) {
	conn, err := p.Listener.Accept()
	select {
	case p.conns <- conn:
	default:
	}
	return conn, err
}

func (p *proxyListener) Close() error {
	return p.Listener.Close()
}
