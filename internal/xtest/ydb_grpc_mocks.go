package xtest

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

func GrpcMockTopicConnString(e fixenv.Env, topicServiceImpl Ydb_Topic_V1.TopicServiceServer) string {
	v := reflect.ValueOf(topicServiceImpl)
	addr := v.Pointer()

	var f fixenv.GenericFixtureFunction[string] = func() (*fixenv.GenericResult[string], error) {
		listener := sf.LocalTCPListenerNamed(e, fmt.Sprintf("ydb-grpc-mock-topic-%v", addr))
		connString := fmt.Sprintf("grpc://%s/local", listener.Addr())

		mock, err := newGrpcMock(listener, topicServiceImpl)
		if err != nil {
			return nil, fmt.Errorf("failed to create grpc mock: %w", err)
		}

		clean := func() {
			_ = mock.Close()
		}

		return fixenv.NewGenericResultWithCleanup(connString, clean), nil
	}

	return fixenv.CacheResult(e, f, fixenv.CacheOptions{CacheKey: addr})
}

type grpcMock struct {
	listener   net.Listener
	grpcServer *grpc.Server
	stopChan   chan error
}

func (m *grpcMock) Close() error {
	m.grpcServer.Stop()

	return m.listener.Close()
}

func newGrpcMock(listener net.Listener, topicServiceImpl Ydb_Topic_V1.TopicServiceServer) (*grpcMock, error) {
	res := &grpcMock{
		listener:   listener,
		grpcServer: grpc.NewServer(),
		stopChan:   make(chan error, 1),
	}

	host, portS, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return nil, fmt.Errorf("failed to split host port addresses: %w", err)
	}

	port, err := strconv.ParseUint(portS, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed convert port to int: %w", err)
	}
	discoveryService := newMockDiscoveryService(host, uint32(port))

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(res.grpcServer, discoveryService)
	Ydb_Topic_V1.RegisterTopicServiceServer(res.grpcServer, topicServiceImpl)

	go func() {
		res.stopChan <- res.grpcServer.Serve(res.listener)
	}()

	select {
	case <-res.stopChan:
		return nil, err
	case <-time.After(time.Millisecond):
		return res, nil
	}
}

type mockDiscoveryService struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer
	host string
	port uint32
}

func newMockDiscoveryService(host string, port uint32) *mockDiscoveryService {
	return &mockDiscoveryService{
		host: host,
		port: port,
	}
}

func (m mockDiscoveryService) ListEndpoints(
	ctx context.Context,
	request *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	res := &Ydb_Discovery.ListEndpointsResult{
		Endpoints: []*Ydb_Discovery.EndpointInfo{
			{
				Address:    m.host,
				Port:       m.port,
				LoadFactor: 0,
				Ssl:        false,
				Service:    nil,
				Location:   "",
				NodeId:     1,
				IpV4:       []string{"127.0.0.1"},
			},
		},
		SelfLocation: "",
	}
	resp := &Ydb_Discovery.ListEndpointsResponse{
		Operation: &Ydb_Operations.Operation{
			Id:     "test-list-operation",
			Ready:  true,
			Status: Ydb.StatusIds_SUCCESS,
			Result: &anypb.Any{},
		},
	}
	err := resp.GetOperation().GetResult().MarshalFrom(res)

	return resp, err
}

func (m mockDiscoveryService) WhoAmI(
	ctx context.Context,
	request *Ydb_Discovery.WhoAmIRequest,
) (*Ydb_Discovery.WhoAmIResponse, error) {
	panic("unimplemented")
}
