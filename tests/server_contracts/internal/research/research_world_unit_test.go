package research_test

import (
	"context"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestResearchTopicFixtureUsesOnlyTopicRPCs(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &topicFixtureServer{address: listener.Addr().(*net.TCPAddr)}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		service.mu.Lock()
		service.methods = append(service.methods, info.FullMethod)
		service.mu.Unlock()
		md, _ := metadata.FromIncomingContext(ctx)
		if got := md.Get("x-ydb-database"); !reflect.DeepEqual(got, []string{"/local"}) {
			t.Errorf("database metadata: %v", got)
		}

		return handler(ctx, request)
	}))
	Ydb_Topic_V1.RegisterTopicServiceServer(server, service)
	// Support discovery so an accidental SDK client reaches the fixture path,
	// rather than failing to connect before the boundary assertion below.
	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Setenv("YDB_CONNECTION_STRING", "grpc://"+listener.Addr().String()+"/local")
	t.Setenv("YDB_ACCESS_TOKEN_CREDENTIALS", "")
	t.Setenv("YDB_SSL_ROOT_CERTIFICATES_FILE", "")
	if err := os.Unsetenv("YDB_SSL_ROOT_CERTIFICATES_FILE"); err != nil {
		t.Fatal(err)
	}
	world := &researchWorld{}
	t.Cleanup(func() { _ = world.Close() })
	defer func() {
		if failure := recover(); failure != nil {
			t.Fatalf("topic fixture must work through gRPC independently of SDK initialization: %v", failure)
		}
	}()
	if err := world.CreateTopic(t.Context(), 2, true, "reader"); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	created := service.created
	service.mu.Unlock()
	if !world.created || created.GetPath() != world.topicPath {
		t.Fatalf("topic fixture was not created: %v", created)
	}
	settings := created.GetPartitioningSettings()
	if settings.GetMinActivePartitions() != 2 || settings.GetMaxActivePartitions() != 2 ||
		settings.GetAutoPartitioningSettings().GetStrategy() !=
			Ydb_Topic.AutoPartitioningStrategy_AUTO_PARTITIONING_STRATEGY_PAUSED ||
		len(created.GetConsumers()) != 1 || created.GetConsumers()[0].GetName() != "reader" {
		t.Fatalf("fixture settings changed: %v", created)
	}
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.dropped != world.topicPath || !reflect.DeepEqual(service.methods, []string{
		Ydb_Topic_V1.TopicService_CreateTopic_FullMethodName,
		Ydb_Topic_V1.TopicService_DropTopic_FullMethodName,
	}) {
		t.Fatalf("fixture must create and drop its topic using only Topic RPCs: methods=%v dropped=%q",
			service.methods, service.dropped)
	}
}

type topicFixtureServer struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	address *net.TCPAddr
	mu      sync.Mutex
	methods []string
	created *Ydb_Topic.CreateTopicRequest
	dropped string
}

func (s *topicFixtureServer) ListEndpoints(
	context.Context, *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	result, err := anypb.New(&Ydb_Discovery.ListEndpointsResult{Endpoints: []*Ydb_Discovery.EndpointInfo{
		{Address: s.address.IP.String(), Port: uint32(s.address.Port), NodeId: 1},
	}})

	return &Ydb_Discovery.ListEndpointsResponse{Operation: &Ydb_Operations.Operation{
		Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: result,
	}}, err
}

func (s *topicFixtureServer) CreateTopic(
	_ context.Context, request *Ydb_Topic.CreateTopicRequest,
) (*Ydb_Topic.CreateTopicResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = request

	return &Ydb_Topic.CreateTopicResponse{
		Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS},
	}, nil
}

func (s *topicFixtureServer) DropTopic(
	_ context.Context, request *Ydb_Topic.DropTopicRequest,
) (*Ydb_Topic.DropTopicResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = request.GetPath()

	return &Ydb_Topic.DropTopicResponse{
		Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS},
	}, nil
}
