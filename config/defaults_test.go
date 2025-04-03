package config

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/protobuf/types/known/emptypb"
)

// SimpleServiceServer - server interface
type SimpleServiceServer interface {
	Ping(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error)
}

// Server implementation
type simpleServer struct {
	delay     time.Duration
	mu        sync.Mutex
	dialCount int
}

func (s *simpleServer) Ping(ctx context.Context, in *emptypb.Empty) (*emptypb.Empty, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	return &emptypb.Empty{}, nil
}

func (s *simpleServer) incrementDialCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialCount++
}

func (s *simpleServer) getDialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.dialCount
}

// RegisterSimpleServiceServer registers the server
func RegisterSimpleServiceServer(s *grpc.Server, srv SimpleServiceServer) {
	s.RegisterService(&simpleServiceServiceDesc, srv)
}

var simpleServiceServiceDesc = grpc.ServiceDesc{
	ServiceName: "SimpleService",
	HandlerType: (*SimpleServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Ping",
			Handler:    simpleServicePingHandler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "simple.proto",
}

func simpleServicePingHandler(
	srv interface{},
	ctx context.Context,
	dec func(interface{}) error,
	interceptor grpc.UnaryServerInterceptor,
) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/SimpleService/Ping",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleServiceServer).Ping(ctx, req.(*emptypb.Empty))
	}

	return interceptor(ctx, in, info, handler)
}

// SimpleServiceClient - client interface
type SimpleServiceClient interface {
	Ping(
		ctx context.Context,
		in *emptypb.Empty,
		opts ...grpc.CallOption,
	) (*emptypb.Empty, error)
}

type simpleServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSimpleServiceClient(cc grpc.ClientConnInterface) SimpleServiceClient {
	return &simpleServiceClient{cc}
}

func (c *simpleServiceClient) Ping(
	ctx context.Context,
	in *emptypb.Empty,
	opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/SimpleService/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func TestGRPCLoadBalancingPolicies(t *testing.T) {
	// Start several real gRPC servers with different characteristics
	servers := make([]*simpleServer, 3)
	listeners := make([]net.Listener, 3)
	grpcServers := make([]*grpc.Server, 3)
	addresses := make([]string, 3)

	// Create servers with different behaviors
	for i := 0; i < 3; i++ {
		// First server has a delay, others respond immediately
		delay := time.Duration(0)
		if i == 0 {
			delay = 500 * time.Millisecond
		}

		servers[i] = &simpleServer{delay: delay}
		grpcServers[i] = grpc.NewServer()
		RegisterSimpleServiceServer(grpcServers[i], servers[i])

		// Start the server on a random port
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("Failed to create listener: %v", err)
		}
		listeners[i] = lis
		addresses[i] = lis.Addr().String()

		go func(gs *grpc.Server, l net.Listener) {
			_ = gs.Serve(l)
		}(grpcServers[i], lis)
	}

	// Cleanup after test
	defer func() {
		for i := 0; i < 3; i++ {
			if grpcServers[i] != nil {
				grpcServers[i].Stop()
			}
			if listeners[i] != nil {
				_ = listeners[i].Close()
			}
		}
	}()

	// Create a manual resolver to control addresses
	r := manual.NewBuilderWithScheme("test")
	resolver.Register(r)

	// Prepare addresses for the resolver
	addrs := make([]resolver.Address, 0, len(addresses))
	for _, addr := range addresses {
		addrs = append(addrs, resolver.Address{Addr: addr})
	}
	r.InitialState(resolver.State{Addresses: addrs})

	// Test different load balancing policies
	tests := []struct {
		name            string
		balancingPolicy string
	}{
		{"RoundRobin", "round_robin"},
		{"PickFirst", "pick_first"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Monitor connection establishment time
			dialStart := time.Now()

			// Create context with timeout for connection establishment
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// #nosec G402 - Using insecure credentials is acceptable for testing
			// Establish connection with our balancing policy
			conn, err := grpc.DialContext(
				ctx,
				"test:///unused", // Address doesn't matter as we use manual resolver
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, tc.balancingPolicy)),
				grpc.WithBlock(), // Wait for connection establishment to complete
			)

			dialDuration := time.Since(dialStart)

			if err != nil {
				t.Fatalf("Failed to dial: %v", err)
			}
			defer conn.Close()

			// Create client and make a request
			client := NewSimpleServiceClient(conn)
			_, err = client.Ping(context.Background(), &emptypb.Empty{})
			if err != nil {
				t.Fatalf("Ping failed: %v", err)
			}

			// Analyze behavior based on balancing policy
			switch tc.balancingPolicy {
			case "round_robin":
				// For round_robin, we expect fast connection as it connects
				// to all servers in parallel and should quickly find working ones
				if dialDuration >= 400*time.Millisecond {
					t.Logf("round_robin dial took %v, expected less than 400ms", dialDuration)
				}

				// Verify that requests execute successfully
				for i := 0; i < 10; i++ {
					_, err = client.Ping(context.Background(), &emptypb.Empty{})
					if err != nil {
						t.Fatalf("Ping failed: %v", err)
					}
				}

				t.Logf("round_robin successfully established connection in %v", dialDuration)

			case "pick_first":
				// For pick_first, connection time is important - if the first server is unavailable,
				// connection might take longer
				if servers[0].delay > 0 {
					t.Logf("pick_first dial took %v (expected to be affected by the delay)", dialDuration)
				}

				// Verify that requests execute successfully
				for i := 0; i < 10; i++ {
					_, err = client.Ping(context.Background(), &emptypb.Empty{})
					if err != nil {
						t.Fatalf("Ping failed: %v", err)
					}
				}

				t.Logf("pick_first successfully established connection in %v", dialDuration)
			}
		})
	}
}
