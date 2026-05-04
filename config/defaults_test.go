package config_test

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
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

// simpleServicePingHandler handles ping requests
// This function must follow gRPC's required signature, where context is the second parameter.
//
//nolint:revive // context-as-argument: gRPC handler requires this signature
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

// NewSimpleServiceClient creates a new client
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

// CustomDialer implements the dialer function with controlled delays
type CustomDialer struct {
	// Map of address to delay before connection
	delays map[string]time.Duration
	// Mutex for thread safety
	mu sync.Mutex
	// Keeps track of dial attempt count
	dialAttempts map[string]int
}

// DialContext is used by gRPC to establish connections
func (d *CustomDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	d.mu.Lock()
	delay, exists := d.delays[addr]
	d.dialAttempts[addr]++
	attemptCount := d.dialAttempts[addr]
	d.mu.Unlock()

	// Log the dial attempt
	fmt.Printf("Attempting to dial '%s' (attempt #%d)\n", addr, attemptCount)

	if exists && delay > 0 {
		// Simulating connection delay or timeout
		fmt.Printf("Simulating delay of %v for '%s'\n", delay, addr)

		select {
		case <-time.After(delay):
			// If this is a simulated failure, return error
			if delay >= 2*time.Second {
				fmt.Printf("Connection to %s timed out after %v\n", addr, delay)

				return nil, fmt.Errorf("connection timeout")
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Establish a real connection to the address
	dialer := &net.Dialer{}

	return dialer.DialContext(ctx, "tcp", addr)
}

// GetDialAttempts returns the number of dial attempts for an address
func (d *CustomDialer) GetDialAttempts(addr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.dialAttempts[addr]
}

// TestGRPCLoadBalancingPolicies tests how different load balancing policies behave
// This is a test function, so we can ignore the staticcheck warnings about deprecated methods
// as we need to use these specific gRPC APIs for testing the load balancing behavior.
func TestGRPCLoadBalancingPolicies(t *testing.T) {
	// Total number of servers to test
	const totalServers = 6

	// Setup servers
	servers := make([]*simpleServer, totalServers)
	listeners := make([]net.Listener, totalServers)
	grpcServers := make([]*grpc.Server, totalServers)
	addresses := make([]string, totalServers)

	// Custom dialer with controlled delays
	dialer := &CustomDialer{
		delays:       make(map[string]time.Duration),
		dialAttempts: make(map[string]int),
	}

	// Start all servers
	for i := 0; i < totalServers; i++ {
		servers[i] = &simpleServer{}
		grpcServers[i] = grpc.NewServer()
		RegisterSimpleServiceServer(grpcServers[i], servers[i])

		// Start the server on a random port
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("Failed to create listener: %v", err)
		}
		listeners[i] = lis
		addresses[i] = lis.Addr().String()

		// First 4 servers will have a "connection delay" of 2.5 seconds, simulating timeout
		if i < 4 {
			dialer.delays[addresses[i]] = 2500 * time.Millisecond
		} else {
			// Last two servers connect quickly
			dialer.delays[addresses[i]] = 50 * time.Millisecond
		}

		t.Logf("Started server %d at %s with delay %v", i, addresses[i], dialer.delays[addresses[i]])

		go func(gs *grpc.Server, l net.Listener) {
			_ = gs.Serve(l)
		}(grpcServers[i], lis)
	}

	// Cleanup after test
	defer func() {
		for i := 0; i < totalServers; i++ {
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
	addrs := make([]resolver.Address, 0, totalServers)
	for i, addr := range addresses {
		t.Logf("Adding server %d at address %s to resolver", i, addr)
		addrs = append(addrs, resolver.Address{Addr: addr})
	}
	r.InitialState(resolver.State{Addresses: addrs})

	// Test different load balancing policies
	tests := []struct {
		name                string
		balancingPolicy     string
		minExpectedDuration time.Duration
		maxExpectedDuration time.Duration
	}{
		{
			name:                "RoundRobin",
			balancingPolicy:     "round_robin",
			minExpectedDuration: 50 * time.Millisecond, // Should connect to a fast server quickly
			maxExpectedDuration: 1 * time.Second,       // Should not take too long
		},
		{
			name:                "PickFirst",
			balancingPolicy:     "pick_first",
			minExpectedDuration: 8 * time.Second,  // Should try first 4 slow servers (4 * 2.5s with some overhead)
			maxExpectedDuration: 15 * time.Second, // Upper bound
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Reset dial attempts for this test
			dialer.dialAttempts = make(map[string]int)

			// Monitor connection establishment time
			dialStart := time.Now()

			// Create context with timeout for connection establishment
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Logf("Attempting to connect with %s balancing policy", tc.balancingPolicy)

			// Establish connection with our balancing policy
			db, err := ydb.Open(ctx, "test://localhost:12345/local", // Используем схему test: которую мы зарегистрировали для manual resolver
				//ydb.WithBalancer(balancers.NoDiscovery()),
				ydb.With(config.WithGrpcOptions(
					grpc.WithResolvers(r),
					grpc.WithContextDialer(dialer.DialContext),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, tc.balancingPolicy)),
					//grpc.WithBlock(),
				)),
			)

			dialDuration := time.Since(dialStart)

			if err != nil {
				t.Fatalf("Failed to dial: %v", err)
			}
			defer db.Close(ctx)

			// Log all dial attempts
			t.Logf("Connection established in %v", dialDuration)
			for i, addr := range addresses {
				attempts := dialer.GetDialAttempts(addr)
				t.Logf("Server %d at %s: %d dial attempts", i, addr, attempts)
			}

			// Create client and make a request
			client := NewSimpleServiceClient(ydb.GRPCConn(db))
			_, err = client.Ping(context.Background(), &emptypb.Empty{})
			if err != nil {
				t.Fatalf("Ping failed: %v", err)
			}

			// Analyze behavior based on balancing policy
			switch tc.balancingPolicy {
			case "round_robin":
				if dialDuration < tc.minExpectedDuration || dialDuration > tc.maxExpectedDuration {
					t.Errorf("round_robin dial took %v, expected between %v and %v",
						dialDuration, tc.minExpectedDuration, tc.maxExpectedDuration)
				}

				// Check if multiple servers were attempted
				attemptedServers := 0
				for _, addr := range addresses {
					if dialer.GetDialAttempts(addr) > 0 {
						attemptedServers++
					}
				}

				// round_robin should try multiple servers in parallel
				if attemptedServers < 2 {
					t.Errorf("Expected round_robin to attempt multiple servers, but only %d were attempted", attemptedServers)
				}

				t.Logf("round_robin successfully established connection")

			case "pick_first":
				if dialDuration < tc.minExpectedDuration {
					t.Errorf("pick_first connected too quickly: %v, expected at least %v",
						dialDuration, tc.minExpectedDuration)
				}

				// Check sequential dialing pattern
				for i := 1; i < totalServers; i++ {
					prevAddr := addresses[i-1]
					currAddr := addresses[i]

					prevAttempts := dialer.GetDialAttempts(prevAddr)
					currAttempts := dialer.GetDialAttempts(currAddr)

					if currAttempts > 0 && prevAttempts == 0 {
						t.Errorf("pick_first should try servers sequentially, but server %d was attempted before server %d",
							i, i-1)
					}
				}

				t.Logf("pick_first eventually found a working server after trying slow ones")
			}

			// Make additional ping requests to verify connection works
			for i := 0; i < 3; i++ {
				_, err = client.Ping(context.Background(), &emptypb.Empty{})
				if err != nil {
					t.Fatalf("Ping %d failed: %v", i, err)
				}
			}

			t.Logf("Successfully completed ping requests with %s policy", tc.balancingPolicy)
		})
	}
}
