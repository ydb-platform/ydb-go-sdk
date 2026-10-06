//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
)

func serve(s settings) error {
	listeners := make([]net.Listener, s.nodes)
	addresses := make([]string, s.nodes)
	endpoints := make([]*Ydb_Discovery.EndpointInfo, s.nodes)
	var listenConfig net.ListenConfig
	for i := range listeners {
		listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer listener.Close()
		listeners[i], addresses[i] = listener, listener.Addr().String()
		host, port, err := net.SplitHostPort(addresses[i])
		if err != nil {
			return err
		}
		number, err := strconv.ParseUint(port, 10, 32)
		if err != nil {
			return err
		}
		endpoints[i] = &Ydb_Discovery.EndpointInfo{Address: host, Port: uint32(number), NodeId: uint32(i + 1)}
	}
	discovery := &discoveryService{endpoints: endpoints}
	for i, listener := range listeners {
		server := grpc.NewServer()
		defer server.Stop()
		Ydb_Discovery_V1.RegisterDiscoveryServiceServer(server, discovery)
		grpc_testing.RegisterTestServiceServer(server, &workService{
			settings: s, index: i, slots: make(chan struct{}, s.workers),
		})
		go func() { _ = server.Serve(listener) }()
	}
	if err := json.NewEncoder(os.Stdout).Encode(addresses); err != nil {
		return err
	}
	_, err := io.Copy(io.Discard, os.Stdin)

	return err
}

type discoveryService struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	endpoints []*Ydb_Discovery.EndpointInfo
}

func (s *discoveryService) ListEndpoints(
	context.Context, *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	value, err := anypb.New(&Ydb_Discovery.ListEndpointsResult{Endpoints: s.endpoints})
	if err != nil {
		return nil, err
	}

	return &Ydb_Discovery.ListEndpointsResponse{Operation: &Ydb_Operations.Operation{
		Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: value,
	}}, nil
}

type workService struct {
	grpc_testing.UnimplementedTestServiceServer

	settings settings
	index    int
	slots    chan struct{}
}

func (s *workService) EmptyCall(ctx context.Context, _ *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("x-p2c-warm")) > 0 {
		return &grpc_testing.Empty{}, nil
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	delay := s.settings.delay
	if (s.settings.scenario == slowScenario || s.settings.scenario == pinnedScenario) &&
		s.index < max(1, s.settings.nodes/4) ||
		s.settings.scenario == temporaryScenario && s.index == 0 && len(md.Get("x-p2c-slow")) > 0 {
		delay *= 5
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return &grpc_testing.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*workService) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(&grpc_testing.StreamingOutputCallResponse{}); err != nil {
			return err
		}
	}
}
