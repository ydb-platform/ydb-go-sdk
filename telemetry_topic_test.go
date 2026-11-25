package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// Reader must remain usable as a map key for API compatibility.
var _ map[topicreader.Reader]struct{}

func TestTopicPartitionSessionMetricPublicSurface(t *testing.T) {
	for _, listener := range []bool{false, true} {
		t.Run(map[bool]string{false: "reader", true: "listener"}[listener], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			address, service := newMetricTopicServer(ctx, t)
			meter, collect := newMetricMeter()
			db, err := ydb.Open(ctx, "grpc://"+address+"/local",
				ydb.WithAnonymousCredentials(), ydb.WithBalancer(balancers.SingleConn()), ydb.WithMeter(meter))
			require.NoError(t, err)
			var closeResources []func(context.Context) error
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				for _, closeResource := range closeResources {
					if closeResource != nil {
						require.NoError(t, closeResource(cleanupCtx))
					}
				}
				require.NoError(t, db.Close(cleanupCtx))
			})
			child, err := db.With(ctx)
			require.NoError(t, err)
			for i, driver := range []*ydb.Driver{db, child} {
				name := []string{"parent", "child"}[i]
				if listener {
					resource, startErr := driver.Topic().StartListener(
						"consumer", topiclistener.BaseHandler{}, topicoptions.ReadTopic("topic"),
						topicoptions.WithListenerReaderName(name),
					)
					require.NoError(t, startErr)
					closeResources = append(closeResources, resource.Close)
					require.NoError(t, resource.WaitInit(ctx))
				} else {
					resource, startErr := driver.Topic().StartReader(
						"consumer", topicoptions.ReadTopic("topic"), topicoptions.WithReaderName(name),
					)
					require.NoError(t, startErr)
					readDone := make(chan struct{})
					readCtx, stopRead := context.WithCancel(ctx)
					go func() {
						defer close(readDone)
						_, _ = resource.ReadMessage(readCtx)
					}()
					closeResources = append(closeResources, func(ctx context.Context) error {
						stopRead()
						select {
						case <-readDone:
						case <-ctx.Done():
							return ctx.Err()
						}

						return resource.Close(ctx)
					})
					require.NoError(t, resource.WaitInit(ctx))
				}
			}
			// Independent native gauge series register before admission.
			assertPartitionSessionMetric(ctx, t, collect, address, map[string]int64{"parent": 0, "child": 0})
			for range 2 {
				select {
				case session := <-service.sessions:
					close(session.start)
					select {
					case <-session.confirmed:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			assertPartitionSessionMetric(ctx, t, collect, address, map[string]int64{"parent": 1, "child": 1})
			// Resources own callbacks; driver close does not maintain a registry.
			require.NoError(t, closeResources[1](ctx))
			closeResources[1] = nil
			require.NoError(t, child.Close(ctx))
			assertPartitionSessionMetric(ctx, t, collect, address, map[string]int64{"parent": 1})
			// Resource close unregisters independently of driver teardown.
			require.NoError(t, closeResources[0](ctx))
			closeResources[0] = nil
			data, err := collect(ctx)
			require.NoError(t, err)
			require.Empty(t, data)
			require.NoError(t, db.Close(ctx))
		})
	}
}

func assertPartitionSessionMetric(
	ctx context.Context, t *testing.T, collect func(context.Context) ([]observedMetric, error),
	endpoint string, want map[string]int64,
) {
	t.Helper()
	data, err := collect(ctx)
	require.NoError(t, err)
	values := make(map[string]int64)
	for _, metric := range data {
		require.Equal(t, "ydb.topic.reader.partition_session.count", metric.Descriptor.Name)
		require.Equal(t, "{session}", metric.Descriptor.Unit)
		for _, point := range metric.Points {
			attributes := make(map[string]string)
			for _, attribute := range point.Attributes {
				attributes[attribute.Key] = attribute.Value
			}
			name := attributes["reader.name"]
			require.Equal(t, map[string]string{
				"endpoint": endpoint, "database": "/local", "topic": "/local/topic",
				"consumer": "consumer", "reader.name": name,
			}, attributes)
			require.NotContains(t, values, name, "duplicate series")
			values[name] = point.Value
		}
	}
	require.Equal(t, want, values)
}

func newMetricTopicServer(ctx context.Context, t *testing.T) (string, *metricTopicServer) {
	t.Helper()
	config := &net.ListenConfig{}
	listener, err := config.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &metricTopicServer{sessions: make(chan *metricReadSession, 2)}
	server := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: time.Second, PermitWithoutStream: true,
	}))
	Ydb_Topic_V1.RegisterTopicServiceServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		<-done
	})

	return listener.Addr().String(), service
}

type metricTopicServer struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	sessions chan *metricReadSession
}

type metricReadSession struct {
	start, confirmed chan struct{}
}

func (s *metricTopicServer) StreamRead(stream Ydb_Topic_V1.TopicService_StreamReadServer) error {
	init, err := stream.Recv()
	if err != nil {
		return err
	}
	if init.GetInitRequest() == nil {
		return fmt.Errorf("expected init request")
	}
	if err = stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{SessionId: "metric-session"},
		},
	}); err != nil {
		return err
	}
	session := &metricReadSession{start: make(chan struct{}), confirmed: make(chan struct{})}
	select {
	case s.sessions <- session:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case <-session.start:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if err = stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest{
			StartPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest{
				PartitionSession: &Ydb_Topic.StreamReadMessage_PartitionSession{
					PartitionSessionId: 1, Path: "/local/topic", PartitionId: 1,
				},
				PartitionOffsets: &Ydb_Topic.OffsetsRange{},
			},
		},
	}); err != nil {
		return err
	}
	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if message.GetStartPartitionSessionResponse() != nil {
			close(session.confirmed)
		}
	}
}

type observedMetric struct {
	Descriptor telemetry.Descriptor
	Points     []observedPoint
}

type observedPoint struct {
	Value      int64
	Attributes []telemetry.Attribute
}

func newMetricMeter() (telemetry.Meter, func(context.Context) ([]observedMetric, error)) {
	type registration struct {
		descriptor telemetry.Descriptor
		callback   telemetry.Int64GaugeCallback
	}
	registrations := make(map[*registration]struct{})
	var mu sync.Mutex
	meter := func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		mu.Lock()
		defer mu.Unlock()
		reg := &registration{desc, callback}
		registrations[reg] = struct{}{}

		return func() error {
			mu.Lock()
			defer mu.Unlock()
			delete(registrations, reg)

			return nil
		}, nil
	}
	collect := func(ctx context.Context) ([]observedMetric, error) {
		mu.Lock()
		defer mu.Unlock()
		var metrics []observedMetric
		for reg := range registrations {
			data := observedMetric{Descriptor: reg.descriptor}
			if err := reg.callback(ctx, func(value int64, attrs ...telemetry.Attribute) {
				data.Points = append(data.Points, observedPoint{Value: value, Attributes: slices.Clone(attrs)})
			}); err != nil {
				return nil, err
			}
			if len(data.Points) != 0 {
				metrics = append(metrics, data)
			}
		}

		return metrics, nil
	}

	return meter, collect
}
