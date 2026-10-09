package ydb_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				require.NoError(t, db.Close(cleanupCtx))
			})
			child, err := db.With(ctx)
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
			})
			for i, driver := range []*ydb.Driver{db, child} {
				name := []string{"parent", "child"}[i]
				if listener {
					resource, startErr := driver.Topic().StartListener(
						"consumer", topiclistener.BaseHandler{}, topicoptions.ReadTopic("topic"),
						topicoptions.WithListenerName(name),
					)
					require.NoError(t, startErr)
					closeResources = append(closeResources, resource.Close)
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
				}
			}
			// Independent native gauge series register before admission.
			assertPartitionSessionMetric(ctx, t, collect, address, map[string]int64{"parent": 0, "child": 0})
			for range 2 {
				var session *metricReadSession
				select {
				case session = <-service.sessions:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case session.start <- struct{}{}:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-session.confirmed:
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
	start     chan struct{}
	confirmed chan struct{}
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
