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
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestTopicPartitionSessionMetricPublicSurface(t *testing.T) {
	for _, listener := range []bool{false, true} {
		t.Run(map[bool]string{false: "reader", true: "listener"}[listener], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			address, service := newMetricTopicServer(ctx, t)
			meter := telemetry.NewCollector()
			db, err := ydb.Open(ctx, "grpc://"+address+"/local",
				ydb.WithAnonymousCredentials(), ydb.WithBalancer(balancers.SingleConn()), ydb.WithMeter(meter))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close(context.Background()) })
			child, err := db.With(ctx)
			require.NoError(t, err)

			var closeResources []func(context.Context) error
			for _, driver := range []*ydb.Driver{db, child} {
				if listener {
					resource, startErr := driver.Topic().StartListener(
						"consumer", topiclistener.BaseHandler{}, topicoptions.ReadTopic("topic"),
						topicoptions.WithListenerName("shared"),
					)
					require.NoError(t, startErr)
					closeResources = append(closeResources, resource.Close)
				} else {
					resource, startErr := driver.Topic().StartReader(
						"consumer", topicoptions.ReadTopic("topic"), topicoptions.WithReaderName("shared"),
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
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				for _, closeResource := range closeResources {
					_ = closeResource(cleanupCtx)
				}
			})
			// Both scopes register zero before a partition is admitted.
			assertPartitionSessionMetric(ctx, t, meter, address, 0)
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
			assertPartitionSessionMetric(ctx, t, meter, address, 2)
			// Child teardown unregisters its source, not the shared backend.
			require.NoError(t, child.Close(ctx))
			assertPartitionSessionMetric(ctx, t, meter, address, 1)
			// Resource close unregisters independently of driver teardown.
			require.NoError(t, closeResources[0](ctx))
			data, err := meter.Collect(ctx)
			require.NoError(t, err)
			require.Empty(t, data)
			require.NoError(t, db.Close(ctx))
		})
	}
}

func assertPartitionSessionMetric(
	ctx context.Context, t *testing.T, meter *telemetry.Collector, endpoint string, value int64,
) {
	t.Helper()
	data, err := meter.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data, 1)
	require.Equal(t, "ydb.topic.reader.partition_session.count", data[0].Descriptor.Name)
	require.Equal(t, "{session}", data[0].Descriptor.Unit)
	require.Len(t, data[0].Points, 1)
	require.Equal(t, value, data[0].Points[0].Value)
	attributes := make(map[string]string)
	for _, attribute := range data[0].Points[0].Attributes {
		attributes[attribute.Key] = attribute.Value
	}
	require.Equal(t, map[string]string{
		"endpoint": endpoint, "database": "/local", "topic": "/local/topic",
		"consumer": "consumer", "reader.name": "shared",
	}, attributes)
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
