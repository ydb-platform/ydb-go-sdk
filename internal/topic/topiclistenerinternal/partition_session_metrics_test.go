package topiclistenerinternal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestListenerPartitionSessionCount(t *testing.T) {
	e := fixenv.New(t)
	stream := StreamListener(e)
	r := &TopicListenerReconnector{streamListener: stream}
	counts, err := r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Equal(t, int64(1), counts[""])
	r.streamListener = nil
	counts, err = r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Empty(t, counts)
}

func TestListenerMetricRegistrationFailureDoesNotConnect(t *testing.T) {
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "topic"}}
	failure := errors.New("registration failed")
	cfg.Metrics.Meter = func(telemetry.Descriptor, telemetry.Int64GaugeCallback) (func() error, error) {
		return nil, failure
	}
	// Nil client and handler would fail if the connection goroutine started.
	listener, err := NewTopicListenerReconnector(nil, &cfg, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, listener)
}

func TestListenerActiveIndexStaysBoundedAcrossSessionStops(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(map[bool]string{true: "graceful", false: "forced"}[graceful], func(t *testing.T) {
			e := fixenv.New(t)
			ctx := sf.Context(e)
			stream := StreamListener(e)
			t.Cleanup(func() { require.NoError(t, stream.Close(xtest.Context(t), ErrUserCloseTopic)) })
			const cycles = 50
			starts := make(chan *PublicEventStartPartitionSession, 1)
			stops := make(chan *PublicEventStopPartitionSession, 1)
			handler := EventHandlerMock(e)
			handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
					starts <- event

					return nil
				}).Times(cycles)
			handler.EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStopPartitionSession) error {
					stops <- event

					return nil
				}).Times(cycles)

			for i := range cycles {
				start := createTestStartPartitionRequest()
				id := rawtopicreader.PartitionSessionID(1000 + i)
				start.PartitionSession.PartitionSessionID = id
				start.PartitionSession.Path = "topic"
				require.NoError(t, stream.routeMessage(ctx, start))
				xtest.Receive(t, starts, "start request").Confirm()
				xtest.Receive(t, stream.hasNewMessagesToSend, "start acknowledgement")
				require.Equal(t, int64(1), stream.sessions.ActiveCountsByTopic()["topic"])

				require.NoError(t, stream.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
					PartitionSessionID: id, Graceful: graceful,
				}))
				xtest.Receive(t, stops, "stop request").Confirm()
				if graceful {
					xtest.Receive(t, stream.hasNewMessagesToSend, "stop acknowledgement")
				}
				require.Zero(t, stream.sessions.ActiveCountsByTopic()["topic"])
			}
			// The fixture's original session is still active. The metric's
			// scan set must not include the retained protocol lookup history.
			active := reflect.Indirect(reflect.ValueOf(stream.sessions)).FieldByName("active").Elem()
			require.Equal(t, 1, active.FieldByName("sessions").Len())
		})
	}
}

func TestListenerPartitionSessionCountAfterProtocolStop(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(map[bool]string{true: "graceful", false: "forced"}[graceful], func(t *testing.T) {
			e := fixenv.New(t)
			ctx := sf.Context(e)
			stream := StreamListener(e)
			t.Cleanup(func() { require.NoError(t, stream.Close(xtest.Context(t), ErrUserCloseTopic)) })
			reconnector := &TopicListenerReconnector{streamListener: stream}
			var callback telemetry.Int64GaugeCallback
			stream.cfg.Metrics.Meter = func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
				callback = cb

				return func() error {
					callback = nil

					return nil
				}, nil
			}
			reg, err := topicreadercommon.RegisterPartitionSessionCount(
				stream.cfg.Metrics, "consumer",
				[]*topicreadercommon.PublicReadSelector{{Path: "topic"}}, reconnector.PartitionSessionCounts,
			)
			require.NoError(t, err)
			t.Cleanup(reg)
			metricValue := func() int64 {
				var values []int64
				require.NoError(t, callback(ctx, func(value int64, _ ...telemetry.Attribute) {
					values = append(values, value)
				}))
				require.Len(t, values, 1)

				return values[0]
			}

			handler := EventHandlerMock(e)
			starts := make(chan *PublicEventStartPartitionSession, 2)
			handler.EXPECT().OnStartPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStartPartitionSession) error {
					starts <- event

					return nil
				}).Times(2)
			events := make(chan *PublicEventStopPartitionSession, 1)
			lateForced := make(chan error, 1)
			handler.EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStopPartitionSession) error {
					events <- event

					return nil
				})
			handler.EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, _ *PublicEventStopPartitionSession) error {
					lateForced <- ctx.Err()

					return nil
				})

			first := createTestStartPartitionRequest()
			first.PartitionSession.PartitionSessionID = 100
			first.PartitionSession.Path = "topic"
			require.NoError(t, stream.routeMessage(ctx, first))
			start := xtest.Receive(t, starts, "start request")
			require.Equal(t, int64(1), metricValue(), "an assigned session counts before the handler confirms it")
			start.Confirm()
			xtest.Receive(t, stream.hasNewMessagesToSend, "start acknowledgement")
			require.Equal(t, int64(1), metricValue())

			require.NoError(t, stream.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
				PartitionSessionID: first.PartitionSession.PartitionSessionID, Graceful: graceful,
			}))
			if !graceful {
				require.Equal(t, int64(0), metricValue(), "forced revocation takes effect at routing")
			}
			event := xtest.Receive(t, events, "stop notification")
			if graceful {
				require.Equal(t, int64(1), metricValue())
				event.Confirm()
				xtest.Receive(t, stream.hasNewMessagesToSend, "graceful stop acknowledgement")
			} else {
				require.Equal(t, int64(0), metricValue())
				event.Confirm()
			}
			require.Equal(t, int64(0), metricValue())

			replacement := createTestStartPartitionRequest()
			replacement.PartitionSession = first.PartitionSession
			replacement.PartitionSession.PartitionSessionID++
			require.NoError(t, stream.routeMessage(ctx, replacement))
			start = xtest.Receive(t, starts, "replacement start")
			require.Equal(t, int64(1), metricValue())
			start.Confirm()
			xtest.Receive(t, stream.hasNewMessagesToSend, "replacement acknowledgement")
			require.Equal(t, int64(1), metricValue())

			// A late forced notification still reaches the callback with the
			// revoked session's context and must not retire its successor.
			require.NoError(t, stream.routeMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
				PartitionSessionID: first.PartitionSession.PartitionSessionID,
			}))
			require.ErrorIs(t, xtest.Receive(t, lateForced, "late forced notification"), context.Canceled)
			require.Equal(t, int64(1), metricValue())

			require.NoError(t, stream.Close(ctx, ErrUserCloseTopic))
			require.Equal(t, int64(0), metricValue())
		})
	}
}
