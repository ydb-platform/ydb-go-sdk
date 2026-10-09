package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestListenerPartitionSessionCount(t *testing.T) {
	e := fixenv.New(t)
	stream := StreamListener(e)
	r := &TopicListenerReconnector{streamListener: stream}
	counts, err := r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Equal(t, int64(1), counts[""])
	PartitionSession(e).Close()
	counts, err = r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Empty(t, counts)
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

func TestListenerPartitionSessionCountAfterProtocolStop(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(map[bool]string{true: "graceful", false: "forced"}[graceful], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			storage := &topicreadercommon.PartitionSessionStorage{}
			session := topicreadercommon.NewPartitionSession(ctx, "topic", 1, 1, "", 1, 1, 0)
			t.Cleanup(session.Close)
			require.NoError(t, storage.Add(session))
			reconnector := &TopicListenerReconnector{streamListener: &streamListener{sessions: storage}}
			var callback telemetry.Int64GaugeCallback
			meter := func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
				callback = cb

				return func() error {
					callback = nil

					return nil
				}, nil
			}
			reg, err := topicreadercommon.RegisterPartitionSessionCount(
				topicreadercommon.ReaderMetricsConfig{Meter: meter}, "consumer",
				[]*topicreadercommon.PublicReadSelector{{Path: "topic"}}, reconnector.PartitionSessionCounts,
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reg()) })
			metricValue := func() int64 {
				var values []int64
				require.NoError(t, callback(ctx, func(value int64, _ ...telemetry.Attribute) {
					values = append(values, value)
				}))
				require.Len(t, values, 1)

				return values[0]
			}

			handler := NewMockEventHandler(gomock.NewController(t))
			events := make(chan *PublicEventStopPartitionSession, 1)
			handler.EXPECT().OnStopPartitionSessionRequest(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, event *PublicEventStopPartitionSession) error {
					events <- event

					return nil
				}).Times(2)
			sender := newMockMessageSender()
			worker := NewPartitionWorker(session.StreamPartitionSessionID, session, sender, handler,
				nil, &trace.Topic{}, "listener")
			done := make(chan error, 1)
			go func() {
				done <- worker.processRawServerMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
					PartitionSessionID: session.StreamPartitionSessionID, Graceful: graceful,
				})
			}()
			var event *PublicEventStopPartitionSession
			select {
			case event = <-events:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if graceful {
				require.Equal(t, int64(1), metricValue())
			} else {
				require.Equal(t, int64(0), metricValue())
			}
			event.Confirm()
			select {
			case err = <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, int64(0), metricValue())

			// A later forced notification still reaches the callback after a
			// completed stop; retirement must not cancel that callback context.
			go func() {
				done <- worker.processRawServerMessage(ctx, &rawtopicreader.StopPartitionSessionRequest{
					PartitionSessionID: session.StreamPartitionSessionID,
				})
			}()
			select {
			case event = <-events:
				event.Confirm()
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case err = <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			replacement := topicreadercommon.NewPartitionSession(ctx, "topic", 2, 1, "", 2, 2, 0)
			t.Cleanup(replacement.Close)
			require.NoError(t, storage.Add(replacement))
			require.Equal(t, int64(1), metricValue())
		})
	}
}
