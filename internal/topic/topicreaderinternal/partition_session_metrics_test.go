package topicreaderinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestPartitionSessionCountStop(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		t.Run(map[bool]string{true: "graceful", false: "forced"}[graceful], func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			require.Equal(t, int64(1), e.reader.PartitionSessionCounts()["/test"])
			request := &rawtopicreader.StopPartitionSessionRequest{
				PartitionSessionID: e.partitionSessionID, Graceful: graceful,
			}
			require.NoError(t, e.reader.onStopPartitionSessionRequest(request))
			want := int64(0)
			if graceful {
				want = 1
			}
			require.Equal(t, want, e.reader.PartitionSessionCounts()["/test"])
			if graceful {
				e.stream.EXPECT().Send(&rawtopicreader.StopPartitionSessionResponse{
					PartitionSessionID: e.partitionSessionID,
				}).Return(nil)
			}
			require.NoError(t, e.reader.onStopPartitionSessionRequestFromBuffer(request))
			require.Empty(t, e.reader.PartitionSessionCounts())
		})
	}
}

func TestPartitionSessionCountTracksCurrentStream(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	r := &readerReconnector{}
	counts, err := r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Empty(t, counts)
	r.streamVal = e.reader
	counts, err = r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Equal(t, int64(1), counts["/test"])
	r.streamVal = nil
	counts, err = r.PartitionSessionCounts()
	require.NoError(t, err)
	require.Empty(t, counts)
	r.streamVal = NewMockbatchedStreamReader(gomock.NewController(t))
	_, err = r.PartitionSessionCounts()
	require.Error(t, err)
}

func TestReaderMetricRegistrationFailure(t *testing.T) {
	failure := errors.New("registration failed")
	reader, err := NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			return nil, errors.New("unexpected connection")
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "topic"}},
		func(cfg *ReaderConfig) {
			cfg.Metrics.Meter = func(telemetry.Descriptor, telemetry.Int64GaugeCallback) (func() error, error) {
				return nil, failure
			}
		},
	)
	require.ErrorIs(t, err, failure)
	require.Nil(t, reader.reader)
}
