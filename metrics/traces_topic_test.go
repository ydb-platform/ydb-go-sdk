package metrics

import (
	"context"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicCommitObserverFollowsReaderKind(t *testing.T) {
	dsn := topicmock.GrpcMockTopicConnString(fixenv.New(t), &Ydb_Topic_V1.UnimplementedTopicServiceServer{})
	for _, tc := range []struct {
		name           string
		details        trace.Details
		pull, listener bool
	}{
		{"pull only", trace.TopicReaderStreamEvents, true, false},
		{"listener only", trace.TopicListenerStreamEvents, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := ydb.Open(ctx, dsn, WithTraces(recordingConfig{registry: newRecordingRegistry(), details: tc.details}))
			require.NoError(t, err)
			defer db.Close(ctx)

			topics := topicoptions.ReadTopic("topic")
			reader, err := db.Topic().StartReader("consumer", topics, func(c *topicreaderinternal.ReaderConfig) {
				assert.Equal(t, tc.pull, c.Trace.OnReaderCommittedNotify != nil, "pull ACK observer")
			})
			require.NoError(t, err)
			defer reader.Close(ctx)

			listener, err := db.Topic().StartListener("consumer", topiclistener.BaseHandler{}, topics,
				func(c *topiclistenerinternal.StreamListenerConfig) {
					assert.Equal(t, tc.listener, c.Tracer.OnReaderCommittedNotify != nil, "listener ACK observer")
				})
			require.NoError(t, err)
			defer listener.Close(ctx)
		})
	}
}

func TestTopicCommitObserversCompose(t *testing.T) {
	ctx := t.Context()
	var observations []bool
	opts := []ydb.Option{ydb.WithTraceTopic(trace.Topic{
		OnReaderCommittedNotify: func(info trace.TopicReaderCommittedNotifyInfo) {
			observations = append(observations, info.ReaderInfo.Listener)
		},
	})}
	var captures [2]topicCounterCapture
	for i := range captures {
		opts = append(opts, WithTraces(topicAddCounterConfig{
			recordingConfig: recordingConfig{registry: newRecordingRegistry(), details: trace.TopicListenerStreamEvents},
			capture:         &captures[i],
		}))
	}
	opts = append(opts, ydb.WithTraceTopic(trace.Topic{
		OnReaderCommittedNotify: func(info trace.TopicReaderCommittedNotifyInfo) {
			if info.ReaderInfo.Listener {
				assert.Equal(t, []int64{5}, captures[0].adds["ydb.topic.reader.commit.acknowledged"], "earlier metric observer")
			}
		},
	}))
	dsn := topicmock.GrpcMockTopicConnString(fixenv.New(t), &Ydb_Topic_V1.UnimplementedTopicServiceServer{})
	db, err := ydb.Open(ctx, dsn, opts...)
	require.NoError(t, err)
	defer db.Close(ctx)

	topics := topicoptions.ReadTopic("topic")
	reader, err := db.Topic().StartReader("consumer", topics, func(c *topicreaderinternal.ReaderConfig) {
		c.Trace.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{MessagesCount: 3})
	})
	require.NoError(t, err)
	defer reader.Close(ctx)
	listener, err := db.Topic().StartListener("consumer", topiclistener.BaseHandler{}, topics,
		func(c *topiclistenerinternal.StreamListenerConfig) {
			c.Tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
				ReaderInfo: trace.TopicReaderInfo{Listener: true}, MessagesCount: 5,
			})
		})
	require.NoError(t, err)
	defer listener.Close(ctx)

	require.Equal(t, []bool{false, true}, observations)
	for _, capture := range captures {
		require.Equal(t, []int64{5}, capture.adds["ydb.topic.reader.commit.acknowledged"])
	}
}
