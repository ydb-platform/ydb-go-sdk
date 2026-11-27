package topiclistenerinternal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderSessionErrorReportsTracePanicsAndPreservesInitError(t *testing.T) {
	const readerPanic = "reader option session error"
	const driverPanic = "driver session error"

	for _, tt := range []struct {
		name       string
		driverHook func(trace.TopicReaderSessionErrorInfo)
		want       []any
	}{
		{name: "reader option", want: []any{readerPanic}},
		{
			name:       "driver and reader options",
			driverHook: func(trace.TopicReaderSessionErrorInfo) { panic(driverPanic) },
			want:       []any{driverPanic, readerPanic},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			var mu sync.Mutex
			var observed []any
			ran := make(chan struct{}, 1)
			db, err := ydb.Open(ctx, topicmock.GrpcMockTopicConnString(fixenv.New(t), &deniedListenerServer{}),
				ydb.WithPanicCallback(func(p any) {
					mu.Lock()
					defer mu.Unlock()
					observed = append(observed, p)
				}),
				ydb.WithTraceTopic(trace.Topic{OnReaderSessionError: tt.driverHook}),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close(ctx)) }()

			reader, err := db.Topic().StartReader("consumer", topicoptions.ReadTopic("topic"),
				topicoptions.WithReaderTrace(trace.Topic{
					OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
						ran <- struct{}{}
						panic(readerPanic)
					},
				}),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close(ctx)) }()

			require.Equal(t, codes.PermissionDenied, status.Code(reader.WaitInit(ctx)))
			require.Len(t, ran, 1, "reader option hook must have run")
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tt.want, observed)
		})
	}
}

func TestWriterCloseReportsOptionTracePanic(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const marker = "writer option close"
	var observed []any
	db, err := ydb.Open(ctx, topicmock.GrpcMockTopicConnString(fixenv.New(t), &deniedListenerServer{}),
		ydb.WithPanicCallback(func(p any) { observed = append(observed, p) }),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()

	writer, err := db.Topic().StartWriter("topic",
		topicoptions.WithWriterTrace(trace.Topic{
			OnWriterClose: func(trace.TopicWriterCloseStartInfo) func(trace.TopicWriterCloseDoneInfo) {
				panic(marker)
			},
		}),
	)
	require.NoError(t, err)

	require.NotPanics(t, func() { require.NoError(t, writer.Close(ctx)) })
	require.Equal(t, []any{marker}, observed)
}
