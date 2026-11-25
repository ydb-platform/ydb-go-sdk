package topiclistenerinternal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicListenerCloseFromWorkerTrace(t *testing.T) {
	for _, hook := range []string{"first-credit", "messages-delivered", "retirement-credit"} {
		t.Run(hook, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ready := make(chan struct{})
			result := make(chan error, 1)
			var listener *topiclistener.TopicListener
			closeListener := func() {
				<-ready
				result <- listener.Close(ctx)
			}
			var tracer trace.Topic
			var handler topiclistener.EventHandler = topiclistener.BaseHandler{}
			switch hook {
			case "first-credit":
				tracer.OnListenerCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
					if info.BytesDelta > 0 {
						closeListener()
					}
				}
			case "messages-delivered":
				tracer.OnListenerMessagesDelivered = func(trace.TopicReaderMessagesDeliveredInfo) { closeListener() }
			case "retirement-credit":
				sent := make(chan struct{})
				handler = retireAfterCredit{sent: sent}
				tracer.OnListenerCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
					if info.BytesDelta < 0 {
						closeListener()
					}
				}
				tracer.OnListenerSendDataRequest = func(info trace.TopicListenerSendDataRequestInfo) {
					if info.MessageType == "ReadRequest" {
						close(sent)
					}
				}
			}
			server := &orderingTopicServer{errors: make(chan error, 1)}
			db, err := ydb.Open(ctx, topicmock.GrpcMockTopicConnString(fixenv.New(t), server), ydb.WithTraceTopic(tracer))
			require.NoError(t, err)
			defer db.Close(context.Background())
			listener, err = db.Topic().StartListener(
				"consumer", handler, topicoptions.ReadTopic("test-topic"),
			)
			require.NoError(t, err)
			close(ready)
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("worker trace did not return from listener.Close")
			}
		})
	}
}

type retireAfterCredit struct {
	topiclistener.BaseHandler

	sent chan struct{}
}

func (h retireAfterCredit) OnStartPartitionSessionRequest(
	ctx context.Context, _ *topiclistener.EventStartPartitionSession,
) error {
	select {
	case <-h.sent:
		return errors.New("retire the stream before any messages arrive")
	case <-ctx.Done():
		return ctx.Err()
	}
}
