package log

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicCommitACKHookFollowsStaticDetails(t *testing.T) {
	logger := &recordingLogger{}
	for _, details := range []trace.Details{0, trace.TopicReaderMessageEvents} {
		topic := Topic(logger, details)
		require.Nil(t, topic.OnReaderCommittedNotify)
	}

	topic := Topic(logger, trace.TopicReaderStreamEvents)
	require.NotNil(t, topic.OnReaderCommittedNotify)
	topic.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{CommittedOffset: 42})

	require.Len(t, logger.records, 1)
	record := logger.records[0]
	require.Equal(t, "topic reader received commit ack", record.message)
	require.Equal(t, int64(42), fieldValue(record.fields, "committed_offset"))
}

func TestTopicCommitACKHookUsesCurrentMutableDetails(t *testing.T) {
	logger := &recordingLogger{}
	var details mutableTopicDetailer
	topic := Topic(logger, &details)
	require.NotNil(t, topic.OnReaderCommittedNotify)

	info := trace.TopicReaderCommittedNotifyInfo{}
	topic.OnReaderCommittedNotify(info)
	require.Empty(t, logger.records)

	details.mask = trace.TopicReaderStreamEvents
	topic.OnReaderCommittedNotify(info)
	require.Len(t, logger.records, 1)

	details.mask = 0
	topic.OnReaderCommittedNotify(info)
	require.Len(t, logger.records, 1)
}

func TestLazyProtoStringiferUpdateTokenRequest(t *testing.T) {
	const token = "3:serv:CMmNAXXXXXXSA"

	message := &Ydb_Topic.StreamWriteMessage_FromClient{
		ClientMessage: &Ydb_Topic.StreamWriteMessage_FromClient_UpdateTokenRequest{
			UpdateTokenRequest: &Ydb_Topic.UpdateTokenRequest{
				Token: token,
			},
		},
	}

	formatted := lazyProtoStringifer{message: message}.String()

	require.NotContains(t, formatted, token)
	require.Contains(t, formatted, secret.Token(token))
	require.True(t, strings.Contains(formatted, "updateTokenRequest"))
	require.Equal(t, token, message.GetUpdateTokenRequest().GetToken())
}

type mutableTopicDetailer struct {
	mask trace.Details
}

func (d *mutableTopicDetailer) Details() trace.Details {
	return d.mask
}
