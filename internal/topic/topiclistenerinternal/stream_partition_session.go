package topiclistenerinternal

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

type partitionSession struct {
	session  *topicreadercommon.PartitionSession
	handler  EventHandler
	messages []rawtopicreader.ServerMessage
}

func newPartitionSession(
	session *topicreadercommon.PartitionSession,
	handler EventHandler,
) *partitionSession {
	return &partitionSession{
		session: session,
		handler: handler,
	}
}

func (s *partitionSession) addMessage(message rawtopicreader.ServerMessage) {
	s.messages = append(s.messages, message)
}
