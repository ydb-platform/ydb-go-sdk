package topiclistenerinternal

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type partitionSession struct {
	session  *topicreadercommon.PartitionSession
	handler  EventHandler
	messages *xsync.UnboundedChan[sessionMessage]
}

func newPartitionSession(
	session *topicreadercommon.PartitionSession,
	handler EventHandler,
) *partitionSession {
	return &partitionSession{
		session:  session,
		handler:  handler,
		messages: xsync.NewUnboundedChanWithMergeLastItem(mergeMessages),
	}
}

func (s *partitionSession) onServerMessage(message rawtopicreader.ServerMessage) {
	s.messages.Send(sessionMessage{server: message})
}

func (s *partitionSession) onDataBatches(batches []*topicreadercommon.PublicBatch) {
	s.messages.Send(sessionMessage{batches: batches})
}

func mergeMessages(first, second sessionMessage) (result sessionMessage, merged bool) {
	if !first.IsBatches() || !second.IsBatches() {
		return sessionMessage{}, false
	}

	batch := first.batches[len(first.batches)-1]
	var err error
	for i := range second.batches {
		batch, err = topicreadercommon.BatchAppend(batch, second.batches[i])
		if err != nil {
			panic(fmt.Sprintf("ydb: sdk internal error. Try to merge incompatible batches"))
		}
	}
	first.batches[len(first.batches)-1] = batch
	return first, true
}

type sessionMessage struct {
	batches []*topicreadercommon.PublicBatch
	server  rawtopicreader.ServerMessage
}

func (s *sessionMessage) IsBatches() bool {
	return len(s.batches) > 0
}
