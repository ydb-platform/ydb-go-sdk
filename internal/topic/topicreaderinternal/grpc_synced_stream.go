package topicreaderinternal

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

var _ RawTopicReaderStream = &syncedStream{}

type syncedStream struct {
	m      sync.Mutex
	stream RawTopicReaderStream
}

func (s *syncedStream) Recv() (rawtopicreader.ServerMessage, error) {
	// not need synced
	return s.stream.Recv()
}

func (s *syncedStream) Send(msg rawtopicreader.ClientMessage) error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.stream.Send(msg)
}

func (s *syncedStream) CloseSend() error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.stream.CloseSend()
}
