package topicreaderinternal

import "errors"

type partitionSessionCounter interface {
	PartitionSessionCounts() map[string]int64
}

func (r *readerReconnector) PartitionSessionCounts() (map[string]int64, error) {
	var stream batchedStreamReader
	r.m.WithRLock(func() {
		stream = r.streamVal
	})
	if stream == nil {
		return map[string]int64{}, nil
	}
	counter, ok := stream.(partitionSessionCounter)
	if !ok {
		return nil, errors.New("ydb: reader stream does not expose partition session state")
	}

	return counter.PartitionSessionCounts(), nil
}

func (r *topicStreamReaderImpl) PartitionSessionCounts() map[string]int64 {
	var closed bool
	r.m.WithRLock(func() {
		closed = r.closed
	})
	if closed || r.ctx.Err() != nil {
		return nil
	}

	return r.sessionController.ActiveCountsByTopic()
}
