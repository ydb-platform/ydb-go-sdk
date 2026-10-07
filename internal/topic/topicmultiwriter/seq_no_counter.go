package topicmultiwriter

import "sync/atomic"

type seqNoCounter struct {
	atomic.Int64
}

func (counter *seqNoCounter) next() int64 {
	return counter.Add(1)
}

func (counter *seqNoCounter) advance(lastSeqNo int64) {
	for {
		current := counter.Load()
		if lastSeqNo <= current || counter.CompareAndSwap(current, lastSeqNo) {
			return
		}
	}
}
