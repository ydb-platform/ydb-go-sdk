package topicmultiwriter

import "sync/atomic"

type seqNoCounter struct {
	value atomic.Int64
}

func (counter *seqNoCounter) next() int64 {
	return counter.value.Add(1)
}

func (counter *seqNoCounter) advance(lastSeqNo int64) {
	for {
		current := counter.value.Load()
		if lastSeqNo <= current || counter.value.CompareAndSwap(current, lastSeqNo) {
			return
		}
	}
}
