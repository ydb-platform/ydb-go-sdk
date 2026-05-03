package topicreadercommon

import "sync/atomic"

var globalReaderCounter atomic.Int64

func NextReaderID() int64 {
	return globalReaderCounter.Add(1)
}
