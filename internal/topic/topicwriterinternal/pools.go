package topicwriterinternal

import (
	"bytes"
	"sync"
)

var (
	globalBufferPool                   = &sync.Pool{}
	globalMessageWithContentSlicesPool = &sync.Pool{}
)

func newBuffer() *bytes.Buffer {
	if buf := globalBufferPool.Get(); buf != nil {
		return buf.(*bytes.Buffer)
	}
	return &bytes.Buffer{}
}

func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	globalBufferPool.Put(buf)
}

func newContentMessagesSlice() *messageWithDataContentSlice {
	if stored := globalMessageWithContentSlicesPool.Get(); stored != nil {
		return stored.(*messageWithDataContentSlice)
	}

	return &messageWithDataContentSlice{}
}

func putContentMessagesSlice(obj *messageWithDataContentSlice) {
	for i := range obj.m {
		obj.m[i] = messageWithDataContent{}
	}
	obj.m = obj.m[:0]
	globalMessageWithContentSlicesPool.Put(obj)
}
