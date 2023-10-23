package xstring

import (
	"bytes"
	"sync"
)

type buffer struct {
	bytes.Buffer
}

var buffersPool = sync.Pool{New: func() interface{} {
	return &buffer{}
}}

func (b *buffer) Free() {
	b.Reset()
	buffersPool.Put(b)
}

func Buffer() *buffer {
	return buffersPool.Get().(*buffer)
}
