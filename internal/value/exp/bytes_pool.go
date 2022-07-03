//go:build !go1.18
// +build !go1.18

package value

import (
	"bytes"
	"sync"
)

var bytesPool bytesPoolType

type bytesPoolType struct {
	sync.Pool
}

func (p bytesPoolType) Get() *bytes.Buffer {
	v := p.Pool.Get()
	if v == nil {
		v = new(bytes.Buffer)
	}
	return v.(*bytes.Buffer)
}

func (p bytesPoolType) Put(b *bytes.Buffer) {
	b.Reset()
	p.Pool.Put(b)
}
