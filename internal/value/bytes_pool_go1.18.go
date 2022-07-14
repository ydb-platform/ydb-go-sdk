//go:build go1.18
// +build go1.18

package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/pool"
)

var bytesPool = &bytesPoolType{}

type bytesPoolType struct {
	pool.Pool[bytes.Buffer]
}

func (p *bytesPoolType) Put(b *bytes.Buffer) {
	b.Reset()
	p.Pool.Put(b)
}
