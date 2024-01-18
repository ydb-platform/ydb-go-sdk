package xatomic

import "sync/atomic"

type (
	Bool   = atomic.Bool
	Int64  = atomic.Int64
	Uint32 = atomic.Uint32
	Uint64 = atomic.Uint64
)
