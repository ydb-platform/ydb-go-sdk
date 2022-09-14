//go:build go1.19
// +build go1.19

package xatomic

import "sync/atomic"

type Bool = atomic.Bool
