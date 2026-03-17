//go:build go1.24

package xtest

import (
	"context"
	"testing"
)

func rootContext(t testing.TB) context.Context {
	return t.Context()
}
