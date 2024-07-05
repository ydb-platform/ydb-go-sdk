//go:build !go1.21
// +build !go1.21

package xcontext

import (
	"context"
)

func WithDone(parent context.Context, done <-chan struct{}) (context.Context, context.CancelFunc) {
	// ignore done chan for go1.20
	return context.WithCancel(parent)
}
