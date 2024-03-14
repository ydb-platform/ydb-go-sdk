package xcontext

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithDone(t *testing.T) {
	require.NotPanics(t, func() {
		for range make([]struct{}, 100000) {
			ctx1, cancel1 := context.WithCancel(context.Background())
			done := make(chan struct{})
			goroutines := make(chan struct{}, 1)
			ctx2, cancel2 := withDone(ctx1, done,
				func(t *withDoneOpts) {
					t.onStart = func() {
						goroutines <- struct{}{}
					}
				},
				func(t *withDoneOpts) {
					t.onDone = func() {
						goroutines <- struct{}{}
					}
				},
			)
			go func() {
				cancel1()
			}()
			go func() {
				cancel2()
			}()
			go func() {
				close(done)
			}()
			<-goroutines
			<-ctx2.Done()
			select {
			case <-time.After(time.Second):
				t.Failed()
			case <-goroutines:
			}
			close(goroutines)
		}
	})
}
