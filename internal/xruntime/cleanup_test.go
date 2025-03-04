package xruntime

import (
	"runtime"
	"testing"
	"time"
)

func TestAddCleanup(t *testing.T) {
	cleanupCalled := make(chan struct{})
	func() {
		v := &struct {
			a int64
			b string
			c bool
		}{}
		AddCleanup(v, func(cleanupCalled chan struct{}) {
			close(cleanupCalled)
		}, cleanupCalled)
	}()
	runtime.GC()
	select {
	case <-cleanupCalled:
	case <-time.After(time.Second):
		t.Fail()
	}
}
