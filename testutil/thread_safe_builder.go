package testutil

import (
	"strings"
	"sync"
)

type ThreadSafeBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (tsb *ThreadSafeBuilder) WriteString(s string) (int, error) {
	tsb.mu.Lock()
	defer tsb.mu.Unlock()

	return tsb.b.WriteString(s)
}

func (tsb *ThreadSafeBuilder) Write(p []byte) (int, error) {
	tsb.mu.Lock()
	defer tsb.mu.Unlock()

	return tsb.b.Write(p)
}

func (tsb *ThreadSafeBuilder) String() string {
	tsb.mu.Lock()
	defer tsb.mu.Unlock()

	return tsb.b.String()
}

func (tsb *ThreadSafeBuilder) Reset() {
	tsb.mu.Lock()
	defer tsb.mu.Unlock()
	tsb.b.Reset()
}
