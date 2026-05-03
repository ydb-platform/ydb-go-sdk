package main

import (
	"net/http"
	"sync/atomic"
)

type balancer struct {
	handlers []http.Handler
	counter  atomic.Int32
}

func newBalancer(handlers ...http.Handler) *balancer {
	return &balancer{
		handlers: handlers,
	}
}

func (b *balancer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	counter := b.counter.Add(1)
	if counter < 0 {
		counter = -counter
	}
	index := int(counter-1) % len(b.handlers)
	b.handlers[index].ServeHTTP(writer, request)
}
