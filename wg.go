package ydb

import (
	"sync"
)

// wrap sync.WaitGroup to allow for 'wait 1st' logic
type WG interface {
	Done()
	Wait()
	Add(delta int)
}

var _ WG = &sync.WaitGroup{}

type wgImpl struct {
	wg        *sync.WaitGroup
	once      *sync.Once
	firstDone chan struct{}
}

func (w wgImpl) Done() {
	w.once.Do(w.first)
	w.wg.Done()
}

func (w wgImpl) first() {
	close(w.firstDone)
}

func (w wgImpl) Wait() {
	w.wg.Wait()
}

func (w wgImpl) Add(delta int) {
	w.wg.Add(delta)
}

func (w wgImpl) WaitFirst() {
	<-w.firstDone
}
