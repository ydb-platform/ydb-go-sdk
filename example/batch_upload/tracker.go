package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

type Result struct {
	err    error
	pushed int
	failed int
}

type Tracker struct {
	cap    int
	pushed int
	failed int
	done   chan error
	track  chan Result
}

func InitTracker(cap int, infly int) *Tracker {
	t := Tracker{
		cap:    cap,
		pushed: 0,
		failed: 0,
		done:   make(chan error),
		track:  make(chan Result, infly),
	}

	go func() {
		defer close(t.done)
		var err error
		for t.pushed+t.failed < t.cap {
			track, more := <-t.track
			if !more || track.err != nil {
				err = track.err
				break
			}
			t.pushed += track.pushed
			t.failed += track.failed
		}

		t.done <- err
	}()
	return &t
}

func (t *Tracker) report() {
	if t.pushed > 0 {
		fmt.Printf("Success on %v of %v\n", t.pushed, t.cap)
	}
	if t.failed > 0 {
		fmt.Printf("Success on %v of %v\n", t.failed, t.cap)
	}
}

func (t *Tracker) respondSignal(s syscall.Signal) {
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, s)
		for range ch {
			t.report()
		}
	}()
}
