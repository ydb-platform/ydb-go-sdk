package repeater

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRepeaterNoWakeUpsAfterStop(t *testing.T) {
	testFunc := func(i int) {
		if i%100 == 0 {
			t.Logf("check %d ...", i)
			defer t.Logf("check %d done", i)
		}
		var (
			interval    = time.Millisecond
			wakeUpStart = make(chan struct{})
			wakeUpDone  = make(chan struct{})
		)
		r := New(interval, func(ctx context.Context) (err error) {
			wakeUpStart <- struct{}{}
			<-wakeUpDone
			return nil
		})
		<-wakeUpStart            // wait first wake up
		wakeUpDone <- struct{}{} // unlock exit from first wake up
		<-wakeUpStart            // wait first wake up
		r.stop(func() {          // call stop
			wakeUpDone <- struct{}{} // unlock exit from second wake up
		})
		select {
		case <-wakeUpStart:
			t.Fatalf("unexpected wake up after stop")
		case <-time.After(2 * interval):
			// ok
		}
	}
	for i := 0; i < 10000; i++ {
		testFunc(i)
	}
}

func TestRepeaterForceLogBackoff(t *testing.T) {
	var (
		delays = map[int]time.Duration{
			0: 0 * time.Second,
			1: 1 * time.Second,
			2: 2 * time.Second,
			3: 4 * time.Second,
			4: 8 * time.Second,
			5: 16 * time.Second,
			6: 32 * time.Second,
		}
		tolerance = 250 * time.Millisecond
	)
	testFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		var (
			wakeUps    = 0
			lastWakeUp = time.Now()
		)
		r := New(time.Minute, func(ctx context.Context) (err error) {
			sinceLastWakeUp := time.Since(lastWakeUp)
			if d, ok := delays[wakeUps]; !ok {
				t.Fatalf("unexpected wake up's count: %v", wakeUps)
			} else if sinceLastWakeUp < d || sinceLastWakeUp > (d+tolerance) {
				t.Fatalf("unexpected wake up delay: %v, exp: [%v...%v]", sinceLastWakeUp, d, d+tolerance)
			}
			lastWakeUp = time.Now()
			wakeUps++
			return fmt.Errorf("spatial error for force with log backoff")
		})
		r.Force()
		time.Sleep(44 * time.Second)
		r.Stop()
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		// nolint: govet
		go testFunc(&wg)
	}
	wg.Wait()
}
