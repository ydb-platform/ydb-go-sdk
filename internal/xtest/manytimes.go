package xtest

import (
	"sync"
	"testing"
	"time"
)

type testManyTimesOptions struct {
	stopAfter time.Duration
}

type TestManyTimesOption func(opts *testManyTimesOptions)

func StopAfter(stopAfter time.Duration) TestManyTimesOption {
	return func(opts *testManyTimesOptions) {
		opts.stopAfter = stopAfter
	}
}

func TestManyTimes(t testing.TB, test TestFunc, opts ...TestManyTimesOption) {
	t.Helper()

	testCounter := 0
	defer func() {
		t.Helper()
		t.Log("test run counter:", testCounter)
	}()

	options := testManyTimesOptions{
		stopAfter: time.Second,
	}

	for _, o := range opts {
		if o != nil {
			o(&options)
		}
	}

	start := time.Now()
	for {
		testCounter++
		// run test, then check stopAfter for guarantee run test least once
		runTest(t, test)

		if time.Since(start) > options.stopAfter || t.Failed() {
			return
		}
	}
}

func TestManyTimesWithName(t *testing.T, name string, test TestFunc) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		t.Helper()
		TestManyTimes(t, test)
	})
}

type TestFunc func(t testing.TB)

func runTest(t testing.TB, test TestFunc) {
	t.Helper()

	tw := &testWrapper{
		TB: t,
	}

	defer tw.doCleanup()

	test(tw)
}

type testWrapper struct {
	testing.TB

	m       sync.Mutex
	logs    []logRecord
	cleanup []func()
}

func (tw *testWrapper) Cleanup(f func()) {
	tw.Helper()

	tw.m.Lock()
	defer tw.m.Unlock()

	tw.cleanup = append(tw.cleanup, f)
}

func (tw *testWrapper) Error(args ...interface{}) {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.Error(args...)
}

func (tw *testWrapper) Errorf(format string, args ...interface{}) {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.Errorf(format, args...)
}

func (tw *testWrapper) Fatal(args ...interface{}) {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.Fatal(args...)
}

func (tw *testWrapper) Fatalf(format string, args ...interface{}) {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.Fatalf(format, args...)
}

func (tw *testWrapper) Fail() {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.Fail()
}

func (tw *testWrapper) FailNow() {
	tw.TB.Helper()

	tw.flushLogs()
	tw.TB.FailNow()
}

func (tw *testWrapper) Log(args ...interface{}) {
	tw.TB.Helper()

	tw.m.Lock()
	defer tw.m.Unlock()

	if tw.TB.Failed() {
		tw.TB.Log(args...)
	} else {
		tw.logs = append(tw.logs, logRecord{
			format: "",
			args:   args,
		})
	}
}

func (tw *testWrapper) Logf(format string, args ...interface{}) {
	tw.TB.Helper()

	tw.m.Lock()
	defer tw.m.Unlock()

	if tw.TB.Failed() {
		tw.TB.Logf(format, args...)
	} else {
		tw.logs = append(tw.logs, logRecord{
			format: format,
			args:   args,
		})
	}
}

func (tw *testWrapper) flushLogs() {
	tw.TB.Helper()

	tw.m.Lock()
	defer tw.m.Unlock()

	for _, rec := range tw.logs {
		if rec.format == "" {
			tw.TB.Log(rec.args...)
		} else {
			tw.TB.Logf(rec.format, rec.args...)
		}
	}

	tw.logs = nil
}

func (tw *testWrapper) doCleanup() {
	tw.Helper()

	for len(tw.cleanup) > 0 {
		last := tw.cleanup[len(tw.cleanup)-1]
		tw.cleanup = tw.cleanup[:len(tw.cleanup)-1]

		last()
	}
}

type logRecord struct {
	format string
	args   []interface{}
}
