package xtest

import (
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func MakeSyncedTest(t *testing.T) (st *SyncedTest) {
	return &SyncedTest{
		T: t,
	}
}

type SyncedTest struct {
	m sync.Mutex
	*testing.T
}

func (s *SyncedTest) checkGoroutinesLeak() {
	var (
		bb               = make([]byte, 2<<32)
		currentGoroutine string
	)

	time.Sleep(time.Millisecond)

	if n := runtime.Stack(bb, false); n < len(bb) {
		currentGoroutine = string(regexp.MustCompile("^goroutine \\d+ ").Find(bb[:n]))
	}

	if n := runtime.Stack(bb, true); n < len(bb) {
		bb = bb[:n]
	}

	goroutines := strings.Split(string(bb), "\n\n")
	unexpectedGoroutines := make([]string, 0, len(goroutines))

	for _, g := range goroutines {
		if strings.HasPrefix(g, currentGoroutine) {
			continue
		}
		stack := strings.Split(g, "\n")
		firstFunction := stack[1]
		state := strings.Trim(regexp.MustCompile("\\[.*\\]").FindString(regexp.MustCompile("^goroutine \\d+ \\[.*\\]").FindString(stack[0])), "[]")
		switch {
		case strings.HasPrefix(firstFunction, "testing.RunTests"),
			strings.HasPrefix(firstFunction, "testing.(*T).Run"),
			strings.HasPrefix(firstFunction, "testing.(*T).Parallel"),
			strings.HasPrefix(firstFunction, "testing.runFuzzing"),
			strings.HasPrefix(firstFunction, "testing.runFuzzTests"):
			if strings.Contains(state, "chan receive") {
				continue
			}

		case strings.HasPrefix(firstFunction, "runtime.goexit") && state == "syscall":
			continue

		case strings.HasPrefix(firstFunction, "os/signal.signal_recv"),
			strings.HasPrefix(firstFunction, "os/signal.loop"):
			if strings.Contains(g, "runtime.ensureSigM") {
				continue
			}

			//case strings.HasPrefix(firstFunction, "syscall.syscall"):
			//	if strings.Contains(state, "syscall") {
			//		continue
			//	}
		}

		unexpectedGoroutines = append(unexpectedGoroutines, g)
	}
	if l := len(unexpectedGoroutines); l > 0 {
		s.T.Errorf("found %d unexpected goroutines:\n%s", l, strings.Join(unexpectedGoroutines, "\n"))
	}
}

func (s *SyncedTest) Cleanup(f func()) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Cleanup(f)
}

func (s *SyncedTest) Error(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Error(args...)
}

func (s *SyncedTest) Errorf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Errorf(format, args...)
}

func (s *SyncedTest) Fail() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Fail()
}

func (s *SyncedTest) FailNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.FailNow()
}

func (s *SyncedTest) Failed() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Failed()
}

func (s *SyncedTest) Fatal(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Fatal(args...)
}

func (s *SyncedTest) Fatalf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Fatalf(format, args...)
}

func (s *SyncedTest) Log(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Log(args...)
}

func (s *SyncedTest) Logf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Logf(format, args...)
}

func (s *SyncedTest) Name() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Name()
}

func (s *SyncedTest) Run(name string, f func(t *testing.T)) bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Run(name, f)
}

func (s *SyncedTest) RunSynced(name string, f func(t *SyncedTest)) bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Run(name, func(t *testing.T) {
		defer s.checkGoroutinesLeak()

		syncedTest := MakeSyncedTest(t)
		f(syncedTest)
	})
}

func (s *SyncedTest) Setenv(key, value string) {
	panic("not implemented")
}

func (s *SyncedTest) Skip(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Skip(args...)
}

func (s *SyncedTest) SkipNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.SkipNow()
}

func (s *SyncedTest) Skipf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()
	s.T.Skipf(format, args...)
}

func (s *SyncedTest) Skipped() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Skipped()
}

func (s *SyncedTest) TempDir() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.TempDir()
}
