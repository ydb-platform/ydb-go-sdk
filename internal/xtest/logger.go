package xtest

import (
	"sync"
	"testing"
)

func MakeSyncedTest(t *testing.T) *SyncedTest {
	return &SyncedTest{
		T: t,
	}
}

type SyncedTest struct {
	m sync.Mutex
	*testing.T
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

// must direct called
// func (s *SyncedTest) Helper() {
//	s.m.Lock()
//	defer s.m.Unlock()
//	s.T.Helper()
//}

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
