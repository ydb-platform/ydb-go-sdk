package xtest

import (
	"sync"
	"testing"
)

func MakeSyncedTest(t testing.TB) *SyncedTest {
	return &SyncedTest{
		TB: t,
	}
}

type SyncedTest struct {
	m sync.Mutex
	testing.TB
}

func (s *SyncedTest) Cleanup(f func()) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Cleanup(f)
}

func (s *SyncedTest) Error(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Error(args...)
}

func (s *SyncedTest) Errorf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Errorf(format, args...)
}

func (s *SyncedTest) Fail() {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Fail()
}

func (s *SyncedTest) FailNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.FailNow()
}

func (s *SyncedTest) Failed() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	return s.TB.Failed()
}

func (s *SyncedTest) Fatal(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Fatal(args...)
}

func (s *SyncedTest) Fatalf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Fatalf(format, args...)
}

// must direct called
// func (s *SyncedTest) Helper() {
//	s.m.Lock()
//	defer s.m.Unlock()
//	s.TB.Helper()
//}

func (s *SyncedTest) Log(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Log(args...)
}

func (s *SyncedTest) Logf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Logf(format, args...)
}

func (s *SyncedTest) Name() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	return s.TB.Name()
}

func (s *SyncedTest) Setenv(key, value string) {
	panic("not implemented")
}

func (s *SyncedTest) Skip(args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.Skip(args...)
}

func (s *SyncedTest) SkipNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	s.TB.SkipNow()
}

func (s *SyncedTest) Skipf(format string, args ...interface{}) {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()
	s.TB.Skipf(format, args...)
}

func (s *SyncedTest) Skipped() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	return s.TB.Skipped()
}

func (s *SyncedTest) TempDir() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.TB.Helper()

	return s.TB.TempDir()
}
