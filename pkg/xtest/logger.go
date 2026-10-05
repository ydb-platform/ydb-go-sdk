package xtest

import (
	"sync"
	"testing"
)

// MakeSyncedTest wraps t for compatibility with existing tests.
//
// Deprecated: Use [testing.T] directly.
func MakeSyncedTest(t *testing.T) *SyncedTest {
	return &SyncedTest{
		T: t,
	}
}

// SyncedTest wraps [testing.T] with additional synchronization.
//
// Deprecated: Use [testing.T] directly.
type SyncedTest struct {
	*testing.T

	m sync.Mutex
}

// Cleanup delegates to [testing.T.Cleanup].
//
// Deprecated: Use [testing.T.Cleanup] directly.
func (s *SyncedTest) Cleanup(f func()) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Cleanup(f)
}

// Error delegates to [testing.T.Error].
//
// Deprecated: Use [testing.T.Error] directly.
func (s *SyncedTest) Error(args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Error(args...)
}

// Errorf delegates to [testing.T.Errorf].
//
// Deprecated: Use [testing.T.Errorf] directly.
func (s *SyncedTest) Errorf(format string, args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Errorf(format, args...)
}

// Fail delegates to [testing.T.Fail].
//
// Deprecated: Use [testing.T.Fail] directly.
func (s *SyncedTest) Fail() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Fail()
}

// FailNow delegates to [testing.T.FailNow].
//
// Deprecated: Use [testing.T.FailNow] directly.
func (s *SyncedTest) FailNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.FailNow()
}

// Failed delegates to [testing.T.Failed].
//
// Deprecated: Use [testing.T.Failed] directly.
func (s *SyncedTest) Failed() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Failed()
}

// Fatal delegates to [testing.T.Fatal].
//
// Deprecated: Use [testing.T.Fatal] directly.
func (s *SyncedTest) Fatal(args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Fatal(args...)
}

// Fatalf delegates to [testing.T.Fatalf].
//
// Deprecated: Use [testing.T.Fatalf] directly.
func (s *SyncedTest) Fatalf(format string, args ...any) {
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

// Log delegates to [testing.T.Log].
//
// Deprecated: Use [testing.T.Log] directly.
func (s *SyncedTest) Log(args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Log(args...)
}

// Logf delegates to [testing.T.Logf].
//
// Deprecated: Use [testing.T.Logf] directly.
func (s *SyncedTest) Logf(format string, args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Logf(format, args...)
}

// Name delegates to [testing.T.Name].
//
// Deprecated: Use [testing.T.Name] directly.
func (s *SyncedTest) Name() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Name()
}

// Run delegates to [testing.T.Run].
//
// Deprecated: Use [testing.T.Run] directly.
func (s *SyncedTest) Run(name string, f func(t *testing.T)) bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Run(name, f)
}

// RunSynced runs f as a subtest using a SyncedTest wrapper.
//
// Deprecated: Use [testing.T.Run] directly.
func (s *SyncedTest) RunSynced(name string, f func(t *SyncedTest)) bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Run(name, func(t *testing.T) {
		syncedTest := MakeSyncedTest(t)
		f(syncedTest)
	})
}

// Setenv retains its legacy panic.
//
// Deprecated: Use [testing.T.Setenv] directly.
func (s *SyncedTest) Setenv(key, value string) {
	panic("not implemented")
}

// Skip delegates to [testing.T.Skip].
//
// Deprecated: Use [testing.T.Skip] directly.
func (s *SyncedTest) Skip(args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.Skip(args...)
}

// SkipNow delegates to [testing.T.SkipNow].
//
// Deprecated: Use [testing.T.SkipNow] directly.
func (s *SyncedTest) SkipNow() {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	s.T.SkipNow()
}

// Skipf delegates to [testing.T.Skipf].
//
// Deprecated: Use [testing.T.Skipf] directly.
func (s *SyncedTest) Skipf(format string, args ...any) {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()
	s.T.Skipf(format, args...)
}

// Skipped delegates to [testing.T.Skipped].
//
// Deprecated: Use [testing.T.Skipped] directly.
func (s *SyncedTest) Skipped() bool {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.Skipped()
}

// TempDir delegates to [testing.T.TempDir].
//
// Deprecated: Use [testing.T.TempDir] directly.
func (s *SyncedTest) TempDir() string {
	s.m.Lock()
	defer s.m.Unlock()
	s.T.Helper()

	return s.T.TempDir()
}
