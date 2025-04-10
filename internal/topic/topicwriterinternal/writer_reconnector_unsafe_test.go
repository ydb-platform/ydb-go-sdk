package topicwriterinternal

import (
	"reflect"
	"runtime"
	"unsafe"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// getWaitersCount returns number of goroutines waiting for semaphore acquisition
// by checking overflow state. This is unsafe function which uses reflection
// and unsafe pointers to access internal fields. It should be used only in tests.
func getWaitersCount(sem *xsync.SoftWeightedSemaphore) int64 {
	// Prevent garbage collection of the semaphore while we work with its fields
	defer runtime.KeepAlive(sem)

	// Get access to overflow field through reflection
	semVal := reflect.ValueOf(sem).Elem()
	overflowField := semVal.FieldByName("overflow")
	overflowAddr := unsafe.Pointer(overflowField.UnsafeAddr())
	overflow := (*int64)(overflowAddr)

	return *overflow
}
