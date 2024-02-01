package topicwriterinternal

import (
	"container/list"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sync/semaphore"
)

func getWaitersCount(sem *semaphore.Weighted) int {
	defer runtime.KeepAlive(sem)

	semVal := reflect.ValueOf(sem).Elem()
	mutexField := semVal.FieldByName("mu")

	mutexAddr := unsafe.Pointer(mutexField.UnsafeAddr())
	mutex := (*sync.Mutex)(mutexAddr)
	mutex.Lock()
	defer mutex.Unlock()

	waitersField := semVal.FieldByName("waiters")
	waitersPointer := unsafe.Pointer(waitersField.UnsafeAddr())
	waiters := (*list.List)(waitersPointer)

	return waiters.Len()
}
