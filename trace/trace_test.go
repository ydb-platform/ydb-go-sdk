package trace

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTable(t *testing.T) {
	testSingleTrace(t, Table{}, "Table")
}

func TestTraceDriver(t *testing.T) {
	testSingleTrace(t, Driver{}, "Driver")
}

func TestRetry(t *testing.T) {
	testSingleTrace(t, Retry{}, "Driver")
}

func testSingleTrace(t *testing.T, x interface{}, traceName string) {
	a := reflect.New(reflect.TypeOf(x))
	defer assertCalled(t, traceName, stubEachFunc(a))
	callEachFunc(a.Elem())
}

func assertCalled(t *testing.T, prefix string, called map[string]bool) {
	for name, called := range called {
		if !called {
			t.Error(prefix, fmt.Sprintf("%s field is not called", name))
		}
	}
}

func stubEachFunc(x reflect.Value) map[string]bool {
	fs := make(map[string]bool)
	(FieldStubber{
		OnStub: func(name string) {
			fs[name] = false
		},
		OnCall: func(name string, _ ...interface{}) {
			fs[name] = true
		},
	}).Stub(x)
	return fs
}

func callEachFunc(x reflect.Value) {
	var (
		t = x.Type()
	)
	for i := 0; i < t.NumField(); i++ {
		var (
			f  = x.Field(i)
			ft = f.Type()
		)
		if ft.Kind() != reflect.Func {
			continue
		}
		if f.IsNil() {
			continue
		}
		args := make([]reflect.Value, ft.NumIn())
		for i := range args {
			args[i] = reflect.New(ft.In(i)).Elem()
		}
		f.Call(args)
	}
}
