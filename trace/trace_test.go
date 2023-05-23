package trace

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTable(t *testing.T) {
	testSingleTrace(t, &Table{}, "Table")
}

func TestDriver(t *testing.T) {
	testSingleTrace(t, &Driver{}, "Driver")
}

func TestRetry(t *testing.T) {
	testSingleTrace(t, &Retry{}, "Retry")
}

func TestCoordination(t *testing.T) {
	testSingleTrace(t, &Table{}, "Coordination")
}

func TestRatelimiter(t *testing.T) {
	testSingleTrace(t, &Ratelimiter{}, "Ratelimiter")
}

func TestTopic(t *testing.T) {
	testSingleTrace(t, &Topic{}, "Topic")
}

func TestDiscovery(t *testing.T) {
	testSingleTrace(t, &Discovery{}, "Discovery")
}

func TestDatabaseSQL(t *testing.T) {
	testSingleTrace(t, &DatabaseSQL{}, "DatabaseSQL")
}

func testSingleTrace(t *testing.T, x interface{}, traceName string) {
	t.Helper()
	v := reflect.ValueOf(x)
	m := v.MethodByName("Compose")
	m.Call(
		[]reflect.Value{reflect.New(reflect.ValueOf(x).Elem().Type())},
	)
	a := reflect.New(reflect.TypeOf(x).Elem())
	defer assertCalled(t, traceName, stubEachFunc(a))
	callEachFunc(a.Elem())
}

func assertCalled(t *testing.T, prefix string, called map[string]bool) {
	t.Helper()
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

func callFunc(f reflect.Value, ft reflect.Type) {
	if ft.Kind() != reflect.Func {
		return
	}
	if f.IsNil() {
		return
	}
	args := make([]reflect.Value, ft.NumIn())
	for j := range args {
		args[j] = reflect.New(ft.In(j)).Elem()
	}
	for _, xx := range f.Call(args) {
		switch xx.Type().Kind() {
		case reflect.Struct:
			callEachFunc(xx)
		case reflect.Func:
			callFunc(xx, xx.Type())
		default:
			_ = xx
		}
	}
}

func callEachFunc(x reflect.Value) {
	t := x.Type()
	for i := 0; i < t.NumField(); i++ {
		var (
			f  = x.Field(i)
			ft = f.Type()
		)
		callFunc(f, ft)
	}
}
