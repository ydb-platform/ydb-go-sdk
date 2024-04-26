package trace

import (
	"fmt"
	"reflect"
	"testing"
)

// Stub is a helper function that stubs all functional fields of x with given f.
func Stub(x interface{}, f func(name string, args ...interface{})) {
	(FieldStubber{
		OnCall: f,
	}).Stub(reflect.ValueOf(x))
}

func ClearContext(x interface{}) interface{} {
	p := reflect.ValueOf(x).Index(0)
	t := p.Elem().Type()
	f, has := t.FieldByName("Context")
	if has && f.Type.Kind() == reflect.Interface {
		x := reflect.New(t)
		x.Elem().Set(p.Elem())
		c := x.Elem().FieldByName(f.Name)
		c.Set(reflect.Zero(c.Type()))
		p.Set(x)
	}

	return p.Interface()
}

// FieldStubber contains options of filling all struct functional fields.
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
type FieldStubber struct {
	// OnStub is an optional callback that is called when field getting
	// stubbed.
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	OnStub func(name string)

	// OnCall is an optional callback that will be called for each stubbed
	// field getting called.
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	OnCall func(name string, args ...interface{})
}

// Stub fills in given x struct.
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func (f FieldStubber) Stub(x reflect.Value) {
	var (
		v = x.Elem()
		t = v.Type()
	)
	for i := 0; i < t.NumField(); i++ {
		var (
			fx = v.Field(i)
			ft = fx.Type()
		)
		if ft.Kind() != reflect.Func {
			continue
		}
		name := t.Field(i).Name
		if f.OnStub != nil {
			f.OnStub(name)
		}
		out := []reflect.Value{}
		for i := 0; i < ft.NumOut(); i++ {
			ti := reflect.New(ft.Out(i)).Elem()
			out = append(out, ti)
		}
		fn := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
			if f.OnCall == nil {
				return out
			}
			params := make([]interface{}, len(args))
			for i, arg := range args {
				params[i] = arg.Interface()
			}
			f.OnCall(name, params...)

			return out
		})
		fx.Set(fn)
	}
}

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
