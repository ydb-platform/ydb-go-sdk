package trace_test

import (
	"fmt"
	"reflect"
	"testing"

	conngtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	coordinationgtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/gtrace"
	discoverygtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/gtrace"
	querygtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/gtrace"
	retrygtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/retry/gtrace"
	tablegtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/table/gtrace"
	topicgtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	xsqlgtrace "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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
	OnCall func(name string, args ...any)
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
			params := make([]any, len(args))
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
	testSingleTrace(t, &trace.Table{}, "Table", func(lhs, rhs *trace.Table) *trace.Table {
		return tablegtrace.Compose(lhs, rhs)
	})
}

func TestDriver(t *testing.T) {
	testSingleTrace(t, &trace.Driver{}, "Driver", func(lhs, rhs *trace.Driver) *trace.Driver {
		return conngtrace.Compose(lhs, rhs)
	})
}

func TestRetry(t *testing.T) {
	testSingleTrace(t, &trace.Retry{}, "Retry", func(lhs, rhs *trace.Retry) *trace.Retry {
		return retrygtrace.Compose(lhs, rhs)
	})
}

func TestCoordination(t *testing.T) {
	testSingleTrace(t, &trace.Coordination{}, "Coordination", func(lhs, rhs *trace.Coordination) *trace.Coordination {
		return coordinationgtrace.Compose(lhs, rhs)
	})
}

func TestRatelimiter(t *testing.T) {
	testSingleTrace(t, &trace.Ratelimiter{}, "Ratelimiter", func(lhs, rhs *trace.Ratelimiter) *trace.Ratelimiter {
		return lhs
	})
}

func TestTopic(t *testing.T) {
	testSingleTrace(t, &trace.Topic{}, "Topic", func(lhs, rhs *trace.Topic) *trace.Topic {
		return topicgtrace.Compose(lhs, rhs)
	})
}

func TestDiscovery(t *testing.T) {
	testSingleTrace(t, &trace.Discovery{}, "Discovery", func(lhs, rhs *trace.Discovery) *trace.Discovery {
		return discoverygtrace.Compose(lhs, rhs)
	})
}

func TestDatabaseSQL(t *testing.T) {
	testSingleTrace(t, &trace.DatabaseSQL{}, "DatabaseSQL", func(lhs, rhs *trace.DatabaseSQL) *trace.DatabaseSQL {
		return xsqlgtrace.Compose(lhs, rhs)
	})
}

func testSingleTrace[T any](t *testing.T, x *T, traceName string, compose func(lhs, rhs *T) *T) {
	t.Helper()
	compose(x, new(T))
	a := reflect.New(reflect.TypeFor[T]())
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
		OnCall: func(name string, _ ...any) {
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
		}
	}
}

func callEachFunc(v reflect.Value) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		callFunc(v.Field(i), t.Field(i).Type)
	}
}

func TestQuery(t *testing.T) {
	testSingleTrace(t, &trace.Query{}, "Query", func(lhs, rhs *trace.Query) *trace.Query {
		return querygtrace.Compose(lhs, rhs)
	})
}
