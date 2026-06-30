package main_test

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

// fieldStubber fills all func fields of a trace struct with stubs.
type fieldStubber struct {
	onStub func(name string)
	onCall func(name string, args ...any)
}

func (f fieldStubber) stub(x reflect.Value) {
	v := x.Elem()
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		fx := v.Field(i)
		ft := fx.Type()
		if ft.Kind() != reflect.Func {
			continue
		}
		name := t.Field(i).Name
		if f.onStub != nil {
			f.onStub(name)
		}
		out := make([]reflect.Value, ft.NumOut())
		for i := 0; i < ft.NumOut(); i++ {
			out[i] = reflect.New(ft.Out(i)).Elem()
		}
		fn := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
			if f.onCall != nil {
				params := make([]any, len(args))
				for i, arg := range args {
					params[i] = arg.Interface()
				}
				f.onCall(name, params...)
			}

			return out
		})
		fx.Set(fn)
	}
}

// testSingleTrace checks that every hook field on a trace struct can be invoked
// (including nested done-callbacks) without panicking.
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
	fieldStubber{
		onStub: func(name string) {
			fs[name] = false
		},
		onCall: func(name string, _ ...any) {
			fs[name] = true
		},
	}.stub(x)

	return fs
}

func callFunc(f reflect.Value, ft reflect.Type) {
	if ft.Kind() != reflect.Func || f.IsNil() {
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

func TestTableTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Table{}, "Table", func(lhs, rhs *trace.Table) *trace.Table {
		return tablegtrace.Compose(lhs, rhs)
	})
}

func TestDriverTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Driver{}, "Driver", func(lhs, rhs *trace.Driver) *trace.Driver {
		return conngtrace.Compose(lhs, rhs)
	})
}

func TestRetryTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Retry{}, "Retry", func(lhs, rhs *trace.Retry) *trace.Retry {
		return retrygtrace.Compose(lhs, rhs)
	})
}

func TestCoordinationTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Coordination{}, "Coordination", func(lhs, rhs *trace.Coordination) *trace.Coordination {
		return coordinationgtrace.Compose(lhs, rhs)
	})
}

func TestRatelimiterTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Ratelimiter{}, "Ratelimiter", func(lhs, _ *trace.Ratelimiter) *trace.Ratelimiter {
		return lhs
	})
}

func TestTopicTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Topic{}, "Topic", func(lhs, rhs *trace.Topic) *trace.Topic {
		return topicgtrace.Compose(lhs, rhs)
	})
}

func TestDiscoveryTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Discovery{}, "Discovery", func(lhs, rhs *trace.Discovery) *trace.Discovery {
		return discoverygtrace.Compose(lhs, rhs)
	})
}

func TestDatabaseSQLTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.DatabaseSQL{}, "DatabaseSQL", func(lhs, rhs *trace.DatabaseSQL) *trace.DatabaseSQL {
		return xsqlgtrace.Compose(lhs, rhs)
	})
}

func TestQueryTraceHooks(t *testing.T) {
	testSingleTrace(t, &trace.Query{}, "Query", func(lhs, rhs *trace.Query) *trace.Query {
		return querygtrace.Compose(lhs, rhs)
	})
}
