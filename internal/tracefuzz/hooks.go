package tracefuzz

import (
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	nilHook     = "NilHook"
	nilCallback = "NilCallback"
)

// TestHooks checks metadata forwarding and safe invocation of absent callbacks.
func TestHooks[T any](t *testing.T, wrappers ...any) {
	t.Helper()

	for _, wrapper := range wrappers {
		fn := reflect.ValueOf(wrapper)
		name := runtime.FuncForPC(fn.Pointer()).Name()
		name = name[strings.LastIndex(name, ".")+1:]
		hook := name[strings.Index(name, "On"):]
		t.Run(hook, func(t *testing.T) {
			for _, mode := range []string{nilHook, nilCallback, "Callbacks"} {
				t.Run(mode, func(t *testing.T) {
					tr := new(T)
					r := &wrapperRecorder{t: t, nilReturns: mode == nilCallback}
					field := reflect.ValueOf(tr).Elem().FieldByName(hook)
					require.True(t, field.IsValid())
					if mode != nilHook {
						field.Set(r.callback(field.Type()))
					}
					r.invoke(fn, reflect.ValueOf(tr), mode != nilHook)
				})
			}
		})
	}
}

type wrapperRecorder struct {
	t          *testing.T
	args       []any
	calls      int
	nilReturns bool
}

func (r *wrapperRecorder) callback(ft reflect.Type) reflect.Value {
	return reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		r.calls++
		require.Len(r.t, args, 1)
		info := args[0]
		var fields []any
		for i := 0; i < info.NumField(); i++ {
			fields = append(fields, info.Field(i).Interface())
		}
		for _, arg := range r.args {
			index := slices.IndexFunc(fields, func(field any) bool { return reflect.DeepEqual(arg, field) })
			require.NotEqual(r.t, -1, index, "wrapper argument was not forwarded: %v", arg)
			fields = slices.Delete(fields, index, index+1)
		}
		for _, field := range fields {
			require.Zero(r.t, field)
		}
		var results []reflect.Value
		if rt := callbackResult(ft); rt != nil {
			result := reflect.Zero(rt)
			if !r.nilReturns {
				result = r.callback(rt)
			}
			results = append(results, result)
		}

		return results
	})
}

func (r *wrapperRecorder) invoke(fn, tr reflect.Value, expectCall bool) {
	r.args = nil
	args := make([]reflect.Value, fn.Type().NumIn())
	f := New([]byte("wrapper-hook-arguments"))
	for i := range args {
		if i == 0 && tr.IsValid() {
			args[i] = tr

			continue
		}
		args[i] = reflect.New(fn.Type().In(i)).Elem()
		Fill(f, args[i])
		r.args = append(r.args, args[i].Interface())
	}
	r.calls = 0
	results := fn.Call(args)
	if expectCall {
		require.Equal(r.t, 1, r.calls)
	} else {
		require.Zero(r.t, r.calls)
	}
	for _, result := range results {
		require.False(r.t, result.IsNil())
		r.invoke(result, reflect.Value{}, expectCall && !r.nilReturns)
	}
}
