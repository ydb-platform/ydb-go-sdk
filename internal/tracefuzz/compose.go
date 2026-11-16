package tracefuzz

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	lhsSide   = "lhs"
	rhsSide   = "rhs"
	hookPanic = "hook panic"
)

// TestCompose checks the composition contract for every hook and callback stage.
func TestCompose[T, O any](
	t *testing.T, compose func(*T, *T, ...O) *T, withPanic func(func(any)) O, deprecated ...string,
) {
	t.Helper()

	rhs := new(T)
	require.Same(t, rhs, compose(nil, rhs))
	require.Nil(t, compose(nil, nil))

	traceType := reflect.TypeFor[T]()
	for i := 0; i < traceType.NumField(); i++ {
		field := traceType.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			if slices.Contains(deprecated, field.Name) {
				lhs, rhs := new(T), new(T)
				panicHook := reflect.MakeFunc(field.Type, func([]reflect.Value) []reflect.Value {
					t.Fatal("deprecated hook called")

					return nil
				})
				reflect.ValueOf(lhs).Elem().Field(i).Set(panicHook)
				reflect.ValueOf(rhs).Elem().Field(i).Set(panicHook)
				require.True(t, reflect.ValueOf(compose(lhs, rhs)).Elem().Field(i).IsNil())

				return
			}
			testComposeCallbacks(t, compose, i, field)
			testComposePanics(t, compose, withPanic, i, field)
		})
	}
}

func testComposeCallbacks[T, O any](t *testing.T, compose func(*T, *T, ...O) *T, i int, field reflect.StructField) {
	t.Helper()
	for _, sides := range [][]string{nil, {lhsSide}, {rhsSide}, {lhsSide, rhsSide}} {
		t.Run("Hooks/"+strings.Join(sides, "+"), func(t *testing.T) {
			r := &hookRecorder{t: t, sides: sides}
			lhs, rhs := new(T), new(T)
			for _, side := range sides {
				tr := lhs
				if side == rhsSide {
					tr = rhs
				}
				reflect.ValueOf(tr).Elem().Field(i).Set(r.callback(field.Type, side, 0))
			}
			var option O
			composed := compose(lhs, rhs, option)
			r.invoke(reflect.ValueOf(composed).Elem().Field(i), 0)
		})
	}

	t.Run("NilReturnedCallbacks", func(t *testing.T) {
		r := &hookRecorder{t: t, sides: []string{lhsSide, rhsSide}, nilReturns: true}
		lhs, rhs := new(T), new(T)
		reflect.ValueOf(lhs).Elem().Field(i).Set(r.callback(field.Type, lhsSide, 0))
		reflect.ValueOf(rhs).Elem().Field(i).Set(r.callback(field.Type, rhsSide, 0))
		r.invoke(reflect.ValueOf(compose(lhs, rhs)).Elem().Field(i), 0)
	})
}

func testComposePanics[T, O any](
	t *testing.T, compose func(*T, *T, ...O) *T, withPanic func(func(any)) O, i int, field reflect.StructField,
) {
	t.Helper()
	for depth, ft := 0, field.Type; ft != nil; depth, ft = depth+1, callbackResult(ft) {
		for _, side := range []string{lhsSide, rhsSide} {
			t.Run("Panic/"+side+"/"+strconv.Itoa(depth), func(t *testing.T) {
				for _, recoverPanic := range []bool{false, true} {
					r := &hookRecorder{
						t: t, sides: []string{lhsSide, rhsSide}, panicSide: side, panicDepth: depth,
					}
					lhs, rhs := new(T), new(T)
					reflect.ValueOf(lhs).Elem().Field(i).Set(r.callback(field.Type, lhsSide, 0))
					reflect.ValueOf(rhs).Elem().Field(i).Set(r.callback(field.Type, rhsSide, 0))
					var options []O
					var panics []any
					if recoverPanic {
						options = append(options, withPanic(func(value any) {
							panics = append(panics, value)
						}))
					}
					fn := reflect.ValueOf(compose(lhs, rhs, options...)).Elem().Field(i)
					invoke := func() { r.invoke(fn, 0) }
					if recoverPanic {
						require.NotPanics(t, invoke)
						require.Equal(t, []any{hookPanic}, panics)
					} else {
						require.PanicsWithValue(t, hookPanic, invoke)
					}
				}
			})
		}
	}
}

type hookRecorder struct {
	t          *testing.T
	sides      []string
	calls      []string
	args       []reflect.Value
	nilReturns bool
	panicSide  string
	panicDepth int
}

func (r *hookRecorder) callback(ft reflect.Type, side string, depth int) reflect.Value {
	return reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
		r.calls = append(r.calls, side)
		require.Len(r.t, args, len(r.args))
		for i := range args {
			require.Equal(r.t, r.args[i].Interface(), args[i].Interface())
		}
		if side == r.panicSide && depth == r.panicDepth {
			panic(hookPanic)
		}
		var results []reflect.Value
		if rt := callbackResult(ft); rt != nil {
			result := reflect.Zero(rt)
			if !r.nilReturns {
				result = r.callback(rt, side, depth+1)
			}
			results = append(results, result)
		}

		return results
	})
}

func (r *hookRecorder) invoke(fn reflect.Value, depth int) {
	if fn.IsNil() {
		return
	}
	r.args = make([]reflect.Value, fn.Type().NumIn())
	f := New([]byte("composed-hook-arguments"))
	for i := range r.args {
		r.args[i] = reflect.New(fn.Type().In(i)).Elem()
		Fill(f, r.args[i])
	}
	r.calls = nil
	results := fn.Call(r.args)
	want := r.sides
	if r.nilReturns && depth > 0 {
		want = nil
	}
	if r.panicSide == lhsSide && depth == r.panicDepth {
		want = []string{lhsSide}
	}
	require.Equal(r.t, want, r.calls)
	for _, result := range results {
		r.invoke(result, depth+1)
	}
}

func callbackResult(ft reflect.Type) reflect.Type {
	if ft.NumOut() == 0 {
		return nil
	}

	return ft.Out(0)
}
