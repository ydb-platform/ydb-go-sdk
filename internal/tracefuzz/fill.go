package tracefuzz

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type contextKey struct{}

// Fill sets v to a fuzz-generated value.
func Fill(f *Fuzzer, v reflect.Value) {
	if !v.IsValid() {
		return
	}

	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(f.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(f.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(f.uint64())
	case reflect.Float32, reflect.Float64:
		v.SetFloat(float64(f.Int()) / 1000)
	case reflect.String:
		v.SetString(f.String())
	case reflect.Interface:
		fillInterface(f, v)
	case reflect.Ptr:
		fillPointer(f, v)
	case reflect.Slice:
		fillSlice(f, v)
	case reflect.Array:
		fillArray(f, v)
	case reflect.Struct:
		fillStruct(f, v)
	case reflect.Map:
		// trace structs do not contain maps; leave zero value.
	case reflect.Func:
		// trace info structs do not contain func fields.
	case reflect.Chan:
		// not used in trace info structs.
	default:
	}
}

func fillStruct(f *Fuzzer, v reflect.Value) {
	t := v.Type()
	if t == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(time.Unix(int64(f.Int()%1_000_000), 0)))

		return
	}
	if t == reflect.TypeFor[time.Duration]() {
		v.Set(reflect.ValueOf(time.Duration(f.Int())))

		return
	}
	if t == reflect.TypeFor[trace.Method]() {
		v.Set(reflect.ValueOf(trace.Method(f.String())))

		return
	}
	if t == reflect.TypeFor[trace.Details]() {
		v.Set(reflect.ValueOf(trace.Details(f.uint64())))

		return
	}
	if t == reflect.TypeFor[context.Context]() {
		ctx := context.Background()
		if f.Bool() {
			ctx = context.WithValue(context.Background(), contextKey{}, f.String())
		}
		v.Set(reflect.ValueOf(ctx))

		return
	}

	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).CanSet() {
			continue
		}
		Fill(f, v.Field(i))
	}
}

func fillPointer(f *Fuzzer, v reflect.Value) {
	switch f.Choice(4) {
	case 0:
		v.SetZero()
	case 1:
		fillSpecialPointer(f, v)
	default:
		v.Set(reflect.New(v.Type().Elem()))
	}
}

func fillSpecialPointer(f *Fuzzer, v reflect.Value) {
	elemType := v.Type().Elem()
	switch elemType {
	case reflect.TypeFor[context.Context]():
		ctx := context.Background()
		if f.Bool() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		v.Set(reflect.ValueOf(&ctx))
	case reflect.TypeFor[Ydb_TableStats.QueryStats]():
		v.Set(reflect.ValueOf(&Ydb_TableStats.QueryStats{}))
	default:
		v.Set(reflect.New(elemType))
	}
}

func fillSlice(f *Fuzzer, v reflect.Value) {
	n := f.Intn(4)
	slice := reflect.MakeSlice(v.Type(), n, n)
	for i := range n {
		Fill(f, slice.Index(i))
	}
	v.Set(slice)
}

func fillArray(f *Fuzzer, v reflect.Value) {
	for i := 0; i < v.Len(); i++ {
		Fill(f, v.Index(i))
	}
}

func fillInterface(f *Fuzzer, v reflect.Value) {
	if v.Type() == reflect.TypeFor[error]() {
		err := newFuzzError(f)
		if err == nil {
			v.SetZero()

			return
		}
		v.Set(reflect.ValueOf(err))

		return
	}

	switch f.Choice(4) {
	case 0:
		v.SetZero()
	case 1:
		if val, ok := typedNil(v.Type()); ok {
			v.Set(val)

			return
		}
		v.SetZero()
	default:
		if val, ok := concreteInterface(f, v.Type()); ok {
			v.Set(val)

			return
		}
		v.SetZero()
	}
}

func concreteInterface(f *Fuzzer, t reflect.Type) (reflect.Value, bool) {
	s := f.String()
	switch t {
	case reflect.TypeFor[trace.SessionInfo]():
		return reflect.ValueOf(&fuzzSession{id: s, nodeID: uint32(f.Int()), status: s}), true
	case reflect.TypeFor[trace.TxInfo]():
		return reflect.ValueOf(&fuzzTx{id: s}), true
	case reflect.TypeFor[trace.EndpointInfo]():
		return reflect.ValueOf(&fuzzEndpoint{
			nodeID:     uint32(f.Int()),
			address:    s,
			location:   s,
			loadFactor: float32(f.Int()) / 100,
			updated:    time.Unix(int64(f.Int()%1_000_000), 0),
			localDC:    f.Bool(),
		}), true
	case reflect.TypeFor[trace.ConnState]():
		return reflect.ValueOf(&fuzzConnState{code: f.Int(), valid: f.Bool(), text: s}), true
	case reflect.TypeFor[trace.Call]():
		return reflect.ValueOf(fuzzCall(s)), true
	case reflect.TypeFor[trace.TableQueryParameters]():
		return reflect.ValueOf(fuzzTableQueryParameters(s)), true
	case reflect.TypeFor[trace.TableDataQuery]():
		return reflect.ValueOf(&fuzzTableDataQuery{id: s, yql: s}), true
	case reflect.TypeFor[trace.TableResultErr]():
		return reflect.ValueOf(&fuzzTableResultErr{err: newFuzzError(f)}), true
	case reflect.TypeFor[trace.TableResult]():
		return reflect.ValueOf(&fuzzTableResult{
			fuzzTableResultErr: fuzzTableResultErr{err: newFuzzError(f)},
			count:              f.Intn(10),
		}), true
	case reflect.TypeFor[trace.ScriptingQueryParameters]():
		return reflect.ValueOf(fuzzScriptingQueryParameters(s)), true
	case reflect.TypeFor[trace.ScriptingResultErr]():
		return reflect.ValueOf(&fuzzScriptingResultErr{err: newFuzzError(f)}), true
	case reflect.TypeFor[trace.ScriptingResult]():
		return reflect.ValueOf(&fuzzScriptingResult{
			fuzzScriptingResultErr: fuzzScriptingResultErr{err: newFuzzError(f)},
			count:                  f.Intn(10),
		}), true
	case reflect.TypeFor[trace.Issue]():
		return reflect.ValueOf(&fuzzIssue{message: s, code: uint32(f.Int()), severity: uint32(f.Int())}), true
	default:
		if t.Implements(reflect.TypeFor[fmt.Stringer]()) {
			return reflect.ValueOf(fuzzStringer(s)), true
		}

		return reflect.Value{}, false
	}
}

// InterfaceValue returns a fuzz-generated value for the given interface type.
// It is exported for packages that need topic-specific interfaces.
func InterfaceValue(f *Fuzzer, t reflect.Type) reflect.Value {
	v := reflect.New(t).Elem()
	fillInterface(f, v)

	return v
}
