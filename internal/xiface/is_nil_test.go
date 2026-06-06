package xiface

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type testSession struct {
	id string
}

func (s *testSession) ID() string     { return s.id }
func (s *testSession) NodeID() uint32 { return 0 }
func (s *testSession) Status() string { return "" }

type testCall string

func (c testCall) String() string { return string(c) }

func TestIsNil(t *testing.T) {
	var (
		nilSession trace.SessionInfo
		typedNil   *testSession
		session    trace.SessionInfo = &testSession{id: "s1"}
		nilCall    trace.Call
		typedCall  *testCall
		call       trace.Call = testCall("fn")
	)

	for _, tt := range []struct {
		name string
		v    any
		want bool
	}{
		{name: "untyped nil", v: nil, want: true},
		{name: "nil SessionInfo", v: nilSession, want: true},
		{name: "typed nil SessionInfo", v: trace.SessionInfo(typedNil), want: true},
		{name: "valid SessionInfo", v: session, want: false},
		{name: "nil Call", v: nilCall, want: true},
		{name: "typed nil Call", v: trace.Call(typedCall), want: true},
		{name: "valid Call value", v: call, want: false},
		{name: "string value in Stringer", v: fmt.Stringer(testCall("x")), want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNil(tt.v); got != tt.want {
				t.Fatalf("IsNil() = %v, want %v", got, tt.want)
			}
		})
	}
}

func isNilReflect(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func BenchmarkIsNilUnsafe(b *testing.B) {
	var typedNil *testSession
	v := trace.SessionInfo(typedNil)
	for b.Loop() {
		_ = IsNil(v)
	}
}

func BenchmarkIsNilReflect(b *testing.B) {
	var typedNil *testSession
	v := trace.SessionInfo(typedNil)
	for b.Loop() {
		_ = isNilReflect(v)
	}
}

//nolint:staticcheck // benchmark baseline: interface == nil is false for typed nil
func BenchmarkIsNilEqOnly(b *testing.B) {
	var typedNil *testSession
	v := trace.SessionInfo(typedNil)
	for b.Loop() {
		_ = v == nil
	}
}
