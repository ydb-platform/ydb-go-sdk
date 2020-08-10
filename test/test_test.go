package test

import (
	"context"
	"reflect"
	"testing"
)

func TestConditional(t *testing.T) {
	var called bool
	trace := ConditionalBuildTrace{
		OnSomething: func() {
			called = true
		},
	}
	trace.onSomething()
	if !called {
		t.Fatalf("not called")
	}
}

func TestCompose(t *testing.T) {
	var act []string
	called := func(name, arg string) {
		act = append(act, name+":"+arg)
	}
	for _, test := range []struct {
		name string
		a    Trace
		b    Trace
		exp  []string
	}{
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return nil
				},
			},
			b: Trace{},

			exp: []string{
				"a:start",
			},
		},
		{
			a: Trace{},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return nil
				},
			},
			exp: []string{
				"b:start",
			},
		},
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return nil
				},
			},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return nil
				},
			},
			exp: []string{
				"a:start",
				"b:start",
			},
		},
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return func(s string) {
						called("a", s)
					}
				},
			},
			b: Trace{},

			exp: []string{
				"a:start",
				"a:done",
			},
		},
		{
			a: Trace{},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return func(s string) {
						called("b", s)
					}
				},
			},
			exp: []string{
				"b:start",
				"b:done",
			},
		},
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return func(s string) {
						called("a", s)
					}
				},
			},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return func(s string) {
						called("b", s)
					}
				},
			},
			exp: []string{
				"a:start",
				"b:start",
				"a:done",
				"b:done",
			},
		},
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return func(s string) {
						called("a", s)
					}
				},
			},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return nil
				},
			},
			exp: []string{
				"a:start",
				"b:start",
				"a:done",
			},
		},
		{
			a: Trace{
				OnTest: func(s string) func(string) {
					called("a", s)
					return nil
				},
			},
			b: Trace{
				OnTest: func(s string) func(string) {
					called("b", s)
					return func(s string) {
						called("b", s)
					}
				},
			},
			exp: []string{
				"a:start",
				"b:start",
				"b:done",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(func() {
				act = nil
			})
			c := test.a.Compose(test.b)
			done := c.onTest(context.Background(), "start")
			done("done")

			if exp := test.exp; !reflect.DeepEqual(act, exp) {
				t.Fatalf("unexpected calls: %v; want %v", act, exp)
			}
		})
	}
}
