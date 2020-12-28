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

func TestBuildTagTrace(t *testing.T) {
	t0 := BuildTagTrace{
		OnSomethingA: func() func() {
			panic("must not be called")
		},
		OnSomethingB: func(int8, int16) func(int32, int64) {
			panic("must not be called")
		},
		OnSomethingC: func(Type) func(Type) {
			panic("must not be called")
		},
	}
	t1 := BuildTagTrace{
		OnSomethingA: func() func() {
			panic("must not be called")
		},
		OnSomethingB: func(int8, int16) func(int32, int64) {
			panic("must not be called")
		},
		OnSomethingC: func(Type) func(Type) {
			panic("must not be called")
		},
	}
	trace := t0.Compose(t1)
	trace.onSomethingA(context.Background())()
	trace.onSomethingB(context.Background(), 1, 2)(3, 4)
	trace.onSomethingC(context.Background(), Type{})(Type{})
}

func BenchmarkBuildTagTrace(b *testing.B) {
	x := BuildTagTrace{
		OnSomethingA: func() func() {
			return func() {
				//
			}
		},
		OnSomethingB: func(int8, int16) func(int32, int64) {
			return func(int32, int64) {
				//
			}
		},
		OnSomethingC: func(Type) func(Type) {
			return func(Type) {
				//
			}
		},
	}

	t := x.Compose(x).Compose(x).Compose(x)

	b.Run("OnSomethingA", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t.onSomethingA(context.Background())()
		}
	})
	b.Run("OnSomethingB", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t.onSomethingB(context.Background(), 1, 2)(3, 4)
		}
	})
	b.Run("OnSomethingC", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t.onSomethingC(context.Background(), Type{})(Type{})
		}
	})
}
