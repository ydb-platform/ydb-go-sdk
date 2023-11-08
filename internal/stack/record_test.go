package stack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testStruct struct {
	depth int
	opts  []recordOption
}

func (s testStruct) TestFunc() string {
	return func() string {
		return Record(s.depth, s.opts...)
	}()
}

func (s *testStruct) TestPointerFunc() string {
	return func() string {
		return Record(s.depth, s.opts...)
	}()
}

func TestRecord(t *testing.T) {
	for _, tt := range []struct {
		act string
		exp string
	}{
		{
			act: Record(0),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:32)",
		},
		{
			act: func() string {
				return Record(1)
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:38)",
		},
		{
			act: func() string {
				return func() string {
					return Record(2)
				}()
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:46)",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				// PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "stack",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				// StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "testStruct",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				// FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "TestFunc",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				// Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				// FunctionName(false),
				// Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "TestFunc.func1",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				// FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "record_test.go",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				// Line(false),
			}}.TestFunc(),
			exp: "",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				PackagePath(false),
				PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				// FileName(false),
				// Line(false),
			}}.TestFunc(),
			exp: "record_test.go:16",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				// FunctionName(false),
				Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				// FunctionName(false),
				// Lambda(false),
				FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				// FunctionName(false),
				// Lambda(false),
				// FileName(false),
				Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1(record_test.go)",
		},
		{
			act: testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				// FunctionName(false),
				// Lambda(false),
				// FileName(false),
				// Line(false),
			}}.TestFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1(record_test.go:16)",
		},
		{
			act: (&testStruct{depth: 0, opts: []recordOption{
				// PackagePath(false),
				// PackageName(false),
				// StructName(false),
				// FunctionName(false),
				// Lambda(false),
				// FileName(false),
				// Line(false),
			}}).TestPointerFunc(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.(*testStruct).TestPointerFunc.func1(record_test.go:22)",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, tt.act)
		})
	}
}

func BenchmarkCall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Call(0)
	}
}

func BenchmarkRecord(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Record(0)
	}
}

func BenchmarkCallRecord(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Call(0).Record()
	}
}

func BenchmarkCallFuncionID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Call(0).FunctionID()
	}
}
