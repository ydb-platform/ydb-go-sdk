package stack

import (
	"reflect"
	"runtime"
	"strings"
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
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:35)",
		},
		{
			act: func() string {
				return Record(1)
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:41)",
		},
		{
			act: func() string {
				return func() string {
					return Record(2)
				}()
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestRecord(record_test.go:49)",
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
			exp: "record_test.go:19",
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
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1(record_test.go:19)",
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
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.(*testStruct).TestPointerFunc.func1(record_test.go:25)",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.exp, tt.act)
		})
	}
}

func TestExtractNames(t *testing.T) {
	testFunc := func() {}
	funcPtr := reflect.ValueOf(testFunc).Pointer()

	funcNameExpected := runtime.FuncForPC(funcPtr).Name()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller should return true indicating success")

	fileParts := strings.Split(file, "/")
	fileNameExpected := fileParts[len(fileParts)-1]

	name, fileName := extractName(funcPtr, file)

	require.Equal(t, funcNameExpected, name, "Function name should match expected value")
	require.Equal(t, fileNameExpected, fileName, "File name should match expected value")
}

func TestParseFunctionName(t *testing.T) {
	name := "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.TestParseFunctionName.func1"
	fnDetails := parseFunctionName(name)

	require.Equal(t, "github.com/ydb-platform/ydb-go-sdk/v3/internal", fnDetails.pkgPath)
	require.Equal(t, "stack", fnDetails.pkgName)
	require.Empty(t, fnDetails.structName, "Struct name should be empty for standalone functions")
	require.Equal(t, "TestParseFunctionName", fnDetails.funcName)
	require.Contains(t, fnDetails.lambdas, "func1", "Lambdas should include 'func1'")
}

func TestBuildRecordString(t *testing.T) {
	optionsHolder := recordOptions{
		packagePath:  true,
		packageName:  false,
		structName:   true,
		functionName: true,
		fileName:     true,
		line:         true,
		lambdas:      true,
	}
	fnDetails := functionDetails{
		pkgPath:    "github.com/ydb-platform/ydb-go-sdk/v3/internal",
		pkgName:    "",
		structName: "testStruct",
		funcName:   "TestFunc",

		lambdas: []string{"func1"},
	}
	file := "record_test.go"
	line := 319

	result := buildRecordString(optionsHolder, &fnDetails, file, line)
	expected := "github.com/ydb-platform/ydb-go-sdk/v3/internal.testStruct.TestFunc.func1(record_test.go:319)"
	require.Equal(t, expected, result)
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
		_ = Call(0).String()
	}
}
