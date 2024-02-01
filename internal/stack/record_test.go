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
	var testCases []struct {
		name string
		act  string
		exp  string
	}

	testCases = append(testCases, getRecordFunctionDepthTestCases()...)
	testCases = append(testCases, getTestStructOptionCombinationsTestCases()...)
	testCases = append(testCases, getTestStructSelectiveOptionsTestCases()...)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.act)
		})
	}
}

func getRecordFunctionDepthTestCases() []struct {
	name string
	act  string
	exp  string
} {
	return []struct {
		name string
		act  string
		exp  string
	}{
		{
			name: "Record Depth 0",
			act:  Record(0),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.getRecordFunctionDepthTestCases(record_test.go:56)",
		},
		{
			name: "Record Depth 1",
			act: func() string {
				return Record(1)
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.getRecordFunctionDepthTestCases(record_test.go:63)",
		},
		{
			name: "Record Depth 2",
			act: func() string {
				return func() string {
					return Record(2)
				}()
			}(),
			exp: "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.getRecordFunctionDepthTestCases(record_test.go:72)",
		},
	}
}

func getTestStructOptionCombinationsTestCases() []struct {
	name string
	act  string
	exp  string
} {
	return []struct {
		name string
		act  string
		exp  string
	}{
		{
			name: "PackageName Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), StructName(false), FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "stack",
		},
		{
			name: "StructName Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "testStruct",
		},
		{
			name: "FunctionName Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "TestFunc",
		},
		{
			name: "Lambda Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FunctionName(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "",
		},
		{
			name: "FunctionName and Lambda Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "TestFunc.func1",
		},
		{
			name: "FileName Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FunctionName(false), Lambda(false), Line(false)}}.TestFunc(),
			exp:  "record_test.go",
		},
		{
			name: "Line Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FunctionName(false), Lambda(false), FileName(false)}}.TestFunc(),
			exp:  "",
		},
		{
			name: "FileName and Line Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FunctionName(false), Lambda(false)}}.TestFunc(),
			exp:  "record_test.go:16",
		},
		{
			name: "All Options Disabled",
			act:  testStruct{depth: 0, opts: []recordOption{PackagePath(false), PackageName(false), StructName(false), FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "",
		},
	}
}

func getTestStructSelectiveOptionsTestCases() []struct {
	name string
	act  string
	exp  string
} {
	return []struct {
		name string
		act  string
		exp  string
	}{
		{
			name: "PackagePath Only",
			act:  testStruct{depth: 0, opts: []recordOption{PackageName(false), StructName(false), FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal",
		},
		{
			name: "PackagePath and PackageName Only",
			act:  testStruct{depth: 0, opts: []recordOption{StructName(false), FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack",
		},
		{
			name: "PackagePath, PackageName and StructName Only",
			act:  testStruct{depth: 0, opts: []recordOption{FunctionName(false), Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct",
		},
		{
			name: "No Lambda, FileName and Line Only",
			act:  testStruct{depth: 0, opts: []recordOption{Lambda(false), FileName(false), Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc",
		},
		{
			name: "No FileName and Line Only",
			act:  testStruct{depth: 0, opts: []recordOption{FileName(false), Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1",
		},
		{
			name: "No Line Only",
			act:  testStruct{depth: 0, opts: []recordOption{Line(false)}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1(record_test.go)",
		},
		{
			name: "All Options",
			act:  testStruct{depth: 0, opts: []recordOption{}}.TestFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.testStruct.TestFunc.func1(record_test.go:16)",
		},
		{
			name: "Pointer Receiver Test",
			act:  (&testStruct{depth: 0, opts: []recordOption{}}).TestPointerFunc(),
			exp:  "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.(*testStruct).TestPointerFunc.func1(record_test.go:22)",
		},
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
