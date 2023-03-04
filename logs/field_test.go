package logs

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stringerTest struct{}

func (st *stringerTest) String() string {
	return "stringerTest"
}

func TestField_String(t *testing.T) {

	type test struct {
		name  string
		f     Field
		want  string
		panic bool
		fail  bool
	}

	types := make(map[FieldType]bool)

	tests := []test{}

	for i := 0; i < int(EndType); i++ {

		ftype := FieldType(i)
		name := ftype.String()

		types[ftype] = false

		switch ftype {
		case IntType:
			tests = append(tests, test{name: name, f: Int(name, 1), want: "1"})
		case Int64Type:
			tests = append(tests, test{name: name, f: Int64(name, 9223372036854775807), want: "9223372036854775807"})
		case StringType:
			tests = append(tests, test{name: name, f: String(name, name), want: name})
		case BoolType:
			tests = append(tests, test{name: name, f: Bool(name, true), want: "true"})
		case DurationType:
			tests = append(tests, test{name: name, f: Duration(name, time.Hour), want: time.Hour.String()})
		case StringsType:
			tests = append(tests, test{name: name, f: Strings(name, []string{"Abc", "Def", "Ghi"}), want: "[Abc Def Ghi]"})
		case ErrorType:
			tests = append(tests, test{name: name, f: NamedError(name, errors.New("named error")), want: "named error"})
			tests = append(tests, test{name: name, f: Error(errors.New("error")), want: "error"})
			tests = append(tests, test{name: name, f: Error(nil), want: "<nil>"})
		case AnyType:
			tests = append(tests, test{name: name, f: Any(name, 1), want: "1"})
			tests = append(tests, test{name: name, f: Any(name, 9223372036854775807), want: "9223372036854775807"})
			tests = append(tests, test{name: name, f: Any(name, "any string"), want: "any string"})
			tests = append(tests, test{name: name, f: Any(name, true), want: "true"})
			tests = append(tests, test{name: name, f: Any(name, []string{"Abc", "Def", "Ghi"}), want: "[Abc Def Ghi]"})
			tests = append(tests, test{name: name, f: Any(name, errors.New("error")), want: "error"})
			tests = append(tests, test{name: name, f: Any(name, nil), want: "<nil>"})
		case StringerType:
			tests = append(tests, test{name: name, f: Stringer(name, &stringerTest{}), want: "stringerTest"})
		case InvalidType:
			tests = append(tests, test{name: "panic InvalidType" + name, f: Field{ftype: ftype, key: "default"}, want: "", panic: true})
		default:
			tests = append(tests, test{name: "fail " + name, f: Field{ftype: ftype, key: "default"}, want: "", fail: true})
		}

	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			// Known fieldType, but String() panics with it.
			if tt.panic {
				require.Panics(t, func() { _ = tt.f.String() })
				return
			}

			// Unknown fieldType, maybe a new one has been added
			if tt.fail {
				t.Fail()
				return
			}

			require.Equal(t, tt.want, tt.f.String())

		})
	}

}

func TestField_AnyValue(t *testing.T) {

	tests := []struct {
		name string
		f    Field
		want interface{}
	}{

		{name: "int", f: Int("any", 1), want: 1},
		{name: "int64", f: Int64("any", 9223372036854775807), want: int64(9223372036854775807)},
		{name: "string", f: String("any", "any string"), want: "any string"},
		{name: "bool", f: Bool("any", true), want: true},
		{name: "[]string", f: Strings("any", []string{"Abc", "Def", "Ghi"}), want: []string{"Abc", "Def", "Ghi"}},
		{name: "error", f: Error(errors.New("error")), want: errors.New("error")},
		{name: "namedError", f: NamedError("any", nil), want: nil},
		{name: "stringer", f: Stringer("any", &stringerTest{}), want: &stringerTest{}},

		{name: "any_int", f: Any("any", 1), want: 1},
		{name: "any_int64", f: Any("any", 9223372036854775807), want: 9223372036854775807},
		{name: "any_string", f: Any("any", "any string"), want: "any string"},
		{name: "any_bool", f: Any("any", true), want: true},
		{name: "any_[]string", f: Any("any", []string{"Abc", "Def", "Ghi"}), want: []string{"Abc", "Def", "Ghi"}},
		{name: "any_error", f: Any("any", errors.New("error")), want: errors.New("error")},
		{name: "struct", f: Any("any", struct{ str string }{str: "test"}), want: struct{ str string }{str: "test"}},
		{name: "any_nil", f: Any("any", nil), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			require.Equal(t, tt.want, tt.f.AnyValue())

		})
	}
}
