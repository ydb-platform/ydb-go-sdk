package logs

import (
	"errors"
	"fmt"
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
	}

	tests := []test{}

	for i := 0; i < int(EndType); i++ {

		ftype := FieldType(i)

		name := fmt.Sprintf("type %s", ftype)

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
		default:
			tests = append(tests, test{name: "panic " + name, f: Field{ftype: ftype, key: "default"}, want: "", panic: true})
		}

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.panic {
				require.Panics(t, func() { _ = tt.f.String() })
			} else {
				require.Equal(t, tt.want, tt.f.String())
			}
		})
	}

}
