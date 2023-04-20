package log

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stringerTest string

func (s stringerTest) String() string {
	return string(s)
}

func TestField_String(t *testing.T) {
	for _, tt := range []struct {
		f     Field
		want  string
		panic bool
		fail  bool
	}{
		{f: Int("a", 1), want: "1"},
		{f: Int64("b", 9223372036854775807), want: "9223372036854775807"},
		{f: String("c", "test"), want: "test"},
		{f: Bool("d", true), want: "true"},
		{f: Duration("e", time.Hour), want: time.Hour.String()},
		{f: Strings("f", []string{"Abc", "Def", "Ghi"}), want: "[Abc Def Ghi]"},
		{f: NamedError("g", errors.New("named error")), want: "named error"},
		{f: Error(errors.New("error")), want: "error"},
		{f: Error(nil), want: "<nil>"},
		{f: Any("h", 1), want: "1"},
		{f: Any("i", 9223372036854775807), want: "9223372036854775807"},
		{f: Any("j", "any string"), want: "any string"},
		{f: Any("k", true), want: "true"},
		{f: Any("l", []string{"Abc", "Def", "Ghi"}), want: "[Abc Def Ghi]"},
		{f: Any("m", errors.New("error")), want: "error"},
		{f: Any("n", nil), want: "<nil>"},
		{f: Stringer("o", stringerTest("stringerTest")), want: "stringerTest"},
		{f: Field{ftype: InvalidType, key: "invalid"}, want: "", panic: true},
	} {
		t.Run(tt.f.key, func(t *testing.T) {
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
	for _, tt := range []struct {
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
		{name: "stringer", f: Stringer("any", stringerTest("stringerTest")), want: stringerTest("stringerTest")},

		{name: "any_int", f: Any("any", 1), want: 1},
		{name: "any_int64", f: Any("any", 9223372036854775807), want: 9223372036854775807},
		{name: "any_string", f: Any("any", "any string"), want: "any string"},
		{name: "any_bool", f: Any("any", true), want: true},
		{name: "any_[]string", f: Any("any", []string{"Abc", "Def", "Ghi"}), want: []string{"Abc", "Def", "Ghi"}},
		{name: "any_error", f: Any("any", errors.New("error")), want: errors.New("error")},
		{name: "struct", f: Any("any", struct{ str string }{str: "test"}), want: struct{ str string }{str: "test"}},
		{name: "any_nil", f: Any("any", nil), want: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.f.AnyValue())
		})
	}
}
