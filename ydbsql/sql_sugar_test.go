package ydbsql

import (
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestDeclaration(t *testing.T) {
	for _, test := range []struct {
		decl func(*Declaration)
		exp  string
	}{
		{
			decl: func(d *Declaration) {
				d.Declare("foo", types.TypeString)
				d.Declare("bar", types.TypeInt64)
				d.Declare("baz", types.Struct(
					types.StructField("foo", types.TypeString),
					types.StructField("bar", types.TypeInt64),
					types.StructField("baz", types.Tuple(
						types.TypeString, types.TypeInt64,
					)),
				))
			},
			exp: strings.Join([]string{
				"DECLARE $foo AS \"String\";",
				"DECLARE $bar AS \"Int64\";",
				"DECLARE $baz AS \"Struct<" +
					"foo:String," +
					"bar:Int64," +
					"baz:Tuple<String,Int64>>\";",
				"",
			}, "\n"),
		},
	} {
		t.Run("", func(t *testing.T) {
			var d Declaration
			test.decl(&d)
			if act, exp := d.String(), test.exp; act != exp {
				t.Fatalf("unexpected declaration: %q; want %q", act, exp)
			}
		})
	}
}
