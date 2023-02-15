//go:build go1.18
// +build go1.18

package sugar

import (
	"database/sql"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestGenerateDeclareSection_ParameterOption(t *testing.T) {
	splitDeclares := func(declaresSection string) (declares []string) {
		for _, s := range strings.Split(declaresSection, ";") {
			s = strings.TrimSpace(s)
			if s != "" {
				declares = append(declares, s)
			}
		}
		sort.Strings(declares)
		return declares
	}
	for _, tt := range []struct {
		params  []table.ParameterOption
		declare string
	}{
		{
			params: []table.ParameterOption{
				table.ValueParam(
					"$values",
					types.ListValue(
						types.Uint64Value(1),
						types.Uint64Value(2),
						types.Uint64Value(3),
						types.Uint64Value(4),
						types.Uint64Value(5),
					),
				),
			},
			declare: `
				DECLARE $values AS List<Uint64>;
			`,
		},
		{
			params: []table.ParameterOption{
				table.ValueParam(
					"$delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			},
			declare: `
				DECLARE $delta AS Interval;
			`,
		},
		{
			params: []table.ParameterOption{
				table.ValueParam(
					"$ts",
					types.TimestampValueFromTime(time.Now()),
				),
			},
			declare: `
				DECLARE $ts AS Timestamp;
			`,
		},
		{
			params: []table.ParameterOption{
				table.ValueParam(
					"$a",
					types.BoolValue(true),
				),
				table.ValueParam(
					"$b",
					types.Int64Value(123),
				),
				table.ValueParam(
					"$c",
					types.OptionalValue(types.TextValue("test")),
				),
			},
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
		{
			params: []table.ParameterOption{
				table.ValueParam(
					"$a",
					types.BoolValue(true),
				),
				table.ValueParam(
					"b",
					types.Int64Value(123),
				),
				table.ValueParam(
					"c",
					types.OptionalValue(types.TextValue("test")),
				),
			},
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
	} {
		t.Run("", func(t *testing.T) {
			declares, err := GenerateDeclareSection(tt.params)
			require.NoError(t, err)
			got := splitDeclares(declares)
			want := splitDeclares(tt.declare)
			if len(got) != len(want) {
				t.Errorf("len(got) = %v, len(want) = %v", len(got), len(want))
			} else {
				for i := range got {
					if strings.TrimSpace(got[i]) != strings.TrimSpace(want[i]) {
						t.Errorf(
							"unexpected generation of declare section:\n%v\n\nwant:\n%v",
							strings.Join(got, ";\n"),
							strings.Join(want, ";\n"),
						)
					}
				}
			}
		})
	}
}

func TestGenerateDeclareSection_NamedArg(t *testing.T) {
	splitDeclares := func(declaresSection string) (declares []string) {
		for _, s := range strings.Split(declaresSection, ";") {
			s = strings.TrimSpace(s)
			if s != "" {
				declares = append(declares, s)
			}
		}
		sort.Strings(declares)
		return declares
	}
	for _, tt := range []struct {
		params  []sql.NamedArg
		declare string
	}{
		{
			params: []sql.NamedArg{
				sql.Named(
					"values",
					types.ListValue(
						types.Uint64Value(1),
						types.Uint64Value(2),
						types.Uint64Value(3),
						types.Uint64Value(4),
						types.Uint64Value(5),
					),
				),
			},
			declare: `
				DECLARE $values AS List<Uint64>;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named(
					"delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			},
			declare: `
				DECLARE $delta AS Interval;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named(
					"ts",
					types.TimestampValueFromTime(time.Now()),
				),
			},
			declare: `
				DECLARE $ts AS Timestamp;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named(
					"a",
					types.BoolValue(true),
				),
				sql.Named(
					"b",
					types.Int64Value(123),
				),
				sql.Named(
					"c",
					types.OptionalValue(types.TextValue("test")),
				),
			},
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named(
					"a",
					types.BoolValue(true),
				),
				sql.Named(
					"b",
					types.Int64Value(123),
				),
				sql.Named(
					"c",
					types.OptionalValue(types.TextValue("test")),
				),
			},
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},

		{
			params: []sql.NamedArg{
				sql.Named("delta", time.Hour),
			},
			declare: `
				DECLARE $delta AS Interval;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named("ts", time.Now()),
			},
			declare: `
				DECLARE $ts AS Timestamp;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named("$a", true),
				sql.Named("$b", int64(123)),
				sql.Named("$c", func(s string) *string { return &s }("test")),
			},
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
		{
			params: []sql.NamedArg{
				sql.Named("$a", func(b bool) *bool { return &b }(true)),
				sql.Named("b", func(i int64) *int64 { return &i }(123)),
				sql.Named("c", func(s string) *string { return &s }("test")),
			},
			declare: `
				DECLARE $a AS Optional<Bool>;
				DECLARE $b AS Optional<Int64>; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
	} {
		t.Run("", func(t *testing.T) {
			declares, err := GenerateDeclareSection(tt.params)
			require.NoError(t, err)
			got := splitDeclares(declares)
			want := splitDeclares(tt.declare)
			if len(got) != len(want) {
				t.Errorf("len(got) = %v, len(want) = %v", len(got), len(want))
			} else {
				for i := range got {
					if strings.TrimSpace(got[i]) != strings.TrimSpace(want[i]) {
						t.Errorf("unexpected generation of declare section:\n\n%v\n\nwant:\n%v",
							strings.Join(got, ";\n"),
							strings.Join(want, ";\n"),
						)
					}
				}
			}
		})
	}
}
