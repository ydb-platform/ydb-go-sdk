package sugar

import (
	"database/sql"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestGenerateDeclareSection(t *testing.T) {
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
		params  *table.QueryParameters
		declare string
	}{
		{
			params: table.NewQueryParameters(
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
			),
			declare: `
				DECLARE $values AS List<Uint64>;
			`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam(
					"$delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			),
			declare: `
				DECLARE $delta AS Interval;
			`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam("$ts", types.TimestampValueFromTime(time.Now())),
			),
			declare: `
				DECLARE $ts AS Timestamp;
			`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam("$a", types.BoolValue(true)),
				table.ValueParam("$b", types.Int64Value(123)),
				table.ValueParam("$c", types.OptionalValue(types.TextValue("test"))),
			),
			declare: `
				DECLARE $a AS Bool;
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam("$a", types.BoolValue(true)),
				table.ValueParam("b", types.Int64Value(123)),
				table.ValueParam("c", types.OptionalValue(types.TextValue("test"))),
			),
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

func TestGenerateDeclareSection_ParameterOption(t *testing.T) {
	b := testutil.QueryBind(bind.AutoDeclare{})
	getDeclares := func(declaresSection string) (declares []string) {
		for _, s := range strings.Split(declaresSection, "\n") {
			s = strings.TrimSpace(s)
			if s != "" && !strings.HasPrefix(s, "--") {
				declares = append(declares, strings.TrimRight(s, ";"))
			}
		}
		sort.Strings(declares)

		return declares
	}
	for _, tt := range []struct {
		params   []interface{}
		declares []string
	}{
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $values AS List<Uint64>",
			},
		},
		{
			params: []interface{}{
				table.ValueParam(
					"$delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			},
			declares: []string{
				"DECLARE $delta AS Interval",
			},
		},
		{
			params: []interface{}{
				table.ValueParam(
					"$ts",
					types.TimestampValueFromTime(time.Now()),
				),
			},
			declares: []string{
				"DECLARE $ts AS Timestamp",
			},
		},
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $a AS Bool",
				"DECLARE $b AS Int64",
				"DECLARE $c AS Optional<Utf8>",
			},
		},
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $a AS Bool",
				"DECLARE $b AS Int64",
				"DECLARE $c AS Optional<Utf8>",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, _, err := b.ToYdb("", tt.params...)
			require.NoError(t, err)
			require.Equal(t, tt.declares, getDeclares(yql))
		})
	}
}

func TestGenerateDeclareSection_NamedArg(t *testing.T) {
	b := testutil.QueryBind(bind.AutoDeclare{})
	getDeclares := func(declaresSection string) (declares []string) {
		for _, s := range strings.Split(declaresSection, "\n") {
			s = strings.TrimSpace(s)
			if s != "" && !strings.HasPrefix(s, "--") {
				declares = append(declares, strings.TrimRight(s, ";"))
			}
		}
		sort.Strings(declares)

		return declares
	}
	for _, tt := range []struct {
		params   []interface{}
		declares []string
	}{
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $values AS List<Uint64>",
			},
		},
		{
			params: []interface{}{
				sql.Named(
					"delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			},
			declares: []string{
				"DECLARE $delta AS Interval",
			},
		},
		{
			params: []interface{}{
				sql.Named(
					"ts",
					types.TimestampValueFromTime(time.Now()),
				),
			},
			declares: []string{
				"DECLARE $ts AS Timestamp",
			},
		},
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $a AS Bool",
				"DECLARE $b AS Int64",
				"DECLARE $c AS Optional<Utf8>",
			},
		},
		{
			params: []interface{}{
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
			declares: []string{
				"DECLARE $a AS Bool",
				"DECLARE $b AS Int64",
				"DECLARE $c AS Optional<Utf8>",
			},
		},

		{
			params: []interface{}{
				sql.Named("delta", time.Hour),
			},
			declares: []string{
				"DECLARE $delta AS Interval",
			},
		},
		{
			params: []interface{}{
				sql.Named("ts", time.Now()),
			},
			declares: []string{
				"DECLARE $ts AS Timestamp",
			},
		},
		{
			params: []interface{}{
				sql.Named("$a", true),
				sql.Named("$b", int64(123)),
				sql.Named("$c", func(s string) *string { return &s }("test")),
			},
			declares: []string{
				"DECLARE $a AS Bool",
				"DECLARE $b AS Int64",
				"DECLARE $c AS Optional<Utf8>",
			},
		},
		{
			params: []interface{}{
				sql.Named("$a", func(b bool) *bool { return &b }(true)),
				sql.Named("b", func(i int64) *int64 { return &i }(123)),
				sql.Named("c", func(s string) *string { return &s }("test")),
			},
			declares: []string{
				"DECLARE $a AS Optional<Bool>",
				"DECLARE $b AS Optional<Int64>",
				"DECLARE $c AS Optional<Utf8>",
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			yql, _, err := b.ToYdb("", tt.params...)
			require.NoError(t, err)
			require.Equal(t, tt.declares, getDeclares(yql))
		})
	}
}

func TestToYdbParam(t *testing.T) {
	for _, tt := range []struct {
		name     string
		param    sql.NamedArg
		ydbParam table.ParameterOption
		err      error
	}{
		{
			name:     xtest.CurrentFileLine(),
			param:    sql.Named("a", "b"),
			ydbParam: table.ValueParam("$a", types.TextValue("b")),
			err:      nil,
		},
		{
			name:     xtest.CurrentFileLine(),
			param:    sql.Named("a", 123),
			ydbParam: table.ValueParam("$a", types.Int32Value(123)),
			err:      nil,
		},
		{
			name: xtest.CurrentFileLine(),
			param: sql.Named("a", types.OptionalValue(types.TupleValue(
				types.BytesValue([]byte("test")),
				types.TextValue("test"),
				types.Uint64Value(123),
			))),
			ydbParam: table.ValueParam("$a", types.OptionalValue(types.TupleValue(
				types.BytesValue([]byte("test")),
				types.TextValue("test"),
				types.Uint64Value(123),
			))),
			err: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ydbParam, err := ToYdbParam(tt.param)
			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.ydbParam, ydbParam)
			}
		})
	}
}
