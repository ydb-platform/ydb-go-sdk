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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

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
			yql, _, err := b.RewriteQuery("", tt.params...)
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
			yql, _, err := b.RewriteQuery("", tt.params...)
			require.NoError(t, err)
			require.Equal(t, tt.declares, getDeclares(yql))
		})
	}
}
