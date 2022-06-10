package sugar

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestToDeclare(t *testing.T) {
	removeEmpty := func(in []string) (out []string) {
		for _, s := range in {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
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
			declare: `DECLARE $values AS List<Uint64>;`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam(
					"$delta",
					types.IntervalValueFromDuration(time.Hour),
				),
			),
			declare: `DECLARE $delta AS Interval;`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam(
					"$ts",
					types.TimestampValueFromTime(time.Now()),
				),
			),
			declare: `DECLARE $ts AS Timestamp;`,
		},
		{
			params: table.NewQueryParameters(
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
					types.OptionalValue(types.UTF8Value("test")),
				),
			),
			declare: `
				DECLARE $a AS Bool; 
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
		{
			params: table.NewQueryParameters(
				table.ValueParam(
					"a",
					types.BoolValue(true),
				),
				table.ValueParam(
					"b",
					types.Int64Value(123),
				),
				table.ValueParam(
					"c",
					types.OptionalValue(types.UTF8Value("test")),
				),
			),
			declare: `
				DECLARE $a AS Bool; 
				DECLARE $b AS Int64; 
				DECLARE $c AS Optional<Utf8>;
			`,
		},
	} {
		t.Run("", func(t *testing.T) {
			got := removeEmpty(strings.Split(ToDeclare(tt.params), "\n"))
			want := removeEmpty(strings.Split(tt.declare, "\n"))
			sort.Strings(got)
			sort.Strings(want)
			if len(got) != len(want) {
				t.Errorf("len(got) = %v, len(want) = %v", len(got), len(want))
			} else {
				for i := range got {
					if strings.TrimSpace(got[i]) != strings.TrimSpace(want[i]) {
						t.Errorf("ToDeclare() = %v, want %v", got, want)
					}
				}
			}
		})
	}
}
