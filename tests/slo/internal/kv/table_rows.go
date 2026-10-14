package kv

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"

	"slo/internal/generator"
)

// ReadTableRows materializes an attempt without interpreting its cardinality.
// Unary advance errors need not be stored in Err, so never use the bool advance.
func ReadTableRows(ctx context.Context, res result.BaseResult) ([]generator.Row, error) {
	rows := make([]generator.Row, 0)
	for {
		if err := res.Err(); err != nil {
			return nil, err
		}
		err := res.NextResultSetErr(ctx)
		if errors.Is(err, io.EOF) {
			if err := res.Err(); err != nil {
				return nil, err
			}

			return rows, ctx.Err()
		}
		if err != nil {
			return nil, err
		}
		if res.CurrentResultSet().Truncated() {
			return nil, fmt.Errorf("read rows result set truncated")
		}
		for res.NextRow() {
			var row generator.Row
			if err := res.ScanNamed(
				named.Required("id", &row.ID),
				named.Optional("payload_str", &row.PayloadStr),
				named.Optional("payload_double", &row.PayloadDouble),
				named.Optional("payload_timestamp", &row.PayloadTimestamp),
				named.Optional("payload_hash", &row.PayloadHash),
			); err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
}
