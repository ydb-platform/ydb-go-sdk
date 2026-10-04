package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func decodeArrowRows(
	ctx context.Context, decoder arrow.Decoder, columns []*Ydb.Column, part *Ydb.ResultSet,
) ([][]value.Value, error) {
	if decoder == nil {
		return nil, fmt.Errorf("received Arrow results without an Arrow decoder")
	}
	if len(part.GetData()) == 0 {
		return nil, nil
	}
	metadata := make([]arrow.Column, len(columns))
	for i, column := range columns {
		metadata[i] = arrow.Column{Name: column.GetName(), Type: types.TypeFromYDB(column.GetType())}
	}
	rows, err := decoder(ctx, metadata, io.MultiReader(
		bytes.NewReader(part.GetArrowFormatMeta().GetSchema()), bytes.NewReader(part.GetData()),
	))
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("arrow decoder returned EOF: %w", io.ErrUnexpectedEOF)
	}
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("arrow decoder row %d has %d values, expected %d", i, len(row), len(columns))
		}
		for j, v := range row {
			if v == nil {
				return nil, fmt.Errorf("arrow decoder row %d column %d has a nil value", i, j)
			}
		}
	}

	return rows, nil
}
