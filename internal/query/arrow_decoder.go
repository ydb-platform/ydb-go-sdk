package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func decodeArrowBatches(
	ctx context.Context, decoder arrow.Decoder, columns []*Ydb.Column, part *Ydb.ResultSet,
) (batches []arrow.Batch, err error) {
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
	batches, err = decoder(ctx, metadata, io.MultiReader(
		bytes.NewReader(part.GetArrowFormatMeta().GetSchema()), bytes.NewReader(part.GetData()),
	))
	defer func() {
		if err != nil {
			releaseArrowBatches(batches)
			batches = nil
		}
	}()
	if errors.Is(err, io.EOF) {
		return batches, fmt.Errorf("arrow decoder returned EOF: %w", io.ErrUnexpectedEOF)
	}
	if err != nil {
		return batches, err
	}
	for i, batch := range batches {
		if batch == nil {
			return batches, fmt.Errorf("arrow decoder batch %d is nil", i)
		}
		if batch.NumRows() < 0 || batch.NumCols() != len(columns) {
			return batches, fmt.Errorf(
				"arrow decoder batch %d has invalid shape: %d rows and %d columns, expected %d columns",
				i, batch.NumRows(), batch.NumCols(), len(columns),
			)
		}
	}

	return batches, nil
}

func releaseArrowBatches(batches []arrow.Batch) {
	for _, batch := range batches {
		if batch != nil {
			batch.Release()
		}
	}
}

func (r *streamResult) decodeArrowBatches(
	ctx context.Context, columns []*Ydb.Column, part *Ydb.ResultSet,
) ([]*arrowRowData, error) {
	batches, err := decodeArrowBatches(ctx, r.arrowDecoder, columns, part)
	if err != nil {
		return nil, fmt.Errorf("arrow result set %d: %w", r.lastPart.GetResultSetIndex(), err)
	}
	r.arrowBatches = append(r.arrowBatches, batches...)
	data := make([]*arrowRowData, len(batches))
	for i, batch := range batches {
		data[i] = &arrowRowData{columns: columns, batch: batch, rows: make([]arrowRow, batch.NumRows())}
		for j := range data[i].rows {
			data[i].rows[j] = arrowRow{data: data[i], index: j}
		}
	}

	return data, nil
}

func (r *streamResult) takeArrowBatches() func() {
	if len(r.arrowBatches) == 0 {
		return nil
	}
	batches := r.arrowBatches
	r.arrowBatches = nil

	return sync.OnceFunc(func() { releaseArrowBatches(batches) })
}

func (r *streamResult) releaseArrowBatches() {
	releaseArrowBatches(r.arrowBatches)
	r.arrowBatches = nil
}
