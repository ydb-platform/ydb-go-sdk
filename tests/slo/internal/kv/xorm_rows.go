package kv

import (
	"context"

	"xorm.io/xorm"

	"slo/internal/generator"
)

// ReadXORMRows avoids Get's LIMIT 1 and checks both iteration and Close errors.
// Each call owns a fresh result image; its caller checks cardinality after retry.
func ReadXORMRows(ctx context.Context, engine *xorm.Engine, id generator.RowID) (_ []generator.Row, err error) {
	iterator, err := engine.Context(ctx).NoCache().
		Where("hash = Digest::NumericHash(?) AND id = ?", id, id).Rows(&generator.Row{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := iterator.Close(); err == nil {
			err = closeErr
		}
	}()

	rows := make([]generator.Row, 0)
	for iterator.Next() {
		var row generator.Row
		if err := iterator.Scan(&row); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := iterator.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}
