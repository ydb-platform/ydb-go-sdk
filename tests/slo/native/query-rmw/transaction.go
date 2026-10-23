package main

import (
	"context"
	"errors"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"slo/internal/rmw"
)

type transaction struct {
	tx       query.TxActor
	counters string
	ledger   string
}

func (t transaction) Ledger(ctx context.Context, id uint64) (rmw.Operation, bool, error) {
	row, err := t.tx.QueryRow(ctx,
		fmt.Sprintf(`DECLARE $id AS Uint64;
SELECT op_id, counter_id, delta FROM %s WHERE op_id = $id;`, t.ledger),
		query.WithParameters(ydb.ParamsBuilder().Param("$id").Uint64(id).Build()),
	)
	if errors.Is(err, query.ErrNoRows) {
		return rmw.Operation{}, false, nil
	}
	if err != nil {
		return rmw.Operation{}, false, err
	}
	var op rmw.Operation
	err = row.Scan(&op.ID, &op.Counter, &op.Delta)

	return op, true, err
}

func (t transaction) Counter(ctx context.Context, id uint64) (rmw.Counter, error) {
	row, err := t.tx.QueryRow(ctx,
		fmt.Sprintf(`DECLARE $id AS Uint64;
SELECT counter_id, value, version FROM %s WHERE counter_id = $id;`, t.counters),
		query.WithParameters(ydb.ParamsBuilder().Param("$id").Uint64(id).Build()),
	)
	if err != nil {
		return rmw.Counter{}, err
	}
	var counter rmw.Counter
	err = row.Scan(&counter.ID, &counter.Value, &counter.Version)

	return counter, err
}

func (t transaction) Write(ctx context.Context, counter rmw.Counter, op rmw.Operation) error {
	return t.tx.Exec(ctx, fmt.Sprintf(`
DECLARE $counter AS Uint64;
DECLARE $value AS Uint64;
DECLARE $version AS Uint64;
DECLARE $id AS Uint64;
DECLARE $delta AS Uint64;
UPSERT INTO %s (counter_id, value, version) VALUES ($counter, $value, $version);
INSERT INTO %s (op_id, counter_id, delta) VALUES ($id, $counter, $delta);
`, t.counters, t.ledger), query.WithParameters(ydb.ParamsBuilder().
		Param("$counter").Uint64(counter.ID).
		Param("$value").Uint64(counter.Value).
		Param("$version").Uint64(counter.Version).
		Param("$id").Uint64(op.ID).
		Param("$delta").Uint64(op.Delta).
		Build(),
	))
}
