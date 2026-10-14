package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"slo/internal/rmw"
)

type storage struct {
	driver       *ydb.Driver
	counters     string
	ledger       string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (d *storage) ddl(ctx context.Context, sql string) error {
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		return session.Exec(ctx, sql, query.WithTxControl(query.ImplicitTxControl()))
	}, query.WithIdempotent(), query.WithLabel("RMW SCHEMA"))
}

func (d *storage) Setup(ctx context.Context, counters uint64) error {
	if err := d.ddl(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 counter_id Uint64 NOT NULL, value Uint64 NOT NULL, version Uint64 NOT NULL,
 PRIMARY KEY (counter_id)
);`, d.counters)); err != nil {
		return err
	}
	if err := d.ddl(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 op_id Uint64 NOT NULL, counter_id Uint64 NOT NULL, delta Uint64 NOT NULL,
 PRIMARY KEY (op_id)
);`, d.ledger)); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	return d.driver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		for id := range counters {
			if err := tx.Exec(ctx, fmt.Sprintf(`DECLARE $id AS Uint64;
UPSERT INTO %s (counter_id, value, version) VALUES ($id, 0ul, 0ul);`, d.counters),
				query.WithParameters(ydb.ParamsBuilder().Param("$id").Uint64(id).Build()),
			); err != nil {
				return err
			}
		}

		return nil
	}, query.WithIdempotent(), query.WithLabel("RMW INITIALIZE"))
}

func (d *storage) Apply(ctx context.Context, op rmw.Operation) (attempts int, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.writeTimeout)
	defer cancel()

	// DoTx defaults to SerializableReadWrite and owns begin/commit/rollback and
	// retries. op is captured once, outside its callback, so commit ambiguity is
	// safe to retry with the existing idempotent retry policy.
	err = d.driver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		return rmw.Apply(ctx, transaction{tx: tx, counters: d.counters, ledger: d.ledger}, op)
	}, query.WithIdempotent(), query.WithLabel("RMW"), query.WithTrace(&trace.Query{
		OnDoTx: func(trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			return func(info trace.QueryDoTxDoneInfo) { attempts = info.Attempts }
		},
	}))

	return attempts, err
}

func (d *storage) Snapshot(ctx context.Context) (state rmw.State, attempts int, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.readTimeout)
	defer cancel()

	err = d.driver.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		// Discard partial observations when the SDK retries this independent read.
		state = rmw.State{}
		if err := readRows(ctx, tx, fmt.Sprintf("SELECT counter_id, value, version FROM %s;", d.counters),
			func(row query.Row) error {
				var counter rmw.Counter
				if err := row.Scan(&counter.ID, &counter.Value, &counter.Version); err != nil {
					return err
				}
				state.Counters = append(state.Counters, counter)

				return nil
			}); err != nil {
			return err
		}

		return readRows(ctx, tx, fmt.Sprintf("SELECT op_id, counter_id, delta FROM %s;", d.ledger),
			func(row query.Row) error {
				var op rmw.Operation
				if err := row.Scan(&op.ID, &op.Counter, &op.Delta); err != nil {
					return err
				}
				state.Ledger = append(state.Ledger, op)

				return nil
			})
	}, query.WithIdempotent(), query.WithLabel("RMW VERIFY"), query.WithTrace(&trace.Query{
		OnDoTx: func(trace.QueryDoTxStartInfo) func(trace.QueryDoTxDoneInfo) {
			return func(info trace.QueryDoTxDoneInfo) { attempts = info.Attempts }
		},
	}))

	return state, attempts, err
}

func readRows(ctx context.Context, tx query.TxActor, sql string, scan func(query.Row) error) (err error) {
	rows, err := tx.QueryResultSet(ctx, sql)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close(ctx)) }()
	for {
		row, err := rows.NextRow(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := scan(row); err != nil {
			return err
		}
	}
}

func (d *storage) Drop(ctx context.Context) error {
	counterErr := d.ddl(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", d.counters))
	ledgerErr := d.ddl(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", d.ledger))

	return errors.Join(counterErr, ledgerErr)
}

func (d *storage) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}
