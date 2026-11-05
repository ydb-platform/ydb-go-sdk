package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const (
	operationTimeout  = 30 * time.Second
	operationInterval = 500 * time.Millisecond
	shutdownGrace     = 180 * time.Second
)

var errDataMismatch = errors.New("incorrect read-after-write data")

type observation struct {
	Event      string    `json:"event"`
	Time       time.Time `json:"ts"`
	Sequence   uint64    `json:"seq"`
	OK         bool      `json:"ok"`
	DurationMS int64     `json:"durationMs"`
	Error      string    `json:"error,omitempty"`
}

type result struct {
	SDKVersion   string    `json:"sdkVersion"`
	Successes    int       `json:"successes"`
	Failures     int       `json:"failures"`
	Verified     bool      `json:"verified"`
	Missing      []uint64  `json:"missing"`
	Fatal        string    `json:"fatal,omitempty"`
	AuditStarted time.Time `json:"auditStartedAt"`
	Passed       bool      `json:"passed"`
}

func main() {
	r := result{SDKVersion: ydb.Version}
	if err := run(&r); err != nil {
		r.Fatal = err.Error()
	}
	r.Passed = r.Successes > 0 && r.Verified && len(r.Missing) == 0 && r.Fatal == ""
	data, err := json.MarshalIndent(r, "", "  ")
	if err == nil {
		err = os.MkdirAll("/tmp", 0o700)
	}
	if err == nil {
		err = os.WriteFile("/tmp/bridge-probe.json", data, 0o600)
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, _ = fmt.Fprintln(os.Stdout, string(data))
	if !r.Passed {
		os.Exit(1)
	}
}

func run(r *result) error {
	seconds, err := strconv.Atoi(os.Getenv("WORKLOAD_DURATION"))
	if err != nil || seconds <= 0 {
		return errors.New("WORKLOAD_DURATION must be a positive integer")
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	driver, err := ydb.Open(connectCtx, os.Getenv("YDB_ENDPOINT")+os.Getenv("YDB_DATABASE"),
		ydb.WithAnonymousCredentials())
	cancel()
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), operationTimeout)
		defer closeCancel()
		_ = driver.Close(closeCtx)
	}()
	table := fmt.Sprintf("`%s`", path.Join(os.Getenv("YDB_DATABASE"),
		fmt.Sprintf("bridge_probe_%d", time.Now().UnixNano())))
	createCtx, createCancel := context.WithTimeout(context.Background(), time.Minute)
	err = driver.Query().Exec(createCtx,
		fmt.Sprintf("CREATE TABLE %s (id Uint64, payload Utf8, PRIMARY KEY(id));", table),
		query.WithTxControl(query.ImplicitTxControl()))
	createCancel()
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGUSR1)
	defer stop()
	deadline := time.Now().Add(time.Duration(seconds)*time.Second + shutdownGrace)
	confirmed := make(map[uint64]string)
	encoder := json.NewEncoder(os.Stdout)
	for seq := uint64(1); time.Now().Before(deadline) && stopCtx.Err() == nil; seq++ {
		payload := fmt.Sprintf("confirmed_%d", seq)
		started := time.Now()
		ctx, opCancel := context.WithTimeout(context.Background(), operationTimeout)
		err = writeRead(ctx, driver.Query(), table, seq, payload)
		opCancel()
		event := observation{
			Event: "operation", Time: time.Now().UTC(), Sequence: seq,
			OK: err == nil, DurationMS: time.Since(started).Milliseconds(),
		}
		if err != nil {
			event.Error = err.Error()
			r.Failures++
		} else {
			confirmed[seq] = payload
			r.Successes++
		}
		if emitErr := encoder.Encode(event); emitErr != nil {
			return fmt.Errorf("log operation: %w", emitErr)
		}
		if errors.Is(err, errDataMismatch) {
			return err
		}
		timer := time.NewTimer(operationInterval)
		select {
		case <-stopCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	r.AuditStarted = time.Now().UTC()
	auditCtx, auditCancel := context.WithTimeout(context.Background(), time.Minute)
	defer auditCancel()
	var stored map[uint64]string
	err = driver.Query().Do(auditCtx, func(ctx context.Context, session query.Session) error {
		var readErr error
		stored, readErr = readStored(ctx, session, table)

		return readErr
	}, query.WithIdempotent())
	if err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	r.Missing = missingRows(confirmed, stored)
	r.Verified = true

	return nil
}

func writeRead(ctx context.Context, client query.Client, table string, id uint64, payload string) error {
	return client.DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		if err := tx.Exec(ctx,
			fmt.Sprintf("DECLARE $id AS Uint64; DECLARE $payload AS Utf8; "+
				"UPSERT INTO %s (id, payload) VALUES ($id, $payload);", table),
			query.WithParameters(ydb.ParamsBuilder().Param("$id").Uint64(id).
				Param("$payload").Text(payload).Build())); err != nil {
			return err
		}
		row, err := tx.QueryRow(ctx,
			fmt.Sprintf("DECLARE $id AS Uint64; SELECT payload FROM %s WHERE id = $id;", table),
			query.WithParameters(ydb.ParamsBuilder().Param("$id").Uint64(id).Build()))
		if err != nil {
			if errors.Is(err, query.ErrNoRows) {
				return fmt.Errorf("%w: missing row %d", errDataMismatch, id)
			}

			return err
		}
		var actual *string
		if err = row.Scan(&actual); err != nil {
			return err
		}

		return validatePayload(payload, actual)
	}, query.WithIdempotent())
}

func validatePayload(expected string, actual *string) error {
	if actual == nil || *actual != expected {
		return errDataMismatch
	}

	return nil
}

func readStored(ctx context.Context, executor query.Executor, table string) (map[uint64]string, error) {
	res, err := executor.Query(ctx, fmt.Sprintf("SELECT id, payload FROM %s;", table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Close(ctx) }()
	rs, err := res.NextResultSet(ctx)
	if err != nil {
		return nil, err
	}
	stored := make(map[uint64]string)
	for {
		row, rowErr := rs.NextRow(ctx)
		if errors.Is(rowErr, io.EOF) {
			break
		}
		if rowErr != nil {
			return nil, rowErr
		}
		var id *uint64
		var payload *string
		if err = row.Scan(&id, &payload); err != nil {
			return nil, err
		}
		if id == nil || payload == nil {
			return nil, errDataMismatch
		}
		if _, exists := stored[*id]; exists {
			return nil, fmt.Errorf("%w: duplicate row %d", errDataMismatch, *id)
		}
		stored[*id] = *payload
	}
	if _, err = res.NextResultSet(ctx); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("unexpected additional audit result set")
		}

		return nil, err
	}

	return stored, nil
}

func missingRows(confirmed, stored map[uint64]string) []uint64 {
	missing := make([]uint64, 0)
	for id, expected := range confirmed {
		if actual, ok := stored[id]; !ok || actual != expected {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)

	return missing
}
