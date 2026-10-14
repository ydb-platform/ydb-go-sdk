package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"

	"gorm.io/gorm"
	gormtests "gorm.io/gorm/utils/tests"

	"slo/internal/generator"
	"slo/internal/kv"
)

func TestORMOperationsUseOneConnection(t *testing.T) {
	raw := sql.OpenDB(singleConnectionConnector{})
	raw.SetMaxOpenConns(1)
	raw.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = raw.Close() })
	gormDB, err := gorm.Open(gormtests.DummyDialector{}, &gorm.Config{ConnPool: raw, DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	storage := &db{gormDB: gormDB, tableName: "owned", readTimeout: 2 * time.Second, writeTimeout: 2 * time.Second}
	expected := singleConnectionRow()
	actual, attempts, err := storage.Read(context.Background(), expected.ID)
	if err != nil || attempts != 1 {
		t.Fatalf("Read: attempts=%d err=%v", attempts, err)
	}
	if err := kv.CheckRow(expected, actual); err != nil {
		t.Fatal(err)
	}
	if attempts, err := storage.Write(context.Background(), expected); err != nil || attempts != 1 {
		t.Fatalf("Write: attempts=%d err=%v", attempts, err)
	}
	if waits := raw.Stats().WaitCount; waits != 0 {
		t.Fatalf("ORM borrowed a second connection: waits=%d", waits)
	}
}

func singleConnectionRow() generator.Row {
	text, generation, number := "payload", uint64(1), 0.25
	timestamp := time.Unix(1700000000, 123456000).UTC()

	return generator.Row{
		ID: 1, PayloadStr: &text, PayloadHash: &generation,
		PayloadDouble: &number, PayloadTimestamp: &timestamp,
	}
}

type singleConnectionConnector struct{}

func (singleConnectionConnector) Connect(context.Context) (driver.Conn, error) {
	return singleConnection{}, nil
}

func (singleConnectionConnector) Driver() driver.Driver { return singleConnectionDriver{} }

type singleConnectionDriver struct{}

func (singleConnectionDriver) Open(string) (driver.Conn, error) { return singleConnection{}, nil }

type singleConnection struct{}

func (singleConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}

func (singleConnection) Close() error { return nil }

func (singleConnection) Begin() (driver.Tx, error) { return singleConnectionTx{}, nil }

func (singleConnection) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return singleConnectionTx{}, nil
}

func (singleConnection) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (singleConnection) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &singleConnectionRows{}, nil
}

type singleConnectionTx struct{}

func (singleConnectionTx) Commit() error { return nil }

func (singleConnectionTx) Rollback() error { return nil }

type singleConnectionRows struct{ done bool }

func (*singleConnectionRows) Columns() []string {
	return []string{"id", "hash", "payload_str", "payload_hash", "payload_double", "payload_timestamp"}
}

func (*singleConnectionRows) Close() error { return nil }

func (r *singleConnectionRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	row := singleConnectionRow()
	values[0], values[1], values[2] = int64(row.ID), int64(1), *row.PayloadStr
	values[3], values[4], values[5] = int64(*row.PayloadHash), *row.PayloadDouble, *row.PayloadTimestamp

	return nil
}
