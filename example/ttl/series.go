package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"path"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/ydbutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

const (
	DocTablePartitionCount = 4
	ExpirationQueueCount   = 4
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
}

func (cmd *Command) ExportFlags(flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(flag)
	cmd.tls = cli.ExportTLSConfig(flag)
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	dialer := &ydb.Dialer{
		DriverConfig: cmd.config(params),
		TLSConfig:    cmd.tls(),
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, params.Endpoint)
	if err != nil {
		return fmt.Errorf("dial error: %v", err)
	}

	tableClient := table.Client{
		Driver: driver,
	}
	sp := table.SessionPool{
		IdleThreshold: time.Second,
		Builder:       &tableClient,
	}
	defer sp.Close(ctx)

	err = ydbutil.CleanupDatabase(ctx, driver, &sp, params.Database)
	if err != nil {
		return err
	}
	err = ydbutil.EnsurePathExists(ctx, driver, params.Database, params.Path)
	if err != nil {
		return err
	}

	prefix := path.Join(params.Database, params.Path)

	err = createTables(ctx, &sp, prefix)
	if err != nil {
		return fmt.Errorf("create tables error: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		1)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://ya.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}
	err = readDocument(ctx, &sp, prefix, "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, &sp, prefix, i, 1); err != nil {
			return fmt.Errorf("delete expired failed: %v", err)
		}
	}

	err = readDocument(ctx, &sp, prefix, "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	for i := uint64(0); i < ExpirationQueueCount; i++ {
		if err = deleteExpired(ctx, &sp, prefix, i, 2); err != nil {
			return fmt.Errorf("delete expired failed: %v", err)
		}
	}

	err = readDocument(ctx, &sp, prefix, "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}
	err = readDocument(ctx, &sp, prefix, "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	return nil
}

func readExpiredBatchTransaction(ctx context.Context, sp *table.SessionPool, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64) (*table.Result,
	error) {

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            SELECT *
            FROM expiration_queue_%v
            WHERE
                timestamp <= $timestamp
                AND
                timestamp > $prev_timestamp
            ORDER BY timestamp, doc_id
            LIMIT 100

            UNION ALL

            SELECT *
            FROM expiration_queue_%v
            WHERE
                timestamp = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY timestamp, doc_id
            LIMIT 100
        );

        SELECT timestamp, doc_id
        FROM $data
        ORDER BY timestamp, doc_id
        LIMIT 100;`, prefix, queue, queue)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
				table.ValueParam("$prev_timestamp", ydb.Uint64Value(prevTimestamp)),
				table.ValueParam("$prev_doc_id", ydb.Uint64Value(prevDocID)),
			))
			return err
		}),
	)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res, nil
}

func deleteDocumentWithTimestamp(ctx context.Context, sp *table.SessionPool, prefix string, queue, lastDocID, timestamp uint64) error {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND timestamp = $timestamp;

        DELETE FROM expiration_queue_%v
        WHERE timestamp = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$doc_id", ydb.Uint64Value(lastDocID)),
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
			))
			return err
		}),
	)
}

func deleteExpired(ctx context.Context, sp *table.SessionPool, prefix string, queue, timestamp uint64) error {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		res, err := readExpiredBatchTransaction(ctx, sp, prefix, queue, timestamp, lastTimestamp, lastDocID)
		if err != nil {
			return err
		}

		empty = true
		res.NextSet()
		for res.NextRow() {
			empty = false
			res.SeekItem("doc_id")
			lastDocID = res.OUint64()
			res.SeekItem("timestamp")
			lastTimestamp = res.OUint64()
			fmt.Printf("\tDocId: %d\n\tTimestamp: %d\n", lastDocID, lastTimestamp)

			err = deleteDocumentWithTimestamp(ctx, sp, prefix, queue, lastDocID, lastTimestamp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func readDocument(ctx context.Context, sp *table.SessionPool, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $url AS Utf8;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, timestamp
        FROM documents
        WHERE doc_id = $doc_id;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, readTx, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url)),
			))
			return err
		}),
	)
	if err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	if res.NextSet() && res.NextRow() {
		res.SeekItem("doc_id")
		fmt.Printf("\tDocId: %v\n", res.OUint64())

		res.SeekItem("url")
		fmt.Printf("\tUrl: %v\n", res.OUTF8())

		res.SeekItem("timestamp")
		fmt.Printf("\tTimestamp: %v\n", res.OUint64())

		res.SeekItem("html")
		fmt.Printf("\tHtml: %v\n", res.OUTF8())
	} else {
		fmt.Println("\tNot found")
	}

	return nil
}

func addDocument(ctx context.Context, sp *table.SessionPool, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(ExpirationQueueCount)
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, timestamp)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_%v
            (timestamp, doc_id)
        VALUES
            ($timestamp, $doc_id);`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, query)
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url)),
				table.ValueParam("$html", ydb.UTF8Value(html)),
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
			))
			return err
		}),
	)
}

func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "documents"),
				table.WithColumn("doc_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("url", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("html", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("timestamp", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("doc_id"),
				table.WithProfile(
					table.WithPartitioningPolicy(
						table.WithPartitioningPolicyUniformPartitions(uint64(DocTablePartitionCount)))),
			)
		}),
	)
	if err != nil {
		return err
	}

	for i := 0; i < ExpirationQueueCount; i++ {
		err = table.Retry(ctx, sp,
			table.OperationFunc(func(ctx context.Context, s *table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i)),
					table.WithColumn("doc_id", ydb.Optional(ydb.TypeUint64)),
					table.WithColumn("timestamp", ydb.Optional(ydb.TypeUint64)),
					table.WithPrimaryKeyColumn("timestamp", "doc_id"),
				)
			}),
		)
		if err != nil {
			return err
		}
	}

	return nil
}
