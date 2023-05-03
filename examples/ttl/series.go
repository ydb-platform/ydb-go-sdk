package main

import (
	"context"
	"fmt"
	"math/rand"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	docTablePartitionCount = 4
	expirationQueueCount   = 4
)

func readExpiredBatchTransaction(ctx context.Context, c table.Client, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64) (result.Result,
	error,
) {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $timestamp AS Uint64;
        DECLARE $prev_timestamp AS Uint64;
        DECLARE $prev_doc_id AS Uint64;

        $data = (
            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts <= $timestamp
                AND
                ts > $prev_timestamp

            UNION ALL

            SELECT *
            FROM expiration_queue_%v
            WHERE
                ts = $prev_timestamp AND doc_id > $prev_doc_id
            ORDER BY ts, doc_id
            LIMIT 100
        );

        SELECT ts, doc_id
        FROM $data
        ORDER BY ts, doc_id
        LIMIT 100;`, prefix, queue, queue)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res result.Result
	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query, table.NewQueryParameters(
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
				table.ValueParam("$prev_timestamp", types.Uint64Value(prevTimestamp)),
				table.ValueParam("$prev_doc_id", types.Uint64Value(prevDocID)),
			))
			return err
		},
	)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res, nil
}

func deleteDocumentWithTimestamp(ctx context.Context,
	c table.Client, prefix string, queue, lastDocID, timestamp uint64,
) error {
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND ts = $timestamp;

        DELETE FROM expiration_queue_%v
        WHERE ts = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$doc_id", types.Uint64Value(lastDocID)),
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
			))
			return err
		},
	)
	return err
}

func deleteExpired(ctx context.Context, c table.Client, prefix string, queue, timestamp uint64) (err error) {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		err = func() (err error) { // for isolate defer inside lambda
			res, err := readExpiredBatchTransaction(ctx, c, prefix, queue, timestamp, lastTimestamp, lastDocID)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			empty = true
			res.NextResultSet(ctx)
			for res.NextRow() {
				empty = false
				err = res.ScanNamed(
					named.OptionalWithDefault("doc_id", &lastDocID),
					named.OptionalWithDefault("ts", &lastTimestamp),
				)
				if err != nil {
					return err
				}
				fmt.Printf("\tDocId: %d\n\tTimestamp: %d\n", lastDocID, lastTimestamp)

				err = deleteDocumentWithTimestamp(ctx, c, prefix, queue, lastDocID, lastTimestamp)
				if err != nil {
					return err
				}
			}
			return res.Err()
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func readDocument(ctx context.Context, c table.Client, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $url AS Text;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, ts
        FROM documents
        WHERE doc_id = $doc_id;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err := s.Execute(ctx, readTx, query, table.NewQueryParameters(
				table.ValueParam("$url", types.TextValue(url)),
			))
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			var (
				docID  *uint64
				docURL *string
				ts     *uint64
				html   *string
			)
			if res.NextResultSet(ctx) && res.NextRow() {
				err = res.ScanNamed(
					named.Optional("doc_id", &docID),
					named.Optional("url", &docURL),
					named.Optional("ts", &ts),
					named.Optional("html", &html),
				)
				if err != nil {
					return err
				}
				fmt.Printf("\tDocId: %v\n", docID)
				fmt.Printf("\tUrl: %v\n", docURL)
				fmt.Printf("\tTimestamp: %v\n", ts)
				fmt.Printf("\tHtml: %v\n", html)
			} else {
				fmt.Println("\tNot found")
			}
			return res.Err()
		},
	)
	return err
}

func addDocument(ctx context.Context, c table.Client, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(expirationQueueCount) //nolint:gosec
	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Text;
        DECLARE $html AS Text;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, ts)
        VALUES
            ($doc_id, $url, $html, $timestamp);

        REPLACE INTO expiration_queue_%v
            (ts, doc_id)
        VALUES
            ($timestamp, $doc_id);`, prefix, queue)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err := c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$url", types.TextValue(url)),
				table.ValueParam("$html", types.TextValue(html)),
				table.ValueParam("$timestamp", types.Uint64Value(timestamp)),
			))
			return err
		},
	)
	return err
}

func createTables(ctx context.Context, c table.Client, prefix string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "documents"),
				options.WithColumn("doc_id", types.Optional(types.TypeUint64)),
				options.WithColumn("url", types.Optional(types.TypeUTF8)),
				options.WithColumn("html", types.Optional(types.TypeUTF8)),
				options.WithColumn("ts", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("doc_id"),
				options.WithPartitions(options.WithUniformPartitions(uint64(docTablePartitionCount))),
			)
		},
	)
	if err != nil {
		return err
	}

	for i := 0; i < expirationQueueCount; i++ {
		err = c.Do(ctx,
			func(ctx context.Context, s table.Session) error {
				return s.CreateTable(ctx, path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i)),
					options.WithColumn("doc_id", types.Optional(types.TypeUint64)),
					options.WithColumn("ts", types.Optional(types.TypeUint64)),
					options.WithPrimaryKeyColumn("ts", "doc_id"),
				)
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
