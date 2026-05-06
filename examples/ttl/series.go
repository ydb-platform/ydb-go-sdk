package main

import (
	"context"
	"fmt"
	"math/rand"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	docTablePartitionCount = 4
	expirationQueueCount   = 4
)

func readExpiredBatchTransaction(ctx context.Context, c query.Client, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64) (query.ClosableResultSet,
	error,
) {
	sql := fmt.Sprintf(`
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

	var rs query.ClosableResultSet
	err := c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			rs, err = s.QueryResultSet(ctx, sql,
				query.WithTxControl(query.OnlineReadOnlyTxControl()),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$timestamp").Any(types.Uint64Value(timestamp)).
						Param("$prev_timestamp").Any(types.Uint64Value(prevTimestamp)).
						Param("$prev_doc_id").Any(types.Uint64Value(prevDocID)).
						Build(),
				),
			)

			return err
		},
	)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func deleteDocumentWithTimestamp(ctx context.Context,
	c query.Client, prefix string, queue, lastDocID, timestamp uint64,
) error {
	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $doc_id AS Uint64;
        DECLARE $timestamp AS Uint64;

        DELETE FROM documents
        WHERE doc_id = $doc_id AND ts = $timestamp;

        DELETE FROM expiration_queue_%v
        WHERE ts = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	return c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			return s.Exec(ctx, sql,
				query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$doc_id").Any(types.Uint64Value(lastDocID)).
						Param("$timestamp").Any(types.Uint64Value(timestamp)).
						Build(),
				),
			)
		},
	)
}

func deleteExpired(ctx context.Context, c query.Client, prefix string, queue, timestamp uint64) (err error) {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		err = func() (err error) { // for isolate defer inside lambda
			rs, err := readExpiredBatchTransaction(ctx, c, prefix, queue, timestamp, lastTimestamp, lastDocID)
			if err != nil {
				return err
			}
			defer func() {
				_ = rs.Close(ctx)
			}()

			hasRows := false
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				hasRows = true
				err = row.ScanNamed(
					query.Named("doc_id", &lastDocID),
					query.Named("ts", &lastTimestamp),
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

			if !hasRows {
				empty = true
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func readDocument(ctx context.Context, c query.Client, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

        DECLARE $url AS Text;

        $doc_id = Digest::CityHash($url);

        SELECT doc_id, url, html, ts
        FROM documents
        WHERE doc_id = $doc_id;`, prefix)

	return c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			rs, err := s.QueryResultSet(ctx, sql,
				query.WithTxControl(query.OnlineReadOnlyTxControl()),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$url").Any(types.TextValue(url)).
						Build(),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = rs.Close(ctx)
			}()

			found := false
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				found = true
				var (
					docID  *uint64
					docURL *string
					ts     *uint64
					html   *string
				)
				err = row.ScanNamed(
					query.Named("doc_id", &docID),
					query.Named("url", &docURL),
					query.Named("ts", &ts),
					query.Named("html", &html),
				)
				if err != nil {
					return err
				}
				fmt.Printf("\tDocId: %v\n", docID)
				fmt.Printf("\tUrl: %v\n", docURL)
				fmt.Printf("\tTimestamp: %v\n", ts)
				fmt.Printf("\tHtml: %v\n", html)
			}

			if !found {
				fmt.Println("\tNot found")
			}

			return nil
		},
	)
}

func addDocument(ctx context.Context, c query.Client, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(expirationQueueCount) //nolint:gosec
	sql := fmt.Sprintf(`
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

	return c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			return s.Exec(ctx, sql,
				query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$url").Any(types.TextValue(url)).
						Param("$html").Any(types.TextValue(html)).
						Param("$timestamp").Any(types.Uint64Value(timestamp)).
						Build(),
				),
			)
		},
	)
}

func createTables(ctx context.Context, c query.Client, prefix string) (err error) {
	err = c.Exec(ctx,
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
				doc_id Uint64,
				url Text,
				html Text,
				ts Uint64,
				PRIMARY KEY (doc_id)
			)`, path.Join(prefix, "documents")),
		query.WithTxControl(query.ImplicitTxControl()),
	)
	if err != nil {
		return err
	}

	for i := range expirationQueueCount {
		tableName := path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i))
		err = c.Exec(ctx,
			fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
					doc_id Uint64,
					ts Uint64,
					PRIMARY KEY (ts, doc_id)
				)`, tableName),
			query.WithTxControl(query.ImplicitTxControl()),
		)
		if err != nil {
			return err
		}
	}

	return nil
}
