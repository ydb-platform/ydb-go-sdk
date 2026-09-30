package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const (
	docTablePartitionCount = 4
	expirationQueueCount   = 4
)

type expiredDocument struct {
	docID     uint64
	timestamp uint64
}

func readExpiredBatch(ctx context.Context, c query.Client, prefix string, queue,
	timestamp, prevTimestamp, prevDocID uint64,
) ([]expiredDocument, error) {
	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

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

	var documents []expiredDocument
	err := c.Do(ctx, func(ctx context.Context, session query.Session) error {
		res, err := session.Query(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
			Param("$timestamp").Uint64(timestamp).
			Param("$prev_timestamp").Uint64(prevTimestamp).
			Param("$prev_doc_id").Uint64(prevDocID).
			Build()))
		if err != nil {
			return err
		}
		defer func() { _ = res.Close(ctx) }()

		var attemptDocuments []expiredDocument
		for resultSet, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range resultSet.Rows(ctx) {
				if err != nil {
					return err
				}
				var document expiredDocument
				if err = row.ScanNamed(
					query.Named("doc_id", &document.docID),
					query.Named("ts", &document.timestamp),
				); err != nil {
					return err
				}
				attemptDocuments = append(attemptDocuments, document)
			}
		}
		documents = attemptDocuments

		return nil
	}, query.WithIdempotent())
	if err != nil {
		return nil, err
	}

	return documents, nil
}

func deleteDocumentWithTimestamp(ctx context.Context,
	c query.Client, prefix string, queue, lastDocID, timestamp uint64,
) error {
	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DELETE FROM documents
		WHERE doc_id = $doc_id AND ts = $timestamp;

		DELETE FROM expiration_queue_%v
		WHERE ts = $timestamp AND doc_id = $doc_id;`, prefix, queue)

	return c.Exec(ctx, sql,
		query.WithParameters(ydb.ParamsBuilder().
			Param("$doc_id").Uint64(lastDocID).
			Param("$timestamp").Uint64(timestamp).
			Build()),
		query.WithIdempotent(),
	)
}

func deleteExpired(ctx context.Context, c query.Client, prefix string, queue, timestamp uint64) error {
	fmt.Printf("> DeleteExpired from queue #%d:\n", queue)
	empty := false
	lastTimestamp := uint64(0)
	lastDocID := uint64(0)

	for !empty {
		documents, err := readExpiredBatch(ctx, c, prefix, queue, timestamp, lastTimestamp, lastDocID)
		if err != nil {
			return err
		}
		empty = len(documents) == 0
		for _, document := range documents {
			lastDocID = document.docID
			lastTimestamp = document.timestamp
			fmt.Printf("\tDocId: %d\n\tTimestamp: %d\n", lastDocID, lastTimestamp)
			if err = deleteDocumentWithTimestamp(ctx, c, prefix, queue, lastDocID, lastTimestamp); err != nil {
				return err
			}
		}
	}

	return nil
}

func readDocument(ctx context.Context, c query.Client, prefix, url string) error {
	fmt.Printf("> ReadDocument \"%v\":\n", url)

	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		$doc_id = Digest::CityHash($url);

		SELECT doc_id, url, html, ts
		FROM documents
		WHERE doc_id = $doc_id;`, prefix)

	var (
		docID  *uint64
		docURL *string
		ts     *uint64
		html   *string
	)
	err := c.Do(ctx, func(ctx context.Context, session query.Session) error {
		row, err := session.QueryRow(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
			Param("$url").Text(url).
			Build()))
		if errors.Is(err, query.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		return row.ScanNamed(
			query.Named("doc_id", &docID),
			query.Named("url", &docURL),
			query.Named("ts", &ts),
			query.Named("html", &html),
		)
	}, query.WithIdempotent())
	if err != nil {
		return err
	}
	if docID == nil {
		fmt.Println("\tNot found")

		return nil
	}
	fmt.Printf("\tDocId: %v\n", docID)
	fmt.Printf("\tUrl: %v\n", docURL)
	fmt.Printf("\tTimestamp: %v\n", ts)
	fmt.Printf("\tHtml: %v\n", html)

	return nil
}

func addDocument(ctx context.Context, c query.Client, prefix, url, html string, timestamp uint64) error {
	fmt.Printf("> AddDocument: \n\tUrl: %v\n\tTimestamp: %v\n", url, timestamp)

	queue := rand.Intn(expirationQueueCount) //nolint:gosec
	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		$doc_id = Digest::CityHash($url);

		REPLACE INTO documents
			(doc_id, url, html, ts)
		VALUES
			($doc_id, $url, $html, $timestamp);

		REPLACE INTO expiration_queue_%v
			(ts, doc_id)
		VALUES
			($timestamp, $doc_id);`, prefix, queue)

	return c.Exec(ctx, sql,
		query.WithParameters(ydb.ParamsBuilder().
			Param("$url").Text(url).
			Param("$html").Text(html).
			Param("$timestamp").Uint64(timestamp).
			Build()),
		query.WithIdempotent(),
	)
}

func createTables(ctx context.Context, c query.Client, prefix string) error {
	err := c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			doc_id Uint64,
			url Text,
			html Text,
			ts Uint64,
			PRIMARY KEY (doc_id)
		) WITH (
			UNIFORM_PARTITIONS = %d
		)`, "`"+path.Join(prefix, "documents")+"`", docTablePartitionCount), query.WithIdempotent())
	if err != nil {
		return err
	}

	for i := range expirationQueueCount {
		tablePath := path.Join(prefix, fmt.Sprintf("expiration_queue_%v", i))
		err = c.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				doc_id Uint64,
				ts Uint64,
				PRIMARY KEY (ts, doc_id)
			)`, "`"+tablePath+"`"), query.WithIdempotent())
		if err != nil {
			return err
		}
	}

	return nil
}
