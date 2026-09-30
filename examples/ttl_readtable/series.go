package main

import (
	"context"
	"errors"
	"fmt"
	"path"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

const (
	docTablePartitionCount = 4
	deleteBatchSize        = 10
)

func deleteExpiredDocuments(ctx context.Context, c query.Client, prefix string, ids []uint64,
	timestamp uint64,
) error {
	fmt.Printf("> DeleteExpiredDocuments: %+v\n", ids)

	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		$expired = (
			SELECT d.doc_id AS doc_id
			FROM AS_TABLE($keys) AS k
			INNER JOIN documents AS d
			ON k.doc_id = d.doc_id
			WHERE ts <= $timestamp
		);

		DELETE FROM documents ON
		SELECT * FROM $expired;`, prefix)

	keys := types.ListValue(func() []types.Value {
		values := make([]types.Value, len(ids))
		for i := range ids {
			values[i] = types.StructValue(types.StructFieldValue("doc_id", types.Uint64Value(ids[i])))
		}

		return values
	}()...)

	return c.Exec(ctx, sql,
		query.WithParameters(ydb.ParamsBuilder().
			Param("$keys").Any(keys).
			Param("$timestamp").Uint64(timestamp).
			Build()),
		query.WithIdempotent(),
	)
}

func deleteExpiredRange(ctx context.Context, tableClient table.Client, queryClient query.Client,
	prefix string, timestamp uint64, keyRange options.KeyRange,
) error {
	fmt.Printf("> DeleteExpiredRange: %+v\n", keyRange)

	return tableClient.Do(ctx, func(ctx context.Context, session table.Session) error {
		res, err := session.StreamReadTable(ctx, path.Join(prefix, "documents"),
			options.ReadKeyRange(keyRange),
			options.ReadColumn("doc_id"),
			options.ReadColumn("ts"))
		if err != nil {
			return err
		}
		defer func() { _ = res.Close() }()

		// As a single key range usually represents a single shard, batch deletions here
		// without introducing distributed transactions.
		var (
			docIDs []uint64
			docID  uint64
			ts     uint64
		)
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				if err = res.ScanNamed(
					named.OptionalWithDefault("doc_id", &docID),
					named.OptionalWithDefault("ts", &ts),
				); err != nil {
					return err
				}

				if ts <= timestamp {
					docIDs = append(docIDs, docID)
				}
				if len(docIDs) >= deleteBatchSize {
					if err = deleteExpiredDocuments(ctx, queryClient, prefix, docIDs, timestamp); err != nil {
						return err
					}
					docIDs = docIDs[:0]
				}
			}
			if len(docIDs) > 0 {
				if err = deleteExpiredDocuments(ctx, queryClient, prefix, docIDs, timestamp); err != nil {
					return err
				}
				docIDs = docIDs[:0]
			}
		}

		return res.Err()
	}, table.WithIdempotent())
}

func deleteExpired(ctx context.Context, tableClient table.Client, queryClient query.Client,
	prefix string, timestamp uint64,
) error {
	fmt.Printf("> DeleteExpired: timestamp: %v:\n", timestamp)

	description, err := tableClient.DescribeTable(ctx, path.Join(prefix, "documents"), options.WithShardKeyBounds())
	if err != nil {
		return err
	}
	for i := range description.KeyRanges {
		// DeleteExpiredRange can be run in parallel for different ranges.
		// Keep in mind that deletion RPS should be somehow limited in this case to avoid
		// spikes of cluster load due to TTL.
		if err = deleteExpiredRange(
			ctx, tableClient, queryClient, prefix, timestamp, description.KeyRanges[i],
		); err != nil {
			return err
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

	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		$doc_id = Digest::CityHash($url);

		REPLACE INTO documents
			(doc_id, url, html, ts)
		VALUES
			($doc_id, $url, $html, $timestamp);`, prefix)

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
	return c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			doc_id Uint64,
			url Text,
			html Text,
			ts Uint64,
			PRIMARY KEY (doc_id)
		) WITH (
			UNIFORM_PARTITIONS = %d
		)`, "`"+path.Join(prefix, "documents")+"`", docTablePartitionCount), query.WithIdempotent())
}
