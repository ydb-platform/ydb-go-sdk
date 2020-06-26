package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"path"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/ydbutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

const (
	DocTablePartitionCount = 4
	DeleteBatchSize        = 10
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(ctx, flag)
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
		"<html><body><h1>Ya</h1></body></html>",
		2)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://mail.yandex.ru/",
		"<html><body><h1>Mail</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://zen.yandex.ru/",
		"<html><body><h1>Zen</h1></body></html>",
		4)
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

	err = readDocument(ctx, &sp, prefix, "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = deleteExpired(ctx, &sp, prefix, 2)
	if err != nil {
		return fmt.Errorf("delete expired failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://yandex.ru/",
		"<html><body><h1>Yandex</h1></body></html>",
		3)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = addDocument(ctx, &sp, prefix,
		"https://ya.ru/",
		"<html><body><h1>Ya</h1></body></html>",
		4)
	if err != nil {
		return fmt.Errorf("add document failed: %v", err)
	}

	err = deleteExpired(ctx, &sp, prefix, 3)
	if err != nil {
		return fmt.Errorf("delete expired failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://ya.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://mail.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	err = readDocument(ctx, &sp, prefix, "https://zen.yandex.ru/")
	if err != nil {
		return fmt.Errorf("read document failed: %v", err)
	}

	return nil
}

func deleteExpiredDocuments(ctx context.Context, sp *table.SessionPool, prefix string, ids []uint64,
	timestamp uint64) error {
	fmt.Printf("> DeleteExpiredDocuments: %+v\n", ids)

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $keys AS 'List<Struct<
            doc_id: Uint64
        >>';

        DECLARE $timestamp AS Uint64;

        $expired = (
            SELECT d.doc_id AS doc_id
            FROM AS_TABLE($keys) AS k
            INNER JOIN documents AS d
            ON k.doc_id = d.doc_id
            WHERE timestamp <= $timestamp
        );

        DELETE FROM documents ON
        SELECT * FROM $expired;`, prefix)

	keys := ydb.ListValue(func() []ydb.Value {
		var k = make([]ydb.Value, len(ids))
		for i := range ids {
			k[i] = ydb.StructValue(ydb.StructFieldValue("doc_id", ydb.Uint64Value(ids[i])))
		}
		return k
	}()...)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query,
				table.NewQueryParameters(
					table.ValueParam("$keys", keys),
					table.ValueParam("$timestamp", ydb.Uint64Value(timestamp)),
				),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache()))
			return err
		}))
}

func deleteExpiredRange(ctx context.Context, sp *table.SessionPool, prefix string, timestamp uint64,
	keyRange table.KeyRange) error {
	fmt.Printf("> DeleteExpiredRange: %+v\n", keyRange)

	var res *table.Result
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path.Join(prefix, "documents"),
				table.ReadKeyRange(keyRange),
				table.ReadColumn("doc_id"),
				table.ReadColumn("timestamp"))
			return err
		}),
	)
	if err != nil {
		return err
	}
	if err = res.Err(); err != nil {
		return err
	}

	// As single key range usually represents a single shard, so we batch deletions here
	// without introducing distributed transactions.
	var docIds []uint64
	for res.NextStreamSet(ctx) {
		for res.NextRow() {
			res.SeekItem("doc_id")
			docID := res.OUint64()

			res.SeekItem("timestamp")
			rowTimestamp := res.OUint64()

			if rowTimestamp <= timestamp {
				docIds = append(docIds, docID)
			}
			if len(docIds) >= DeleteBatchSize {
				if err := deleteExpiredDocuments(ctx, sp, prefix, docIds, timestamp); err != nil {
					return err
				}
				docIds = []uint64{}
			}
		}
		if len(docIds) > 0 {
			if err := deleteExpiredDocuments(ctx, sp, prefix, docIds, timestamp); err != nil {
				return err
			}
			docIds = []uint64{}
		}
	}

	return nil
}

func deleteExpired(ctx context.Context, sp *table.SessionPool, prefix string, timestamp uint64) error {
	fmt.Printf("> DeleteExpired: timestamp: %v:\n", timestamp)

	var res table.Description
	err := table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.DescribeTable(ctx, path.Join(prefix, "documents"), table.WithShardKeyBounds())
			return err
		}),
	)

	if err != nil {
		return err
	}
	for _, kr := range res.KeyRanges {
		// DeleteExpiredRange can be run in parallel for different ranges.
		// Keep in mind that deletion RPS should be somehow limited in this case to avoid
		// spikes of cluster load due to TTL.
		err = deleteExpiredRange(ctx, sp, prefix, timestamp, kr)
		if err != nil {
			return err
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
			_, res, err = s.Execute(ctx, readTx, query, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url))),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache()))
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

	query := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $url AS Utf8;
        DECLARE $html AS Utf8;
        DECLARE $timestamp AS Uint64;

        $doc_id = Digest::CityHash($url);

        REPLACE INTO documents
            (doc_id, url, html, timestamp)
        VALUES
            ($doc_id, $url, $html, $timestamp);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$url", ydb.UTF8Value(url)),
				table.ValueParam("$html", ydb.UTF8Value(html)),
				table.ValueParam("$timestamp", ydb.Uint64Value(timestamp))),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache()))
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

	return nil
}
