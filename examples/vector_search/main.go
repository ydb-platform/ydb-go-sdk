// Vector search with YQL Knn UDFs — mirrors
// ydb/public/sdk/python/examples/vector_search/vector_search.py.

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	tableName = "ydb_vector_search"
	indexName = "ydb_vector_index"
)

var connectionString = flag.String("ydb", os.Getenv("YDB_CONNECTION_STRING"), "YDB connection string")

type item struct {
	id        string
	document  string
	embedding []float32
}

type searchHit struct {
	id       string
	document string
	score    float32
}

func convertVectorToBytes(vector []float32) []byte {
	buf := make([]byte, len(vector)*4+1)
	for i, v := range vector {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	buf[len(buf)-1] = 0x01

	return buf
}

func dropVectorTableIfExists(ctx context.Context, c query.Client, name string) error {
	if err := c.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", name)); err != nil {
		return err
	}
	fmt.Println("Vector table dropped")

	return nil
}

func createVectorTable(ctx context.Context, c query.Client, name string) error {
	if err := c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id Utf8,
			document Utf8,
			embedding String,
			PRIMARY KEY (id)
		);`, "`"+name+"`")); err != nil {
		return err
	}
	fmt.Println("Vector table created")

	return nil
}

func insertItemsAsBytes(ctx context.Context, c query.Client, name string, items []item) error {
	sql := fmt.Sprintf(`
		UPSERT INTO %s
		(id, document, embedding)
		SELECT id, document, embedding
		FROM AS_TABLE($items);`, "`"+name+"`")

	rows := make([]types.Value, 0, len(items))
	for _, it := range items {
		rows = append(rows, types.StructValue(
			types.StructFieldValue("id", types.UTF8Value(it.id)),
			types.StructFieldValue("document", types.UTF8Value(it.document)),
			types.StructFieldValue("embedding", types.BytesValue(convertVectorToBytes(it.embedding))),
		))
	}

	if err := c.Exec(ctx, sql, query.WithParameters(
		ydb.ParamsBuilder().Param("$items").BeginList().AddItems(rows...).EndList().Build(),
	)); err != nil {
		return err
	}
	fmt.Printf("%d items inserted\n", len(items))

	return nil
}

func addVectorIndex(
	ctx context.Context,
	c query.Client,
	name, idx, strategy string,
	dimension, levels, clusters uint32,
) error {
	tempIndexName := idx + "__temp"
	if err := c.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s
		ADD INDEX %s
		GLOBAL USING vector_kmeans_tree
		ON (embedding)
		WITH (
			%s,
			vector_type="Float",
			vector_dimension=%d,
			levels=%d,
			clusters=%d
		);`, "`"+name+"`", tempIndexName, strategy, dimension, levels, clusters)); err != nil {
		return err
	}

	if err := c.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE `%s` RENAME INDEX `%s` TO `%s`;",
		name, tempIndexName, idx,
	)); err != nil {
		return err
	}
	fmt.Printf("Table index %s created.\n", idx)

	return nil
}

func searchItemsAsBytes(
	ctx context.Context,
	c query.Client,
	name string,
	embedding []float32,
	strategy string,
	limit uint32,
	idx *string,
) ([]searchHit, error) {
	viewIndex := ""
	if idx != nil {
		viewIndex = "VIEW " + *idx
	}
	sortOrder := "ASC"
	if strings.HasSuffix(strategy, "Similarity") {
		sortOrder = "DESC"
	}

	sql := fmt.Sprintf(`
		SELECT
			id,
			document,
			Knn::%s(embedding, $embedding) AS score
		FROM %s %s
		ORDER BY score %s
		LIMIT %d;`, strategy, "`"+name+"`", viewIndex, sortOrder, limit)

	var hits []searchHit
	err := c.Do(ctx, func(ctx context.Context, s query.Session) error {
		stream, err := s.Query(ctx, sql, query.WithParameters(
			ydb.ParamsBuilder().
				Param("$embedding").
				Bytes(convertVectorToBytes(embedding)).
				Build(),
		))
		if err != nil {
			return err
		}
		defer func() { _ = stream.Close(ctx) }()

		for rs, err := range stream.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				var hit searchHit
				if err = row.ScanNamed(
					query.Named("id", &hit.id),
					query.Named("document", &hit.document),
					query.Named("score", &hit.score),
				); err != nil {
					return err
				}
				hits = append(hits, hit)
			}
		}

		return nil
	}, query.WithIdempotent())
	if err != nil {
		return nil, err
	}

	return hits, nil
}

func printResults(items []searchHit) {
	if len(items) == 0 {
		fmt.Println("No items found")

		return
	}
	for _, item := range items {
		fmt.Printf("[score=%v] %s: %s\n", item.score, item.id, item.document)
	}
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx, *connectionString, environ.WithEnvironCredentials())
	if err != nil {
		panic(fmt.Errorf("connect error: %w", err))
	}
	defer func() { _ = db.Close(ctx) }()

	qc := db.Query()

	if err = dropVectorTableIfExists(ctx, qc, tableName); err != nil {
		panic(err)
	}
	if err = createVectorTable(ctx, qc, tableName); err != nil {
		panic(err)
	}

	items := []item{
		{"1", "vector 1", []float32{0.98, 0.1, 0.01}},
		{"2", "vector 2", []float32{1.0, 0.05, 0.05}},
		{"3", "vector 3", []float32{0.9, 0.1, 0.1}},
		{"4", "vector 4", []float32{0.03, 0.0, 0.99}},
		{"5", "vector 5", []float32{0.0, 0.0, 0.99}},
		{"6", "vector 6", []float32{0.0, 0.02, 1.0}},
		{"7", "vector 7", []float32{0.0, 1.05, 0.05}},
		{"8", "vector 8", []float32{0.02, 0.98, 0.1}},
		{"9", "vector 9", []float32{0.0, 1.0, 0.05}},
	}

	if err = insertItemsAsBytes(ctx, qc, tableName, items); err != nil {
		panic(err)
	}

	hits, err := searchItemsAsBytes(ctx, qc, tableName, []float32{1, 0, 0}, "CosineSimilarity", 3, nil)
	if err != nil {
		panic(err)
	}
	printResults(hits)

	if err = addVectorIndex(ctx, qc, tableName, indexName, "similarity=cosine", 3, 1, 3); err != nil {
		panic(err)
	}

	idx := indexName
	hits, err = searchItemsAsBytes(ctx, qc, tableName, []float32{1, 0, 0}, "CosineSimilarity", 3, &idx)
	if err != nil {
		panic(err)
	}
	printResults(hits)
}
