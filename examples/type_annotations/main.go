package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

var connectionString = flag.String("ydb", os.Getenv("YDB_CONNECTION_STRING"), "YDB connection string")

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flag.Parse()

	if *connectionString == "" {
		log.Fatal("YDB_CONNECTION_STRING environment variable or -ydb flag must be set")
	}

	db, err := ydb.Open(ctx, *connectionString,
		environ.WithEnvironCredentials(),
	)
	if err != nil {
		log.Fatalf("failed to connect to YDB: %v", err)
	}
	defer func() { _ = db.Close(ctx) }()

	prefix := path.Join(db.Name(), "type_annotations_example")

	// Clean up any existing data
	_ = sugar.RemoveRecursive(ctx, db, prefix)

	// Create table
	if err := createTable(ctx, db.Query(), prefix); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert data
	if err := insertData(ctx, db.Query(), prefix); err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Read data with type annotations
	if err := readDataWithTypeAnnotations(ctx, db.Query(), prefix); err != nil {
		log.Fatalf("failed to read data: %v", err)
	}

	// Demonstrate type mismatch detection
	if err := demonstrateTypeMismatch(ctx, db.Query(), prefix); err != nil {
		log.Printf("Expected error (type mismatch): %v", err)
	}

	// Clean up
	_ = sugar.RemoveRecursive(ctx, db, prefix)

	fmt.Println("\nExample completed successfully!")
}

func createTable(ctx context.Context, c query.Client, prefix string) error {
	return c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			product_id Uint64,
			name Text,
			description Text,
			price Uint64,
			tags List<Text>,
			rating Optional<Double>,
			metadata Dict<Text, Text>,
			PRIMARY KEY (product_id)
		)`, "`"+path.Join(prefix, "products")+"`"),
		query.WithTxControl(query.NoTx()),
	)
}

func insertData(ctx context.Context, c query.Client, prefix string) error {
	return c.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (product_id, name, description, price, tags, rating)
		VALUES (
			1,
			"Laptop",
			"High-performance laptop",
			999,
			["electronics", "computer", "portable"],
			4.5
		);
	`, "`"+path.Join(prefix, "products")+"`"))
}

// Product demonstrates struct with YDB type annotations
type Product struct {
	// Basic types with annotations
	ProductID   uint64  `sql:"product_id,type:Uint64"`
	Name        string  `sql:"name,type:Text"`
	Description string  `sql:"description,type:Text"`
	Price       uint64  `sql:"price,type:Uint64"`
	
	// List type annotation
	Tags        []string `sql:"tags,type:List<Text>"`
	
	// Optional type annotation (can be NULL)
	Rating      *float64 `sql:"rating,type:Optional<Double>"`
}

func readDataWithTypeAnnotations(ctx context.Context, c query.Client, prefix string) error {
	fmt.Println("\n=== Reading data with type annotations ===")
	
	return c.Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, fmt.Sprintf(`
			SELECT product_id, name, description, price, tags, rating
			FROM %s
		`, "`"+path.Join(prefix, "products")+"`"),
			query.WithTxControl(query.TxControl(query.BeginTx(query.WithSnapshotReadOnly()))),
		)
		if err != nil {
			return err
		}
		defer result.Close(ctx)

		for rs, err := range result.ResultSets(ctx) {
			if err != nil {
				return err
			}

			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}

				var product Product
				
				// ScanStruct will validate that database column types match the annotations
				if err := row.ScanStruct(&product); err != nil {
					return fmt.Errorf("scan error: %w", err)
				}

				fmt.Printf("\nProduct ID: %d\n", product.ProductID)
				fmt.Printf("Name: %s\n", product.Name)
				fmt.Printf("Description: %s\n", product.Description)
				fmt.Printf("Price: $%d\n", product.Price)
				fmt.Printf("Tags: %v\n", product.Tags)
				if product.Rating != nil {
					fmt.Printf("Rating: %.1f/5.0\n", *product.Rating)
				} else {
					fmt.Println("Rating: Not rated")
				}
			}
		}

		return nil
	})
}

// ProductWrongType demonstrates what happens when type annotations don't match
type ProductWrongType struct {
	ProductID uint64  `sql:"product_id,type:Text"` // Wrong! Should be Uint64
	Name      string  `sql:"name,type:Text"`
}

func demonstrateTypeMismatch(ctx context.Context, c query.Client, prefix string) error {
	fmt.Println("\n=== Demonstrating type mismatch detection ===")
	
	return c.Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, fmt.Sprintf(`
			SELECT product_id, name
			FROM %s
		`, "`"+path.Join(prefix, "products")+"`"),
			query.WithTxControl(query.TxControl(query.BeginTx(query.WithSnapshotReadOnly()))),
		)
		if err != nil {
			return err
		}
		defer result.Close(ctx)

		for rs, err := range result.ResultSets(ctx) {
			if err != nil {
				return err
			}

			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}

				var product ProductWrongType
				
				// This will fail because product_id is Uint64 in the database
				// but the annotation says it should be Text
				if err := row.ScanStruct(&product); err != nil {
					return fmt.Errorf("type mismatch detected: %w", err)
				}
			}
		}

		return nil
	})
}
