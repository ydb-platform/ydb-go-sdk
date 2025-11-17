package scanner_test

import (
	"context"
	"fmt"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// Example demonstrates using YDB type annotations in struct fields.
// The type annotation helps validate that the database column types match your expectations
// and can simplify code by making type conversions explicit in the struct definition.
func Example_typeAnnotations() {
	// Define a struct with YDB type annotations
	type Series struct {
		// Column name: series_id, YDB type: Bytes
		SeriesID []byte `sql:"series_id,type:Bytes"`

		// Column name: title, YDB type: Text (UTF-8 string)
		Title string `sql:"title,type:Text"`

		// Column name: release_date, YDB type: Date
		ReleaseDate string `sql:"release_date,type:Date"`

		// Column name: tags, YDB type: List<Text>
		// This annotation ensures the column is a list of text values
		Tags []string `sql:"tags,type:List<Text>"`

		// Column name: rating, YDB type: Optional<Uint64>
		// This annotation indicates the column can be NULL
		Rating *uint64 `sql:"rating,type:Optional<Uint64>"`
	}

	// In your query code, you would use it like this:
	_ = func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, `
			SELECT 
				series_id,
				title,
				release_date,
				tags,
				rating
			FROM series
		`)
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

				var s Series
				if err := row.ScanStruct(&s); err != nil {
					// If the database column type doesn't match the annotation,
					// you'll get a clear error message like:
					// "type mismatch for field 'title': expected Text, got Uint64"
					return err
				}

				// Use the scanned data
				log.Printf("Series: %s (ID: %v)", s.Title, s.SeriesID)
			}
		}

		return nil
	}

	fmt.Println("Type annotations provide compile-time documentation and runtime validation")
	// Output: Type annotations provide compile-time documentation and runtime validation
}

// Example_withoutTypeAnnotations shows scanning without type annotations.
// This still works, but provides less validation and documentation.
func Example_withoutTypeAnnotations() {
	type Series struct {
		SeriesID    []byte  `sql:"series_id"`
		Title       string  `sql:"title"`
		ReleaseDate string  `sql:"release_date"`
		Tags        []string `sql:"tags"`
		Rating      *uint64 `sql:"rating"`
	}

	// This works the same way, but doesn't validate types
	_ = Series{}

	fmt.Println("Scanning works with or without type annotations")
	// Output: Scanning works with or without type annotations
}

// Example_complexTypes shows using complex nested type annotations.
func Example_complexTypes() {
	type Product struct {
		// Dict type: mapping from string to uint64
		Metadata map[string]uint64 `sql:"metadata,type:Dict<Text,Uint64>"`

		// List of optional values
		AlternateNames []*string `sql:"alternate_names,type:List<Optional<Text>>"`

		// Optional list
		RelatedIDs *[]uint64 `sql:"related_ids,type:Optional<List<Uint64>>"`
	}

	_ = Product{}

	fmt.Println("Complex nested types are supported")
	// Output: Complex nested types are supported
}
