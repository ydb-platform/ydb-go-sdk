package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// CustomTime wraps time.Time with custom formatting logic
type CustomTime struct {
	time.Time
}

// CustomID represents a custom identifier type
type CustomID struct {
	ID string
}

// String returns the string representation of CustomID
func (id CustomID) String() string {
	return id.ID
}

// CustomTimeConverter converts CustomTime to YDB text value with specific formatting
type CustomTimeConverter struct{}

func (c *CustomTimeConverter) Convert(v any) (value.Value, bool) {
	if ct, ok := v.(CustomTime); ok {
		// Convert to ISO format without timezone
		return value.TextValue(ct.Format("2006-01-02 15:04:05")), true
	}
	return nil, false
}

// CustomIDConverter converts CustomID to YDB text value with prefix
type CustomIDConverter struct{}

func (c *CustomIDConverter) Convert(v any) (value.Value, bool) {
	if id, ok := v.(CustomID); ok {
		return value.TextValue("ID_" + id.ID), true
	}
	return nil, false
}

// SpecialParameterConverter handles named parameters with special names
type SpecialParameterConverter struct{}

func (c *SpecialParameterConverter) Convert(v any) (value.Value, bool) {
	// This converter doesn't handle regular values
	return nil, false
}

func (c *SpecialParameterConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
	// Handle special parameter names
	switch nv.Name {
	case "timestamp":
		if t, ok := nv.Value.(time.Time); ok {
			return value.Int64Value(t.Unix()), true
		}
	case "version":
		if s, ok := nv.Value.(string); ok {
			return value.TextValue("v" + s), true
		}
	}
	return nil, false
}

func main() {
	ctx := context.Background()

	// Create connector with custom converters
	connector, err := ydb.Connector(
		&ydb.Driver{},
		ydb.WithCustomConverter(&CustomTimeConverter{}),
		ydb.WithCustomConverter(&CustomIDConverter{}),
		ydb.WithCustomNamedValueConverter(&SpecialParameterConverter{}),
	)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}

	// Use the connector with database/sql
	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
	}()

	// Create test table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE custom_data (
			id TEXT,
			name TEXT,
			created_at TEXT,
			updated_at INT64,
			version TEXT,
			PRIMARY KEY(id)
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, `DROP TABLE custom_data`)
	}()

	// Prepare test data
	customTime := CustomTime{Time: time.Now().Truncate(time.Second)}
	customID := CustomID{ID: "12345"}

	// Insert data using custom converters
	_, err = db.ExecContext(ctx, `
		INSERT INTO custom_data (id, name, created_at, updated_at, version) 
		VALUES ($id, $name, $created_at, $timestamp, $version)
	`,
		sql.Named("id", customID),
		sql.Named("name", "Example Record"),
		sql.Named("created_at", customTime),
		sql.Named("timestamp", time.Now()),
		sql.Named("version", "1.0.0"),
	)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Query the data back
	var (
		id        string
		name      string
		createdAt string
		updatedAt int64
		version   string
	)

	err = db.QueryRowContext(ctx, `
		SELECT id, name, created_at, updated_at, version 
		FROM custom_data 
		WHERE id = $id
	`, sql.Named("id", customID)).Scan(&id, &name, &createdAt, &updatedAt, &version)
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}

	// Display results
	fmt.Printf("Retrieved record:\n")
	fmt.Printf("  ID: %s\n", id)
	fmt.Printf("  Name: %s\n", name)
	fmt.Printf("  Created At: %s\n", createdAt)
	fmt.Printf("  Updated At: %d\n", updatedAt)
	fmt.Printf("  Version: %s\n", version)

	// Verify custom conversions worked
	expectedID := "ID_" + customID.ID
	if id != expectedID {
		log.Fatalf("ID conversion failed: expected %s, got %s", expectedID, id)
	}

	expectedTime := customTime.Format("2006-01-02 15:04:05")
	if createdAt != expectedTime {
		log.Fatalf("Time conversion failed: expected %s, got %s", expectedTime, createdAt)
	}

	expectedVersion := "v1.0.0"
	if version != expectedVersion {
		log.Fatalf("Version conversion failed: expected %s, got %s", expectedVersion, version)
	}

	fmt.Println("\nCustom converter example completed successfully!")
}

// Example output:
// Retrieved record:
//   ID: ID_12345
//   Name: Example Record
//   Created At: 2023-12-07 14:30:45
//   Updated At: 1701943845
//   Version: v1.0.0
//
// Custom converter example completed successfully!
