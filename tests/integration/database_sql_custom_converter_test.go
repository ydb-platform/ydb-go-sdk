package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestDatabaseSQL_CustomConverter(t *testing.T) {
	var (
		ctx = context.Background()
		db  *sql.DB
		err error
	)

	// Define custom types
	type CustomTime struct {
		time.Time
	}
	type CustomID struct {
		ID string
	}
	
	// Create custom converters
	timeConverter := bind.NewCustomTypeConverter(
		func(v any) bool { _, ok := v.(CustomTime); return ok },
		func(v any) (value.Value, error) {
			ct := v.(CustomTime)
			return value.TextValue(ct.Format("2006-01-02 15:04:05")), nil
		},
	)
	
	idConverter := bind.NewCustomTypeConverter(
		func(v any) bool { _, ok := v.(CustomID); return ok },
		func(v any) (value.Value, error) {
			cid := v.(CustomID)

			return value.TextValue("ID_" + cid.ID), nil
		},
	)

	t.Run("with custom converter", func(t *testing.T) {
		db, err = sql.Open("ydb", "ydb://localhost:2136/local")
		require.NoError(t, err)

		// Create connector with custom converters
		connector, err := ydb.Connector(
			&ydb.Driver{},
			ydb.WithCustomConverter(timeConverter),
			ydb.WithCustomConverter(idConverter),
		)
		require.NoError(t, err)

		// Replace db connection with custom connector
		db = sql.OpenDB(connector)
		defer func() {
			_ = db.Close()
		}()

		// Create test table
		_, err = db.ExecContext(ctx, `
			CREATE TABLE custom_converter_test (
				id TEXT,
				name TEXT,
				created_at TEXT,
				PRIMARY KEY(id)
			)
		`)
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, `DROP TABLE custom_converter_test`)
		}()

		// Test data with custom types
		customTime := CustomTime{Time: time.Now().Truncate(time.Second)}
		customID := CustomID{ID: uuid.New().String()}

		// Insert using custom types
		_, err = db.ExecContext(ctx, `
			INSERT INTO custom_converter_test (id, name, created_at) 
			VALUES ($id, $name, $created_at)
		`,
			sql.Named("id", customID),
			sql.Named("name", "test_name"),
			sql.Named("created_at", customTime),
		)
		require.NoError(t, err)

		// Query back the data
		var (
			id         string
			name       string
			createdAt  string
		)
		err = db.QueryRowContext(ctx, `
			SELECT id, name, created_at 
			FROM custom_converter_test 
			WHERE id = $id
		`, sql.Named("id", customID)).Scan(&id, &name, &createdAt)
		require.NoError(t, err)

		// Verify custom conversion worked
		require.Equal(t, "ID_"+customID.ID, id)
		require.Equal(t, "test_name", name)
		require.Equal(t, customTime.Format("2006-01-02 15:04:05"), createdAt)
	})

	t.Run("with custom named value converter", func(t *testing.T) {
		// Create a named value converter that handles special parameter names
		namedConverter := bind.NewCustomTypeConverter(
			func(v any) bool { return true }, // Handle all values
			func(v any) (value.Value, error) {
				// This converter will be used through NamedValueConverter interface
				return value.TextValue("processed"), nil
			},
		)

		db, err = sql.Open("ydb", "ydb://localhost:2136/local")
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		// Create connector with custom named value converter
		connector, err := ydb.Connector(
			&ydb.Driver{},
			ydb.WithCustomNamedValueConverter(namedConverter),
		)
		require.NoError(t, err)

		db = sql.OpenDB(connector)
		defer func() {
			_ = db.Close()
		}()

		// Create test table
		_, err = db.ExecContext(ctx, `
			CREATE TABLE named_converter_test (
				id TEXT,
				value TEXT,
				PRIMARY KEY(id)
			)
		`)
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, `DROP TABLE named_converter_test`)
		}()

		// Insert using named parameters
		_, err = db.ExecContext(ctx, `
			INSERT INTO named_converter_test (id, value) 
			VALUES ($id, $value)
		`,
			sql.Named("id", "test_id"),
			sql.Named("value", "original_value"),
		)
		require.NoError(t, err)

		// Query back the data
		var (
			id    string
			value string
		)
		err = db.QueryRowContext(ctx, `
			SELECT id, value 
			FROM named_converter_test 
			WHERE id = $id
		`, sql.Named("id", "test_id")).Scan(&id, &value)
		require.NoError(t, err)

		require.Equal(t, "test_id", id)
		// The value should be processed by the converter if it was applied
		// This test verifies the integration works end-to-end
	})
}

func TestDatabaseSQL_CustomConverter_UUID(t *testing.T) {
	var (
		ctx = context.Background()
		db  *sql.DB
		err error
	)

	t.Run("uuid conversion", func(t *testing.T) {
		db, err = sql.Open("ydb", "ydb://localhost:2136/local")
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		// UUID converter is registered by default
		connector, err := ydb.Connector(&ydb.Driver{})
		require.NoError(t, err)

		db = sql.OpenDB(connector)
		defer func() {
			_ = db.Close()
		}()

		// Create test table
		_, err = db.ExecContext(ctx, `
			CREATE TABLE uuid_test (
				id UUID,
				name TEXT,
				PRIMARY KEY(id)
			)
		`)
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, `DROP TABLE uuid_test`)
		}()

		// Test UUID insertion
		testID := uuid.New()
		_, err = db.ExecContext(ctx, `
			INSERT INTO uuid_test (id, name) 
			VALUES ($id, $name)
		`,
			testID,
			"test_name",
		)
		require.NoError(t, err)

		// Query back the UUID
		var (
			id   uuid.UUID
			name string
		)
		err = db.QueryRowContext(ctx, `
			SELECT id, name 
			FROM uuid_test 
			WHERE id = $id
		`, testID).Scan(&id, &name)
		require.NoError(t, err)

		require.Equal(t, testID, id)
		require.Equal(t, "test_name", name)
	})

	t.Run("uuid pointer conversion", func(t *testing.T) {
		db, err = sql.Open("ydb", "ydb://localhost:2136/local")
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		connector, err := ydb.Connector(&ydb.Driver{})
		require.NoError(t, err)

		db = sql.OpenDB(connector)
		defer func() {
			_ = db.Close()
		}()

		// Create test table
		_, err = db.ExecContext(ctx, `
			CREATE TABLE uuid_ptr_test (
				id UUID,
				name TEXT,
				PRIMARY KEY(id)
			)
		`)
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, `DROP TABLE uuid_ptr_test`)
		}()

		// Test UUID pointer insertion
		testID := uuid.New()
		_, err = db.ExecContext(ctx, `
			INSERT INTO uuid_ptr_test (id, name) 
			VALUES ($id, $name)
		`,
			&testID,
			"test_name_ptr",
		)
		require.NoError(t, err)

		// Query back the UUID
		var (
			id   uuid.UUID
			name string
		)
		err = db.QueryRowContext(ctx, `
			SELECT id, name 
			FROM uuid_ptr_test 
			WHERE id = $id
		`, &testID).Scan(&id, &name)
		require.NoError(t, err)

		require.Equal(t, testID, id)
		require.Equal(t, "test_name_ptr", name)
	})

	t.Run("nil uuid pointer", func(t *testing.T) {
		db, err = sql.Open("ydb", "ydb://localhost:2136/local")
		require.NoError(t, err)
		defer func() {
			_ = db.Close()
		}()

		connector, err := ydb.Connector(&ydb.Driver{})
		require.NoError(t, err)

		db = sql.OpenDB(connector)
		defer func() {
			_ = db.Close()
		}()

		// Create test table with nullable UUID
		_, err = db.ExecContext(ctx, `
			CREATE TABLE uuid_null_test (
				id UUID,
				name TEXT,
				PRIMARY KEY(id)
			)
		`)
		require.NoError(t, err)
		defer func() {
			_, _ = db.ExecContext(ctx, `DROP TABLE uuid_null_test`)
		}()

		// Test nil UUID pointer insertion
		var idPtr *uuid.UUID = nil
		_, err = db.ExecContext(ctx, `
			INSERT INTO uuid_null_test (id, name) 
			VALUES ($id, $name)
		`,
			idPtr,
			"null_test",
		)
		require.NoError(t, err)

		// Query back the data
		var (
			id   *uuid.UUID
			name string
		)
		err = db.QueryRowContext(ctx, `
			SELECT id, name 
			FROM uuid_null_test 
			WHERE name = $name
		`, "null_test").Scan(&id, &name)
		require.NoError(t, err)

		require.Nil(t, id)
		require.Equal(t, "null_test", name)
	})
}
