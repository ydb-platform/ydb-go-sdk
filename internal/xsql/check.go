package xsql

import (
	"database/sql/driver"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func CheckNamedValue(v *driver.NamedValue) (err error) {
	if v.Name == "" {
		return fmt.Errorf("ydb: only named parameters are supported")
	}

	if valuer, ok := v.Value.(driver.Valuer); ok {
		v.Value, err = valuer.Value()
		if err != nil {
			return fmt.Errorf("ydb: driver.Valuer error: %w", err)
		}
	}

	switch x := v.Value.(type) {
	case types.Value:
		// OK.
	case bool:
		v.Value = types.BoolValue(x)
	case int8:
		v.Value = types.Int8Value(x)
	case uint8:
		v.Value = types.Uint8Value(x)
	case int16:
		v.Value = types.Int16Value(x)
	case uint16:
		v.Value = types.Uint16Value(x)
	case int32:
		v.Value = types.Int32Value(x)
	case uint32:
		v.Value = types.Uint32Value(x)
	case int64:
		v.Value = types.Int64Value(x)
	case uint64:
		v.Value = types.Uint64Value(x)
	case float32:
		v.Value = types.FloatValue(x)
	case float64:
		v.Value = types.DoubleValue(x)
	case []byte:
		v.Value = types.StringValue(x)
	case string:
		v.Value = types.UTF8Value(x)
	case [16]byte:
		v.Value = types.UUIDValue(x)

	default:
		return xerrors.WithStackTrace(fmt.Errorf("ydb: unsupported type: %T", x))
	}

	if v.Name[0] != '$' {
		v.Name = "$" + v.Name
	}

	return nil
}
