//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// https://github.com/ydb-platform/ydb-go-sdk/issues/757
// Containers example demonstrates how to work with YDB container values such as `List`, `Tuple`, `Dict`, `Struct` and `Variant`
func TestDatabaseSqlContainers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nativeDriver, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	connector, err := ydb.Connector(nativeDriver)
	require.NoError(t, err)
	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)

	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		rows, err := cc.QueryContext(ctx, `
			SELECT
				AsList("foo", "bar", "baz");
			SELECT
				AsTuple(42, "foo", AsList(41, 42, 43));
			SELECT
				AsDict(
					AsTuple("foo"u, 10),
					AsTuple("bar"u, 20),
					AsTuple("baz"u, 30),
				);
			SELECT
				AsStruct(
					41 AS foo,
					42 AS bar,
					43 AS baz,
				);
		
			$struct = AsStruct(
				Uint32("0") as foo,
				UTF8("x") as bar,
				Int64("0") as baz,
			);
			$variantStructType = VariantType(TypeOf($struct));
			SELECT Variant(42, "baz", $variantStructType);
		
			$tuple = AsTuple(
				Uint32("0"),
				UTF8("x"),
				Int64("0"),
			);
			$variantTupleType = VariantType(TypeOf($tuple));
			SELECT Variant(42, "2", $variantTupleType);
			SELECT CAST(42 AS Int64);
		`)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()

		parsers := [...]func() error{
			func() error {
				return rows.Scan(&testDatabaseSqlContainersExampleList{})
			},
			func() error {
				return rows.Scan(&testDatabaseSqlContainersExampleTuple{})
			},
			func() error {
				return rows.Scan(&testDatabaseSqlContainersExampleDict{})
			},
			func() error {
				return rows.Scan(&testDatabaseSqlContainersExampleStruct{})
			},
			func() error {
				return rows.Scan(&testDatabaseSqlContainersVariantStruct{})
			},
			func() error {
				return rows.Scan(&testDatabaseSqlContainersVariantTuple{test: t})
			},
			func() error {
				var v int64
				return rows.Scan(&v)
			},
		}

		for set := 0; rows.NextResultSet(); set++ {
			rows.Next()
			err = parsers[set]()
			if err != nil {
				return err
			}
		}
		return rows.Err()
	}, retry.WithIdempotent(true))
	require.NoError(t, err)
}

type testDatabaseSqlContainersExampleStruct struct {
	foo int
	bar int
	baz int
}

func (s *testDatabaseSqlContainersExampleStruct) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if fields, err := types.StructFields(v); err != nil {
			return err
		} else {
			if v, ok := fields["foo"]; !ok {
				return fmt.Errorf("struct '%v' (type '%s') cannot have 'foo' field", v, v.Type().Yql())
			} else if err := types.CastTo(v, &s.foo); err != nil {
				return err
			}
			if v, ok := fields["bar"]; !ok {
				return fmt.Errorf("struct '%v' (type '%s') cannot have 'bar' field", v, v.Type().Yql())
			} else if err := types.CastTo(v, &s.bar); err != nil {
				return err
			}
			if v, ok := fields["baz"]; !ok {
				return fmt.Errorf("struct '%v' (type '%s') cannot have 'baz' field", v, v.Type().Yql())
			} else if err := types.CastTo(v, &s.baz); err != nil {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("type '%T' is not a `types.value` type", res)
}

type testDatabaseSqlContainersExampleList []string

func (list *testDatabaseSqlContainersExampleList) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if items, err := types.ListItems(v); err != nil {
			return err
		} else {
			*list = make([]string, len(items))
			for i, item := range items {
				var v string
				if err := types.CastTo(item, &v); err != nil {
					return err
				}
				(*list)[i] = v
			}
			return nil
		}
	}
	return fmt.Errorf("type '%T' is not a `types.Value` type", res)
}

type testDatabaseSqlContainersExampleTuple struct {
	a int
	b string
	c []int
}

func (tuple *testDatabaseSqlContainersExampleTuple) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if items, err := types.TupleItems(v); err != nil {
			return err
		} else {
			if len(items) != 3 {
				return fmt.Errorf("tuple items not consists with testDatabaseSqlContainersExampleTuple type")
			}
			if err := types.CastTo(items[0], &tuple.a); err != nil {
				return err
			}
			if err := types.CastTo(items[1], &tuple.b); err != nil {
				return err
			}
			if items, err := types.ListItems(items[2]); err != nil {
				return err
			} else {
				tuple.c = make([]int, len(items))
				for i, item := range items {
					var v int
					if err := types.CastTo(item, &v); err != nil {
						return err
					}
					tuple.c[i] = i
				}
				return nil
			}
		}
	}
	return fmt.Errorf("type '%T' is not a `types.Value` type", res)
}

type testDatabaseSqlContainersExampleDict map[string]int

func (dict *testDatabaseSqlContainersExampleDict) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if items, err := types.DictValues(v); err != nil {
			return err
		} else {
			*dict = make(map[string]int, len(items))
			for k, v := range items {
				var (
					key   string
					value int
				)
				if err := types.CastTo(k, &key); err != nil {
					return err
				}
				if err := types.CastTo(v, &value); err != nil {
					return err
				}
				(*dict)[key] = value
			}
			return nil
		}
	}
	return fmt.Errorf("type '%T' is not a `types.Value` type", res)
}

type testDatabaseSqlContainersVariantStruct struct {
	foo uint32
	bar string
	baz int64
}

func (s *testDatabaseSqlContainersVariantStruct) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if name, _, value, err := types.VariantValue(v); err != nil {
			return err
		} else {
			switch name {
			case "foo":
				if err := types.CastTo(value, &s.foo); err != nil {
					return err
				}
			case "bar":
				if err := types.CastTo(value, &s.bar); err != nil {
					return err
				}
			case "baz":
				if err := types.CastTo(value, &s.baz); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected variant struct field name '%s'", name)
			}
			return nil
		}
	}
	return fmt.Errorf("type '%T' is not a `types.Value` type", res)
}

type testDatabaseSqlContainersVariantTuple struct {
	a    uint32
	b    string
	c    int64
	test testing.TB
}

func (s *testDatabaseSqlContainersVariantTuple) Scan(res interface{}) error {
	if v, has := res.(types.Value); has {
		if _, idx, value, err := types.VariantValue(v); err != nil {
			return err
		} else {
			switch idx {
			case 0:
				if err := types.CastTo(value, &s.a); err != nil {
					return err
				}
			case 1:
				if err := types.CastTo(value, &s.b); err != nil {
					return err
				}
			case 2:
				if err := types.CastTo(value, &s.c); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected tuple field index '%d'", idx)
			}
			return nil
		}
	}
	return fmt.Errorf("type '%T' is not a `types.Value` type", res)
}

func (s *testDatabaseSqlContainersVariantTuple) UnmarshalYDB(res types.RawValue) error {
	s.test.Logf("T: %s", res.Type())
	name, index := res.Variant()
	var x interface{}
	switch index {
	case 0:
		x = res.Uint32()
	case 1:
		x = res.UTF8()
	case 2:
		x = res.Int64()
	}
	s.test.Logf(
		"(tuple variant): %s %s %q %d = %v",
		res.Path(), res.Type(), name, index, x,
	)
	return res.Err()
}
