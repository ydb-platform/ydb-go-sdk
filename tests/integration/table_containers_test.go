//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// Containers example demonstrates how to work with YDB container values such as `List`, `Tuple`, `Dict`, `Struct` and `Variant`
func TestContainers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) (err error) {
		res, err := tx.Execute(ctx, `
			SELECT
				AsList("foo", "bar", "baz");
			SELECT
				AsTuple(42, "foo", AsList(41, 42, 43));
			SELECT
				AsDict(
					AsTuple("foo", 10),
					AsTuple("bar", 20),
					AsTuple("baz", 30),
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
		`, nil)
		if err != nil {
			return err
		}
		defer func() {
			_ = res.Close()
		}()

		parsers := [...]func() error{
			func() error {
				return res.Scan(&testContainersExampleList{test: t})
			},
			func() error {
				return res.Scan(&testContainersExampleTuple{test: t})
			},
			func() error {
				return res.Scan(&testContainersExampleDict{test: t})
			},
			func() error {
				return res.Scan(&testContainersExampleStruct{test: t})
			},
			func() error {
				return res.Scan(&testContainersVariantStruct{test: t})
			},
			func() error {
				return res.Scan(&testContainersVariantTuple{test: t})
			},
		}

		for set := 0; res.NextResultSet(ctx); set++ {
			res.NextRow()
			err = parsers[set]()
			if err != nil {
				return err
			}
		}
		return res.Err()
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())), table.WithIdempotent())
	require.NoError(t, err)
}

type testContainersExampleStruct struct {
	test testing.TB
}

func (s *testContainersExampleStruct) UnmarshalYDB(res types.RawValue) error {
	s.test.Logf("T: %s", res.Type())
	for i, n := 0, res.StructIn(); i < n; i++ {
		name := res.StructField(i)
		val := res.Int32()
		s.test.Logf("(struct): %s: %d", name, val)
	}
	res.StructOut()
	return res.Err()
}

type testContainersExampleList struct {
	test testing.TB
}

func (c *testContainersExampleList) UnmarshalYDB(res types.RawValue) error {
	c.test.Logf("T: %s", res.Type())
	for i, n := 0, res.ListIn(); i < n; i++ {
		res.ListItem(i)
		c.test.Logf("(list) %q: %s", res.Path(), res.String())
	}
	res.ListOut()
	return res.Err()
}

type testContainersExampleTuple struct {
	test testing.TB
}

func (c *testContainersExampleTuple) UnmarshalYDB(res types.RawValue) error {
	c.test.Logf("T: %s", res.Type())
	for i, n := 0, res.TupleIn(); i < n; i++ {
		res.TupleItem(i)
		switch i {
		case 0:
			c.test.Logf("(tuple) %q: %d", res.Path(), res.Int32())
		case 1:
			c.test.Logf("(tuple) %q: %s", res.Path(), res.String())
		case 2:
			n := res.ListIn()
			for j := 0; j < n; j++ {
				res.ListItem(j)
				c.test.Logf("(tuple) %q: %d", res.Path(), res.Int32())
			}
			res.ListOut()
		}
	}
	res.TupleOut()
	return res.Err()
}

type testContainersExampleDict struct {
	test testing.TB
}

func (c *testContainersExampleDict) UnmarshalYDB(res types.RawValue) error {
	c.test.Logf("T: %s", res.Type())
	for i, n := 0, res.DictIn(); i < n; i++ {
		res.DictKey(i)
		key := res.String()

		res.DictPayload(i)
		val := res.Int32()

		c.test.Logf("(dict) %q: %s: %d", res.Path(), key, val)
	}
	res.DictOut()
	return res.Err()
}

type testContainersVariantStruct struct {
	test testing.TB
}

func (c *testContainersVariantStruct) UnmarshalYDB(res types.RawValue) error {
	c.test.Logf("T: %s", res.Type())
	name, index := res.Variant()
	var x interface{}
	switch name {
	case "foo":
		x = res.Uint32()
	case "bar":
		x = res.UTF8()
	case "baz":
		x = res.Int64()
	}
	c.test.Logf(
		"(struct variant): %s %s %q %d = %v",
		res.Path(), res.Type(), name, index, x,
	)
	return res.Err()
}

type testContainersVariantTuple struct {
	test testing.TB
}

func (c *testContainersVariantTuple) UnmarshalYDB(res types.RawValue) error {
	c.test.Logf("T: %s", res.Type())
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
	c.test.Logf(
		"(tuple variant): %s %s %q %d = %v",
		res.Path(), res.Type(), name, index, x,
	)
	return res.Err()
}
