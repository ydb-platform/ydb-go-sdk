package main

import (
	"bytes"
	"context"
	"log"
	"text/template"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

var query = template.Must(template.New("fill database").Parse(`
	DECLARE $var AS "Variant<Utf8,Uint64,Uint32>";
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
			41 AS "foo",
			42 AS "bar",
			43 AS "baz",
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
`))

func run(ctx context.Context, endpoint, prefix string, config *ydb.DriverConfig) error {
	driver, err := (&ydb.Dialer{
		DriverConfig: config,
	}).Dial(ctx, endpoint)
	if err != nil {
		return err
	}

	c := table.Client{Driver: driver}
	session, err := c.CreateSession(ctx)
	if err != nil {
		return err
	}
	defer session.Delete(context.Background())

	tx, err := session.BeginTransaction(ctx, table.TxSettings(
		table.WithSerializableReadWrite(),
	))
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	res, err := tx.ExecuteDataQuery(ctx, table.TextDataQuery(render(query, nil)), nil)
	if err != nil {
		return err
	}
	tx.Commit(ctx)

	parsers := [...]func(){
		func() {
			for i, n := 0, res.ListIn(); i < n; i++ {
				res.ListItem(i)
				log.Printf("(list) %q: %s", res.Path(), res.String())
			}
			res.ListOut()
		},
		func() {
			for i, n := 0, res.TupleIn(); i < n; i++ {
				res.TupleItem(i)
				switch i {
				case 0:
					log.Printf("(tuple) %q: %d", res.Path(), res.Int32())
				case 1:
					log.Printf("(tuple) %q: %s", res.Path(), res.String())
				case 2:
					n := res.ListIn()
					for j := 0; j < n; j++ {
						res.ListItem(j)
						log.Printf("(tuple) %q: %d", res.Path(), res.Int32())
					}
					res.ListOut()
				}
			}
			res.TupleOut()
		},
		func() {
			for i, n := 0, res.DictIn(); i < n; i++ {
				res.DictKey(i)
				key := res.String()

				res.DictPayload(i)
				val := res.Int32()

				log.Printf("(dict) %q: %s: %d", res.Path(), key, val)
			}
			res.DictOut()
		},
		func() {
			for i, n := 0, res.StructIn(); i < n; i++ {
				name := res.StructField(i)
				val := res.Int32()
				log.Printf("(struct) %q: %s: %d", res.Path(), name, val)
			}
			res.StructOut()
		},
		func() {
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
			log.Printf(
				"(struct variant): %s %s %q %d = %v",
				res.Path(), res.Type(), name, index, x,
			)
		},
		func() {
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
			log.Printf(
				"(tuple variant): %s %s %q %d = %v",
				res.Path(), res.Type(), name, index, x,
			)
		},
	}
	for set := 0; res.NextSet(); set++ {
		res.NextRow()
		res.NextItem()
		log.Printf("T: %s", res.Type())
		parsers[set]()
	}
	if err := res.Err(); err != nil {
		return err
	}

	return nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
