package main

import (
	"bytes"
	"log"
	"text/template"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var query = template.Must(template.New("fill database").Parse(`
	DECLARE $var AS Variant<Text,Uint64,Uint32>;
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
`))

type exampleStruct struct {
}

func (*exampleStruct) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.StructIn(); i < n; i++ {
		name := res.StructField(i)
		val := res.Int32()
		log.Printf("(struct): %s: %d", name, val)
	}
	res.StructOut()
	return res.Err()
}

type exampleList struct {
}

func (*exampleList) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.ListIn(); i < n; i++ {
		res.ListItem(i)
		log.Printf("(list) %q: %s", res.Path(), res.String())
	}
	res.ListOut()
	return res.Err()
}

type exampleTuple struct {
}

func (*exampleTuple) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
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
	return res.Err()
}

type exampleDict struct {
}

func (*exampleDict) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
	for i, n := 0, res.DictIn(); i < n; i++ {
		res.DictKey(i)
		key := res.String()

		res.DictPayload(i)
		val := res.Int32()

		log.Printf("(dict) %q: %s: %d", res.Path(), key, val)
	}
	res.DictOut()
	return res.Err()
}

type variantStruct struct {
}

func (*variantStruct) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
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
	return res.Err()
}

type variantTuple struct {
}

func (*variantTuple) UnmarshalYDB(res types.RawValue) error {
	log.Printf("T: %s", res.Type())
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
	return res.Err()
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
