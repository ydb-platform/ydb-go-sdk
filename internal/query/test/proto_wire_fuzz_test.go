package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	internaltypes "github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func FuzzQueryProtoWireDecoding(f *testing.F) {
	fuzzWireSeeds(f, false)
	// Each worker reuses its transport. EOF synchronizes fixture replacement
	// with the previous handler; fuzz callbacks do not run in parallel.
	frames := make([]*Ydb_Query.ExecuteQueryResponsePart, 12)
	server := mock.Server(f, mock.WithExecuteQueryParts(frames...))
	drivers := fuzzWireDrivers(f, server.ConnString())
	f.Fuzz(func(t *testing.T, data []byte) {
		input := fuzzInput{data: data}
		width, height := int(input.byte()%65), int(input.byte()%4)
		chunk, control := int(input.byte()%3)+1, input.byte()
		prefetch, setCount := int(control&1)*2, int(control>>1&1)+1
		columns := make([]*Ydb.Column, width)
		budget := 64
		for i := range columns {
			columns[i] = &Ydb.Column{Name: fmt.Sprintf("v%d", i), Type: input.schema(0, &budget)}
		}
		want := make([][][]*Ydb.TypedValue, setCount)
		var parts []*Ydb_Query.ExecuteQueryResponsePart
		for set := range setCount {
			rows := make([]*Ydb.Value, height)
			want[set] = make([][]*Ydb.TypedValue, height)
			for i := range rows {
				rows[i] = &Ydb.Value{Items: make([]*Ydb.Value, width)}
				want[set][i] = make([]*Ydb.TypedValue, width)
				for j, column := range columns {
					rows[i].Items[j] = input.cell(column.GetType())
					want[set][i][j] = &Ydb.TypedValue{Type: column.GetType(), Value: rows[i].GetItems()[j]}
				}
			}
			for start := 0; start < height || start == 0; start += chunk {
				rs := &Ydb.ResultSet{Rows: rows[start:min(start+chunk, height)]}
				if start == 0 {
					rs.Columns = columns
				}
				parts = append(parts, &Ydb_Query.ExecuteQueryResponsePart{
					Status: Ydb.StatusIds_SUCCESS, ResultSetIndex: int64(set), ResultSet: rs,
				})
				if control&4 != 0 {
					parts = append(parts, &Ydb_Query.ExecuteQueryResponsePart{Status: Ydb.StatusIds_SUCCESS})
				}
			}
		}
		for i := range frames {
			frames[i] = &Ydb_Query.ExecuteQueryResponsePart{Status: Ydb.StatusIds_SUCCESS}
		}
		copy(frames, parts)
		before := server.ExecuteQueryCalls()
		for mode, db := range drivers {
			t.Run(fmt.Sprintf("wire=%v", mode == 1), func(t *testing.T) {
				err := db.Query().Do(t.Context(), func(ctx context.Context, session query.Session) error {
					result, err := session.Query(ctx, "SELECT fuzz fixture", query.WithResponsePartPrefetch(prefetch))
					if err != nil {
						return err
					}
					defer result.Close(ctx)
					var saved []query.Row
					var checks []func()
					var savedWant [][]*Ydb.TypedValue
					for set, expectedRows := range want {
						rs, err := result.NextResultSet(ctx)
						if err != nil {
							return err
						}
						if len(rs.Columns()) != width || len(rs.ColumnTypes()) != width || rs.Index() != set {
							t.Fatalf("unexpected result schema: %v %v index %d", rs.Columns(), rs.ColumnTypes(), rs.Index())
						}
						for i, column := range columns {
							if rs.Columns()[i] != column.GetName() || !proto.Equal(rs.ColumnTypes()[i].ToYDB(), column.GetType()) {
								t.Fatalf("column %d differs from generated schema", i)
							}
						}
						for row, expected := range expectedRows {
							got, err := rs.NextRow(ctx)
							if err != nil {
								return fmt.Errorf("set %d row %d: %w", set, row, err)
							}
							checks = append(checks, fuzzCheckRow(t, got, expected))
							saved = append(saved, got)
							savedWant = append(savedWant, expected)
						}
						if _, err := rs.NextRow(ctx); !errors.Is(err, io.EOF) {
							t.Fatalf("end of rows = %v, want EOF", err)
						}
					}
					if _, err := result.NextResultSet(ctx); !errors.Is(err, io.EOF) {
						t.Fatalf("end of result sets = %v, want EOF", err)
					}
					if err := result.Close(ctx); err != nil {
						return err
					}
					for i, row := range saved {
						checks[i]()
						fuzzCheckRow(t, row, savedWant[i])
					}

					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
		if got := server.ExecuteQueryCalls() - before; got != 2 {
			t.Fatalf("ExecuteQuery calls = %d, want 2 without retries", got)
		}
	})
}

// FuzzQueryProtoWireValue uses the same grammar without transport overhead.
func FuzzQueryProtoWireValue(f *testing.F) {
	fuzzWireSeeds(f, true)
	f.Fuzz(func(t *testing.T, data []byte) {
		input := fuzzInput{data: data}
		budget := 64
		typ := input.schema(0, &budget)
		expected := &Ydb.TypedValue{Type: typ, Value: input.cell(typ)}
		encoded, err := proto.Marshal(expected.GetValue())
		if err != nil {
			t.Fatal(err)
		}
		var normal Ydb.Value
		if err := proto.Unmarshal(encoded, &normal); err != nil {
			t.Fatal(err)
		}
		fuzzCheckValue(t, value.FromYDB(typ, &normal), expected)
		got, err := value.FromWire(internaltypes.TypeFromYDB(typ), encoded)
		if err != nil {
			t.Fatal(err)
		}
		fuzzCheckValue(t, got, expected)
	})
}

func fuzzWireDrivers(f *testing.F, dsn string) []*ydb.Driver {
	f.Helper()
	drivers := make([]*ydb.Driver, 2)
	for i := range drivers {
		db, err := ydb.Open(f.Context(), dsn, ydb.WithAnonymousCredentials(),
			ydb.WithQueryProtoWireDecoding(i == 1),
			// Use gRPC's disabled keepalive interval with the default mock server.
			// SDK pings every 10 seconds otherwise exceed its enforcement policy.
			ydb.With(config.WithGrpcOptions(grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time: time.Duration(math.MaxInt64),
			}))))
		if err != nil {
			f.Fatal(err)
		}
		f.Cleanup(func() {
			if err := db.Close(context.WithoutCancel(f.Context())); err != nil {
				f.Error(err)
			}
		})
		drivers[i] = db
	}

	return drivers
}

func fuzzWireSeeds(f *testing.F, schemaOnly bool) {
	add := func(data []byte) {
		if schemaOnly {
			data = data[4:]
		}
		f.Add(data)
	}
	for kind := range byte(47) {
		for _, payload := range [][]byte{{}, {1, 1, 1, 1, 1}, bytes.Repeat([]byte{255}, 32)} {
			seed := []byte{1, 2, 1, 0, kind}
			add(append(seed, payload...))
		}
	}
	for _, width := range []byte{0, 1, 2, 8, 16, 64} {
		for _, rows := range []byte{0, 1, 3} {
			add([]byte{width, rows, 1, 7, 33, 33, 34, 39, 2, 1, 1, 1})
		}
	}
	for kind := range byte(32) {
		for _, number := range []uint64{1, 1 << 7, 1 << 15, 1 << 31, 1 << 63, math.MaxUint64 >> 1, math.MaxUint64} {
			seed := []byte{1, 1, 0, 0, kind}
			seed = binary.LittleEndian.AppendUint64(seed, number)
			add(seed)
		}
	}
	for _, number := range []uint64{
		1, 0x80000000, 0x7f7fffff, 0x7f800000, 0xff800000, 0x7fa00001,
		0x8000000000000000, 0x7fefffffffffffff, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff0000000000001,
	} {
		for _, kind := range []byte{9, 10} {
			add(binary.LittleEndian.AppendUint64([]byte{1, 3, 0, 1, kind}, number))
		}
	}

	for _, sign := range []byte{0, 1} {
		decimal := []byte{1, 1, 0, 0, 32, 34, 9}
		decimal = binary.LittleEndian.AppendUint64(decimal, 1)
		decimal = binary.LittleEndian.AppendUint64(decimal, math.MaxUint64)
		add(append(decimal, sign))
	}

	// Keys with a shared decimal prefix must compare independently of YQL order.
	dictionary := []byte{1, 1, 0, 0, 37, 5, 3}
	for _, key := range []uint64{0, 255, 2555952} {
		dictionary = binary.LittleEndian.AppendUint64(dictionary, key)
		dictionary = binary.LittleEndian.AppendUint64(dictionary, 42)
	}
	add(dictionary)

	for _, kind := range []byte{22, 23} {
		for _, payload := range [][]byte{
			nil,
			{0},
			[]byte("Привет\x00"), bytes.Repeat([]byte{255}, 127),
			bytes.Repeat([]byte{127}, 128), bytes.Repeat([]byte{255}, 255),
		} {
			seed := binary.LittleEndian.AppendUint64([]byte{1, 2, 0, 0, kind}, 0)
			seed = append(seed, byte(len(payload)))
			add(append(seed, payload...))
		}
	}

	// Present Optional<List<Optional<Int32>>> with both present and null items.
	nested := []byte{1, 1, 0, 0, 33, 34, 33, 5, 1, 2, 1}
	nested = binary.LittleEndian.AppendUint64(nested, 42)
	add(append(nested, 0))
	// Present Optional<Variant<Tuple<Optional<Int32>>>> with a null payload.
	add([]byte{1, 1, 0, 0, 33, 39, 0, 33, 5, 1, 0, 0})
	// Tagged<Optional<Variant<Tuple<Optional<Int32>>>>> with a null payload.
	add([]byte{1, 1, 0, 0, 41, 33, 39, 0, 33, 5, 1, 0, 0})
}

func fuzzCheckValue(t *testing.T, got types.Value, want *Ydb.TypedValue) {
	t.Helper()
	actual := value.ToYDB(got)
	expected := proto.Clone(want).(*Ydb.TypedValue)
	// Dictionary order is not part of its logical value. Normalize both sides
	// independently of the SDK's YQL-based ordering policy.
	fuzzNormalizePairs(t, actual.GetValue())
	fuzzNormalizePairs(t, expected.GetValue())
	gotBytes, wantBytes := fuzzMarshal(t, actual), fuzzMarshal(t, expected)
	// Exact bytes also preserve signed zero and NaN payloads.
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatalf("got %v (%x), want %v (%x)", actual, gotBytes, expected, wantBytes)
	}
}

func fuzzMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func fuzzNormalizePairs(t *testing.T, cell *Ydb.Value) {
	t.Helper()
	if cell == nil {
		return
	}
	fuzzNormalizePairs(t, cell.GetNestedValue())
	for _, item := range cell.GetItems() {
		fuzzNormalizePairs(t, item)
	}
	for _, pair := range cell.GetPairs() {
		fuzzNormalizePairs(t, pair.GetKey())
		fuzzNormalizePairs(t, pair.GetPayload())
	}
	sort.Slice(cell.GetPairs(), func(i, j int) bool {
		return bytes.Compare(fuzzMarshal(t, cell.GetPairs()[i].GetKey()), fuzzMarshal(t, cell.GetPairs()[j].GetKey())) < 0
	})
}

func fuzzCheckRow(t *testing.T, row query.Row, want []*Ydb.TypedValue) func() {
	t.Helper()
	expected := make([]*Ydb.TypedValue, len(want))
	for i, item := range want {
		expected[i] = proto.Clone(item).(*Ydb.TypedValue)
	}
	want = expected
	check := func(got []types.Value) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("values=%d, want %d", len(got), len(want))
		}
		for i, item := range got {
			fuzzCheckValue(t, item, want[i])
		}
	}
	owned := row.Values()
	check(owned)
	scanned := make([]types.Value, len(want))
	dst := make([]any, len(want))
	named := make([]query.NamedDestination, len(want))
	for i := range dst {
		dst[i] = &scanned[i]
		named[len(dst)-i-1] = query.Named(fmt.Sprintf("v%d", i), dst[i])
	}
	if err := row.Scan(dst...); err != nil {
		t.Fatal(err)
	}
	check(scanned)
	indexedScanned := append([]types.Value(nil), scanned...)
	clear(scanned)
	if err := row.ScanNamed(named...); err != nil {
		t.Fatal(err)
	}
	check(scanned)
	namedScanned := append([]types.Value(nil), scanned...)
	fields := make([]reflect.StructField, len(want))
	for i := range fields {
		fields[i] = reflect.StructField{
			Name: fmt.Sprintf("V%d", i), Type: reflect.TypeFor[types.Value](),
			Tag: reflect.StructTag(fmt.Sprintf(`sql:"v%d"`, i)),
		}
	}
	structure := reflect.New(reflect.StructOf(fields))
	if err := row.ScanStruct(structure.Interface()); err != nil {
		t.Fatal(err)
	}
	for i := range scanned {
		scanned[i] = structure.Elem().Field(i).Interface().(types.Value)
	}
	check(scanned)
	structScanned := append([]types.Value(nil), scanned...)
	native := append([]any(nil), dst...)
	for i, expected := range want {
		if destination := fuzzNativeDestination(expected.GetType()); destination != nil {
			native[i] = destination
		}
	}
	if err := row.Scan(native...); err != nil {
		t.Fatal(err)
	}
	for i, expected := range want {
		destination := fuzzNativeDestination(expected.GetType())
		if destination == nil {
			continue
		}
		if err := row.ScanNamed(query.Named(fmt.Sprintf("v%d", i), destination)); err != nil {
			t.Fatal(err)
		}
		reference := fuzzExpectedNative(expected.GetType(), expected.GetValue())
		if !reflect.DeepEqual(fuzzNativeValue(reflect.ValueOf(destination)), reference) ||
			!reflect.DeepEqual(fuzzNativeValue(reflect.ValueOf(native[i])), reference) {
			t.Fatalf("column %d native Scan differs", i)
		}
	}

	return func() {
		check(owned)
		check(indexedScanned)
		check(namedScanned)
		check(structScanned)
		for i, expected := range want {
			if fuzzNativeDestination(expected.GetType()) == nil {
				continue
			}
			got := fuzzNativeValue(reflect.ValueOf(native[i]))
			want := fuzzExpectedNative(expected.GetType(), expected.GetValue())
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("column %d native Scan changed after subsequent rows or Close", i)
			}
		}
	}
}

func fuzzNativeDestination(t *Ydb.Type) any {
	if optional := t.GetOptionalType(); optional != nil {
		switch optional.GetItem().GetTypeId() {
		case Ydb.Type_INT32:
			return new(*int32)
		case Ydb.Type_BOOL:
			return new(*bool)
		case Ydb.Type_DOUBLE:
			return new(*float64)
		case Ydb.Type_UTF8:
			return new(*string)
		case Ydb.Type_STRING:
			return new(*[]byte)
		}
	}
	switch t.GetTypeId() {
	case Ydb.Type_UINT64:
		return new(uint64)
	case Ydb.Type_FLOAT:
		return new(float32)
	case Ydb.Type_DOUBLE:
		return new(float64)
	}

	return nil
}

func fuzzNativeValue(v reflect.Value) any {
	if v.CanInterface() {
		if p, ok := v.Interface().(*float32); ok {
			return math.Float32bits(*p)
		}
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}

		return fuzzNativeValue(v.Elem())
	}
	if v.Kind() == reflect.Float32 {
		return math.Float32bits(float32(v.Float()))
	}
	if v.Kind() == reflect.Float64 {
		return math.Float64bits(v.Float())
	}
	if v.Kind() == reflect.Slice {
		return string(v.Bytes())
	}

	return v.Interface()
}

func fuzzExpectedNative(t *Ydb.Type, v *Ydb.Value) any {
	if optional := t.GetOptionalType(); optional != nil {
		if _, null := v.GetValue().(*Ydb.Value_NullFlagValue); null {
			return nil
		}

		return fuzzExpectedNative(optional.GetItem(), v)
	}
	switch t.GetTypeId() {
	case Ydb.Type_UINT64:
		return v.GetUint64Value()
	case Ydb.Type_INT32:
		return v.GetInt32Value()
	case Ydb.Type_BOOL:
		return v.GetBoolValue()
	case Ydb.Type_FLOAT:
		return math.Float32bits(v.GetFloatValue())
	case Ydb.Type_DOUBLE:
		return math.Float64bits(v.GetDoubleValue())
	case Ydb.Type_UTF8:
		return v.GetTextValue()
	case Ydb.Type_STRING:
		return string(v.GetBytesValue())
	default:
		panic("missing native expectation")
	}
}

type fuzzInput struct {
	data []byte
	pos  int
}

func (g *fuzzInput) byte() byte {
	if g.pos >= len(g.data) {
		return 0
	}
	b := g.data[g.pos]
	g.pos++

	return b
}

func (g *fuzzInput) number() uint64 {
	var b [8]byte
	for i := range b {
		b[i] = g.byte()
	}

	return binary.LittleEndian.Uint64(b[:])
}

// bytes bounds individual string and byte payloads while crossing wire-length boundaries.
func (g *fuzzInput) bytes() []byte {
	data := make([]byte, int(g.byte()))
	for i := range data {
		data[i] = g.byte()
	}

	return data
}

var fuzzPrimitives = []Ydb.Type_PrimitiveTypeId{
	Ydb.Type_BOOL, Ydb.Type_INT8, Ydb.Type_UINT8, Ydb.Type_INT16, Ydb.Type_UINT16,
	Ydb.Type_INT32, Ydb.Type_UINT32, Ydb.Type_INT64, Ydb.Type_UINT64, Ydb.Type_FLOAT, Ydb.Type_DOUBLE,
	Ydb.Type_DATE, Ydb.Type_DATETIME, Ydb.Type_TIMESTAMP, Ydb.Type_INTERVAL,
	Ydb.Type_TZ_DATE, Ydb.Type_TZ_DATETIME, Ydb.Type_TZ_TIMESTAMP,
	Ydb.Type_DATE32, Ydb.Type_DATETIME64, Ydb.Type_TIMESTAMP64, Ydb.Type_INTERVAL64,
	Ydb.Type_STRING, Ydb.Type_UTF8, Ydb.Type_YSON, Ydb.Type_JSON, Ydb.Type_UUID,
	Ydb.Type_JSON_DOCUMENT, Ydb.Type_DYNUMBER, 0x44, 0x45, 0x46,
}

func (g *fuzzInput) schema(depth int, budget *int) *Ydb.Type {
	kind := int(g.byte() % 47)
	if depth >= 4 || *budget <= 0 {
		kind %= len(fuzzPrimitives)
	}
	*budget--
	if kind < len(fuzzPrimitives) {
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: fuzzPrimitives[kind]}}
	}
	child := func() *Ydb.Type { return g.schema(depth+1, budget) }
	switch kind {
	case 32:
		precision := uint32(g.byte()%35) + 1

		return &Ydb.Type{Type: &Ydb.Type_DecimalType{DecimalType: &Ydb.DecimalType{
			Precision: precision, Scale: uint32(g.byte()) % (precision + 1),
		}}}
	case 33:
		return &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: child()}}}
	case 34:
		return &Ydb.Type{Type: &Ydb.Type_ListType{ListType: &Ydb.ListType{Item: child()}}}
	case 35, 39:
		tuple := &Ydb.TupleType{}
		count := int(g.byte() % 4)
		if kind == 39 {
			count = count%3 + 1
		}
		for range count {
			tuple.Elements = append(tuple.Elements, child())
		}
		if kind == 39 {
			return &Ydb.Type{Type: &Ydb.Type_VariantType{VariantType: &Ydb.VariantType{
				Type: &Ydb.VariantType_TupleItems{TupleItems: tuple},
			}}}
		}

		return &Ydb.Type{Type: &Ydb.Type_TupleType{TupleType: tuple}}
	case 36, 40:
		structure := &Ydb.StructType{}
		count := int(g.byte() % 4)
		if kind == 40 {
			count = count%3 + 1
		}
		for i := range count {
			structure.Members = append(structure.Members, &Ydb.StructMember{Name: fmt.Sprintf("f%d", i), Type: child()})
		}
		if kind == 40 {
			return &Ydb.Type{Type: &Ydb.Type_VariantType{VariantType: &Ydb.VariantType{
				Type: &Ydb.VariantType_StructItems{StructItems: structure},
			}}}
		}

		return &Ydb.Type{Type: &Ydb.Type_StructType{StructType: structure}}
	case 37, 38:
		key := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}}
		payload := &Ydb.Type{Type: &Ydb.Type_VoidType{}}
		if kind == 37 {
			payload = child()
		}

		return &Ydb.Type{Type: &Ydb.Type_DictType{DictType: &Ydb.DictType{Key: key, Payload: payload}}}
	case 41:
		return &Ydb.Type{Type: &Ydb.Type_TaggedType{TaggedType: &Ydb.TaggedType{Tag: "tag", Type: child()}}}
	case 42:
		return &Ydb.Type{Type: &Ydb.Type_VoidType{}}
	case 43:
		return &Ydb.Type{Type: &Ydb.Type_NullType{}}
	case 44:
		return &Ydb.Type{Type: &Ydb.Type_EmptyListType{}}
	case 45:
		return &Ydb.Type{Type: &Ydb.Type_EmptyDictType{}}
	default:
		return &Ydb.Type{Type: &Ydb.Type_PgType{PgType: &Ydb.PgType{Oid: 25}}}
	}
}

func (g *fuzzInput) cell(t *Ydb.Type) *Ydb.Value {
	switch kind := t.GetType().(type) {
	case *Ydb.Type_TypeId:
		return g.primitive(kind.TypeId)
	case *Ydb.Type_OptionalType:
		if g.byte()%3 == 0 {
			return fuzzNull()
		}
		inner := g.cell(kind.OptionalType.GetItem())
		terminal := inner
		for terminal.GetNestedValue() != nil {
			terminal = terminal.GetNestedValue()
		}
		if _, null := terminal.GetValue().(*Ydb.Value_NullFlagValue); null {
			return &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: inner}}
		}

		return inner
	case *Ydb.Type_TaggedType:
		return g.cell(kind.TaggedType.GetType())
	case *Ydb.Type_DecimalType:
		magnitude := new(big.Int).SetUint64(g.number())
		magnitude.Lsh(magnitude, 64).Or(magnitude, new(big.Int).SetUint64(g.number()))
		limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(kind.DecimalType.GetPrecision())), nil)
		magnitude.Mod(magnitude, limit)
		if g.byte()%2 != 0 && magnitude.Sign() != 0 {
			magnitude.Sub(new(big.Int).Lsh(big.NewInt(1), 128), magnitude)
		}
		var encoded [16]byte
		magnitude.FillBytes(encoded[:])

		return &Ydb.Value{
			High_128: binary.BigEndian.Uint64(encoded[:8]),
			Value:    &Ydb.Value_Low_128{Low_128: binary.BigEndian.Uint64(encoded[8:])},
		}
	case *Ydb.Type_VoidType, *Ydb.Type_NullType:
		return fuzzNull()
	case *Ydb.Type_EmptyListType, *Ydb.Type_EmptyDictType:
		return &Ydb.Value{}
	case *Ydb.Type_ListType:
		v := &Ydb.Value{}
		for count := int(g.byte() % 4); count > 0; count-- {
			v.Items = append(v.Items, g.cell(kind.ListType.GetItem()))
		}

		return v
	case *Ydb.Type_TupleType:
		v := &Ydb.Value{}
		for _, item := range kind.TupleType.GetElements() {
			v.Items = append(v.Items, g.cell(item))
		}

		return v
	case *Ydb.Type_StructType:
		v := &Ydb.Value{}
		for _, item := range kind.StructType.GetMembers() {
			v.Items = append(v.Items, g.cell(item.GetType()))
		}

		return v
	case *Ydb.Type_DictType:
		v := &Ydb.Value{}
		seen := make(map[uint64]bool)
		for count := int(g.byte() % 4); count > 0; count-- {
			key, payload := g.cell(kind.DictType.GetKey()), g.cell(kind.DictType.GetPayload())
			if !seen[key.GetUint64Value()] {
				v.Pairs = append(v.Pairs, &Ydb.ValuePair{Key: key, Payload: payload})
				seen[key.GetUint64Value()] = true
			}
		}

		return v
	case *Ydb.Type_VariantType:
		var alternatives []*Ydb.Type
		if tuple := kind.VariantType.GetTupleItems(); tuple != nil {
			alternatives = tuple.GetElements()
		} else {
			for _, item := range kind.VariantType.GetStructItems().GetMembers() {
				alternatives = append(alternatives, item.GetType())
			}
		}
		index := uint32(int(g.byte()) % len(alternatives))

		return &Ydb.Value{VariantIndex: index, Value: &Ydb.Value_NestedValue{NestedValue: g.cell(alternatives[index])}}
	case *Ydb.Type_PgType:
		if g.byte()%3 == 0 {
			return fuzzNull()
		}

		return &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "postgres text"}}
	default:
		panic(fmt.Sprintf("unsupported generated type %T", kind))
	}
}

func fuzzNull() *Ydb.Value { return &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}} }

func (g *fuzzInput) primitive(t Ydb.Type_PrimitiveTypeId) *Ydb.Value {
	n := g.number()
	v := &Ydb.Value{}
	switch t {
	case Ydb.Type_BOOL:
		v.Value = &Ydb.Value_BoolValue{BoolValue: n%2 != 0}
	case Ydb.Type_INT8:
		v.Value = &Ydb.Value_Int32Value{Int32Value: int32(int8(n))}
	case Ydb.Type_INT16:
		v.Value = &Ydb.Value_Int32Value{Int32Value: int32(int16(n))}
	case Ydb.Type_INT32, Ydb.Type_DATE32:
		v.Value = &Ydb.Value_Int32Value{Int32Value: int32(n)}
	case Ydb.Type_UINT8:
		v.Value = &Ydb.Value_Uint32Value{Uint32Value: uint32(uint8(n))}
	case Ydb.Type_UINT16:
		v.Value = &Ydb.Value_Uint32Value{Uint32Value: uint32(uint16(n))}
	case Ydb.Type_UINT32, Ydb.Type_DATE, Ydb.Type_DATETIME:
		v.Value = &Ydb.Value_Uint32Value{Uint32Value: uint32(n)}
	case Ydb.Type_INT64, Ydb.Type_INTERVAL, Ydb.Type_DATETIME64, Ydb.Type_TIMESTAMP64, Ydb.Type_INTERVAL64:
		v.Value = &Ydb.Value_Int64Value{Int64Value: int64(n)}
	case Ydb.Type_UINT64, Ydb.Type_TIMESTAMP:
		v.Value = &Ydb.Value_Uint64Value{Uint64Value: n}
	case Ydb.Type_FLOAT:
		v.Value = &Ydb.Value_FloatValue{FloatValue: math.Float32frombits(uint32(n))}
	case Ydb.Type_DOUBLE:
		v.Value = &Ydb.Value_DoubleValue{DoubleValue: math.Float64frombits(n)}
	case Ydb.Type_UUID:
		v.Value = &Ydb.Value_Low_128{Low_128: n}
		v.High_128 = g.number()
	case Ydb.Type_STRING:
		v.Value = &Ydb.Value_BytesValue{BytesValue: g.bytes()}
	case Ydb.Type_YSON:
		v.Value = &Ydb.Value_BytesValue{BytesValue: [][]byte{[]byte("[]"), []byte("#"), []byte("{a=1;}")}[n%3]}
	default:
		var text string
		switch t {
		case Ydb.Type_UTF8:
			text = string(bytes.ToValidUTF8(g.bytes(), []byte("�")))
		case Ydb.Type_JSON, Ydb.Type_JSON_DOCUMENT:
			text = fmt.Sprintf(`{"value":%d}`, n)
		case Ydb.Type_DYNUMBER:
			text = fmt.Sprintf("%d", n)
		case Ydb.Type_TZ_DATE, 0x44:
			text = "2000-01-01,Europe/Moscow"
		case Ydb.Type_TZ_DATETIME, 0x45:
			text = "2000-01-01T12:34:56,Europe/Moscow"
		case Ydb.Type_TZ_TIMESTAMP, 0x46:
			text = "2000-01-01T12:34:56.123456,Europe/Moscow"
		}
		v.Value = &Ydb.Value_TextValue{TextValue: text}
	}

	return v
}
