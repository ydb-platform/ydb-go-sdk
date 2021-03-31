package result

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
)

func Reset(s *Scanner, set *Ydb.ResultSet) {
	s.reset(set)
}

func Columns(s *Scanner, it func(name string, typ internal.T)) {
	s.columns(it)
}

type Scanner struct {
	set *Ydb.ResultSet
	row *Ydb.Value

	stack    scanStack
	nextRow  int
	nextItem int

	setColumnIndex map[string]int

	err error
}

// Must not be exported.
func (s *Scanner) reset(set *Ydb.ResultSet) {
	s.set = set
	s.row = nil
	s.nextRow = 0
	s.nextItem = 0
	s.setColumnIndex = nil
	s.stack.reset()
}

// ColumnCount returns number of columns in the current result set.
func (s *Scanner) ColumnCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Columns)
}

// RowCount returns number of rows in the result set.
func (s *Scanner) RowCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Rows)
}

// ItemCount returns number of items in the current row.
func (s *Scanner) ItemCount() int {
	if s.row == nil {
		return 0
	}
	return len(s.row.Items)
}

// columns allows to iterate over all columns of the current result set.
// Must not be exported.
func (s *Scanner) columns(it func(name string, typ internal.T)) {
	if s.set == nil {
		return
	}
	for _, m := range s.set.Columns {
		it(m.Name, internal.TypeFromYDB(m.Type))
	}
}

// ResultSetTruncated returns true if current result set has been truncated by server
func (s *Scanner) ResultSetTruncated() bool {
	return s.set.Truncated
}

// Err returns error caused scanner to be broken.
func (s *Scanner) Err() error {
	return s.err
}

// HasNextRow reports whether result row may be advanced.
//
// It may be useful to call HasNextRow() instead of NextRow() to look ahead
// without advancing the result rows.
func (s *Scanner) HasNextRow() bool {
	return s.err == nil && s.set != nil && s.nextRow < len(s.set.Rows)
}

// NextRow selects next row in the current result set.
// It returns false if there are no more rows in the result set.
func (s *Scanner) NextRow() bool {
	if !s.HasNextRow() {
		return false
	}
	s.row = s.set.Rows[s.nextRow]
	s.nextRow++
	s.nextItem = 0
	s.stack.reset()

	return true
}

func (s *Scanner) HasItems() bool {
	return s.err == nil && s.set != nil && s.row != nil
}

func (s *Scanner) HasNextItem() bool {
	return s.HasItems() && s.nextItem < len(s.row.Items)
}

// NextItem selects next item to parse in the current row.
// It returns false if there are no more items in the row.
//
// Note that NextItem() differs from NextRow() and NextSet() – if it return
// false it fails the Result such that no further operations may be processed.
// That is, res.Err() becomes non-nil.
func (s *Scanner) NextItem() (ok bool) {
	if !s.HasNextItem() {
		s.noValueError()
		return false
	}

	i := s.nextItem
	s.nextItem = i + 1

	s.stack.reset()
	s.stack.set(item{
		name: s.set.Columns[i].Name,
		i:    i,
		t:    s.set.Columns[i].Type,
		v:    s.row.Items[i],
	})

	return true
}

// SeekItem finds the column with given name in the result set and selects
// appropriate item to parse in the current row.
func (s *Scanner) SeekItem(name string) bool {
	if !s.HasItems() {
		s.noValueError()
		return false
	}
	if s.setColumnIndex == nil {
		s.indexSetColumns()
	}
	i, ok := s.setColumnIndex[name]
	if !ok {
		s.noColumnError(name)
		return false
	}
	s.nextItem = i + 1

	s.stack.reset()
	s.stack.set(item{
		name: s.set.Columns[i].Name,
		i:    i,
		t:    s.set.Columns[i].Type,
		v:    s.row.Items[i],
	})

	return true
}

func (s *Scanner) Path() string {
	var buf bytes.Buffer
	_, _ = s.WritePathTo(&buf)
	return buf.String()
}

func (s *Scanner) WritePathTo(w io.Writer) (n int64, err error) {
	for sp := 0; sp < s.stack.size(); sp++ {
		if sp > 0 {
			m, err := io.WriteString(w, ".")
			if err != nil {
				return n, err
			}
			n += int64(m)
		}
		x := s.stack.get(sp)
		s := x.name
		if s == "" {
			s = strconv.Itoa(x.i)
		}
		m, err := io.WriteString(w, s)
		if err != nil {
			return n, err
		}
		n += int64(m)
	}
	return n, nil
}

func (s *Scanner) Type() ydb.Type {
	x := s.stack.current()
	if x.isEmpty() {
		return nil
	}
	return internal.TypeFromYDB(x.t)
}

// ListIn interprets current item under scan as a ydb's list.
// It returns the size of the nested items.
// If current item under scan is not a list type, it returns -1.
func (s *Scanner) ListIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeList(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// ListItem selects current item i-th element as an item to scan.
// ListIn() must be called before.
func (s *Scanner) ListItem(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeList(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.ListType.Item,
			v: p.v.Items[i],
		})
	}
}

// ListOut leaves list entered before by ListIn() call.
func (s *Scanner) ListOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeList(p.t); t != nil {
		s.itemsOut()
	}
}

// TupleIn interprets current item under scan as a ydb's tuple.
// It returns the size of the nested items.
func (s *Scanner) TupleIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeTuple(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// TupleItem selects current item i-th element as an item to scan.
// Note that TupleIn() must be called before.
// It panics if i is out of bounds.
func (s *Scanner) TupleItem(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeTuple(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.TupleType.Elements[i],
			v: p.v.Items[i],
		})
	}
}

// TupleOut leaves tuple entered before by TupleIn() call.
func (s *Scanner) TupleOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeTuple(p.t); t != nil {
		s.itemsOut()
	}
}

// StructIn interprets current item under scan as a ydb's struct.
// It returns the size of the nested items – the struct fields values.
// If there is no current item under scan it returns -1.
func (s *Scanner) StructIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeStruct(x.t) != nil {
		return s.itemsIn()
	}
	return 0
}

// StructField selects current item i-th field value as an item to scan.
// Note that StructIn() must be called before.
// It panics if i is out of bounds.
func (s *Scanner) StructField(i int) (name string) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := s.assertTypeStruct(p.t); t != nil {
		m := t.StructType.Members[i]
		name = m.Name
		s.stack.set(item{
			name: m.Name,
			i:    i,
			t:    m.Type,
			v:    p.v.Items[i],
		})
	}
	return
}

// StructOut leaves struct entered before by StructIn() call.
func (s *Scanner) StructOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeStruct(p.t); t != nil {
		s.itemsOut()
	}
}

// DictIn interprets current item under scan as a ydb's dict.
// It returns the size of the nested items pairs.
// If there is no current item under scan it returns -1.
func (s *Scanner) DictIn() (size int) {
	if s.err != nil {
		return 0
	}
	x := s.stack.current()
	if s.assertTypeDict(x.t) != nil {
		return s.pairsIn()
	}
	return 0
}

// DictKey selects current item i-th pair key as an item to scan.
// Note that DictIn() must be called before.
// It panics if i is out of bounds.
func (s *Scanner) DictKey(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := s.assertTypeDict(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.DictType.Key,
			v: p.v.Pairs[i].Key,
		})
	}
}

// DictPayload selects current item i-th pair value as an item to scan.
// Note that DictIn() must be called before.
// It panics if i is out of bounds.
func (s *Scanner) DictPayload(i int) {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if !s.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := s.assertTypeDict(p.t); t != nil {
		s.stack.set(item{
			i: i,
			t: t.DictType.Payload,
			v: p.v.Pairs[i].Payload,
		})
	}
}

// DictOut leaves dict entered before by DictIn() call.
func (s *Scanner) DictOut() {
	if s.err != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeDict(p.t); t != nil {
		s.pairsOut()
	}
}

// Variant unwraps current item under scan interpreting it as Variant<T> type.
// It returns non-empty name of a field that is filled for struct-based
// variant.
// It always returns an index of filled field of a T.
func (s *Scanner) Variant() (name string, index uint32) {
	if s.err != nil {
		return
	}
	x := s.stack.current()
	t := s.assertTypeVariant(x.t)
	if t == nil {
		return
	}
	v, index := s.variant()
	if v == nil {
		return
	}
	name, typ := s.unwrapVariantType(t, index)
	s.stack.set(item{
		name: name,
		i:    int(index),
		t:    typ,
		v:    v,
	})
	return name, index
}

// Unwrap unwraps current item under scan interpreting it as Optional<T> type.
func (s *Scanner) Unwrap() {
	if s.err != nil {
		return
	}
	x := s.stack.current()
	t := s.assertTypeOptional(x.t)
	if t == nil {
		return
	}
	v := x.v
	if isOptional(t.OptionalType.Item) {
		v = s.unwrap()
	}
	s.stack.enter()
	s.stack.set(item{
		name: "*",
		t:    t.OptionalType.Item,
		v:    v,
	})
}

func (s *Scanner) Bool() (v bool) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_BOOL) {
		return
	}
	return s.bool()
}
func (s *Scanner) Int8() (v int8) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT8) {
		return
	}
	return s.int8()
}
func (s *Scanner) Uint8() (v uint8) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT8) {
		return
	}
	return s.uint8()
}
func (s *Scanner) Int16() (v int16) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT16) {
		return
	}
	return s.int16()
}
func (s *Scanner) Uint16() (v uint16) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT16) {
		return
	}
	return s.uint16()
}
func (s *Scanner) Int32() (v int32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT32) {
		return
	}
	return s.int32()
}
func (s *Scanner) Uint32() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT32) {
		return
	}
	return s.uint32()
}
func (s *Scanner) Int64() (v int64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT64) {
		return
	}
	return s.int64()
}
func (s *Scanner) Uint64() (v uint64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT64) {
		return
	}
	return s.uint64()
}
func (s *Scanner) Float() (v float32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_FLOAT) {
		return
	}
	return s.float()
}
func (s *Scanner) Double() (v float64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DOUBLE) {
		return
	}
	return s.double()
}
func (s *Scanner) Date() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATE) {
		return
	}
	return s.uint32()
}
func (s *Scanner) Datetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATETIME) {
		return
	}
	return s.uint32()
}
func (s *Scanner) Timestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	return s.uint64()
}
func (s *Scanner) Interval() (v int64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INTERVAL) {
		return
	}
	return s.int64()
}
func (s *Scanner) TzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	return s.text()
}
func (s *Scanner) TzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	return s.text()
}
func (s *Scanner) TzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	return s.text()
}
func (s *Scanner) String() (v []byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_STRING) {
		return
	}
	return s.bytes()
}
func (s *Scanner) UTF8() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UTF8) {
		return
	}
	return s.text()
}
func (s *Scanner) YSON() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_YSON) {
		return
	}
	return s.text()
}
func (s *Scanner) JSON() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON) {
		return
	}
	return s.text()
}
func (s *Scanner) UUID() (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UUID) {
		return
	}
	return s.uint128()
}
func (s *Scanner) JSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	return s.text()
}
func (s *Scanner) DyNumber() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	return s.text()
}

// Decimal returns decimal value represented by big-endian 128 bit signed
// integes.
func (s *Scanner) Decimal(t ydb.Type) (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeDecimal(t) {
		return
	}
	return s.uint128()
}

// UnwrapDecimal returns decimal value represented by big-endian 128 bit signed
// integer and its type information.
func (s *Scanner) UnwrapDecimal() (v [16]byte, precision, scale uint32) {
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return
	}
	return s.uint128(), d.DecimalType.Precision, d.DecimalType.Scale
}

func (s *Scanner) OBool() (v bool) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_BOOL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bool()
}
func (s *Scanner) OInt8() (v int8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int8()
}
func (s *Scanner) OUint8() (v uint8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint8()
}
func (s *Scanner) OInt16() (v int16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int16()
}
func (s *Scanner) OUint16() (v uint16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint16()
}
func (s *Scanner) OInt32() (v int32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int32()
}
func (s *Scanner) OUint32() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}
func (s *Scanner) OInt64() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}
func (s *Scanner) OUint64() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}
func (s *Scanner) OFloat() (v float32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_FLOAT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.float()
}
func (s *Scanner) ODouble() (v float64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DOUBLE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.double()
}
func (s *Scanner) ODate() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}
func (s *Scanner) ODatetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}
func (s *Scanner) OTimestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}
func (s *Scanner) OInterval() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INTERVAL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}
func (s *Scanner) OTzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OTzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OTzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OString() (v []byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_STRING) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bytes()
}
func (s *Scanner) OUTF8() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UTF8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OYSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_YSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OJSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) OUUID() (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UUID) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint128()
}
func (s *Scanner) OJSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) ODyNumber() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}
func (s *Scanner) ODecimal(t ydb.Type) (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalDecimal(t) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint128()
}

// Any returns any pritmitive value.
// Currently it may return one of this types:
//
//   bool
//   int8
//   uint8
//   int16
//   uint16
//   int32
//   uint32
//   int64
//   uint64
//   float32
//   float64
//   []byte
//   string
//   [16]byte
//
// Or untyped nil.
func (s *Scanner) Any() interface{} {
	x := s.stack.current()
	if s.err != nil || x.isEmpty() {
		return nil
	}

	if s.IsNull() {
		return nil
	}

	t := internal.TypeFromYDB(x.t)
	p, primitive := t.(internal.PrimitiveType)
	if !primitive {
		return nil
	}

	switch p {
	case internal.TypeBool:
		return s.bool()
	case internal.TypeInt8:
		return s.int8()
	case internal.TypeUint8:
		return s.uint8()
	case internal.TypeInt16:
		return s.int16()
	case internal.TypeUint16:
		return s.uint16()
	case internal.TypeInt32:
		return s.int32()
	case internal.TypeFloat:
		return s.float()
	case internal.TypeDouble:
		return s.double()
	case internal.TypeString:
		return s.bytes()
	case internal.TypeUUID:
		return s.uint128()
	case
		internal.TypeUint32,
		internal.TypeDate,
		internal.TypeDatetime:
		return s.uint32()
	case
		internal.TypeUint64,
		internal.TypeTimestamp:
		return s.uint64()
	case
		internal.TypeInt64,
		internal.TypeInterval:
		return s.int64()
	case
		internal.TypeTzDate,
		internal.TypeTzDatetime,
		internal.TypeTzTimestamp,
		internal.TypeUTF8,
		internal.TypeYSON,
		internal.TypeJSON,
		internal.TypeJSONDocument,
		internal.TypeDyNumber:
		return s.text()
	default:
		panic("ydb/table: unknown primitive type")
	}
}

// Value returns current item under scan as ydb.Value type.
func (s *Scanner) Value() ydb.Value {
	if s.err != nil {
		return nil
	}
	x := s.stack.current()
	return internal.ValueFromYDB(x.t, x.v)
}

func (s *Scanner) AssertType(t ydb.Type) bool {
	return s.assertCurrentTypeIs(t)
}

func (s *Scanner) Null() {
	if s.err != nil || !s.assertCurrentTypeNullable() {
		return
	}
	s.null()
}

func (s *Scanner) IsNull() bool {
	if s.err != nil {
		return false
	}
	return s.isNull()
}

func (s *Scanner) IsOptional() bool {
	if s.err != nil {
		return false
	}
	return s.isCurrentTypeOptional()
}

func (s *Scanner) IsDecimal() bool {
	if s.err != nil {
		return false
	}
	return s.isCurrentTypeDecimal()
}

// s.set must be initialized.
func (s *Scanner) indexSetColumns() {
	s.setColumnIndex = make(map[string]int, len(s.set.Columns))
	for i, m := range s.set.Columns {
		s.setColumnIndex[m.Name] = i
	}
}

func (s *Scanner) itemsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()
	return len(x.v.Items)
}
func (s *Scanner) itemsOut() {
	s.stack.leave()
}
func (s *Scanner) itemsBoundsCheck(xs []*Ydb.Value, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *Scanner) pairsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()
	return len(x.v.Pairs)
}
func (s *Scanner) pairsOut() {
	s.stack.leave()
}
func (s *Scanner) pairsBoundsCheck(xs []*Ydb.ValuePair, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *Scanner) boundsCheck(n, i int) bool {
	if i < 0 || n <= i {
		s.boundsError(n, i)
		return false
	}
	return true
}

func (s *Scanner) assertTypeList(typ *Ydb.Type) (t *Ydb.Type_ListType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_ListType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeTuple(typ *Ydb.Type) (t *Ydb.Type_TupleType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TupleType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeStruct(typ *Ydb.Type) (t *Ydb.Type_StructType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_StructType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeDict(typ *Ydb.Type) (t *Ydb.Type_DictType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DictType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeDecimal(typ *Ydb.Type) (t *Ydb.Type_DecimalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DecimalType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypePrimitive(typ *Ydb.Type) (t *Ydb.Type_TypeId) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TypeId); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeOptional(typ *Ydb.Type) (t *Ydb.Type_OptionalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_OptionalType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertTypeVariant(typ *Ydb.Type) (t *Ydb.Type_VariantType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_VariantType); t == nil {
		s.typeError(x, t)
	}
	return
}
func (s *Scanner) assertCurrentTypeNullable() bool {
	c := s.stack.current()
	if isOptional(c.t) {
		return true
	}
	p := s.stack.parent()
	if isOptional(p.t) {
		return true
	}
	s.errorf("not nullable type at %q: %s (%d %s %s)", s.Path(), s.Type(), s.stack.size(), c.t, p.t)
	return false
}
func (s *Scanner) assertCurrentTypeIs(t ydb.Type) bool {
	c := s.stack.current()
	act := internal.TypeFromYDB(c.t)
	if !internal.TypesEqual(act, t) {
		s.errorf(
			"unexpected type at %q %s: %s; want %s",
			s.Path(), s.Type(), act, t,
		)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypeDecimal(t ydb.Type) bool {
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		s.decimalTypeError(t)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypeOptionalDecimal(t ydb.Type) bool {
	typ := s.stack.current().t
	if t, _ := typ.Type.(*Ydb.Type_OptionalType); t != nil {
		typ = t.OptionalType.Item
	}
	if typ == nil {
		return false
	}
	d := s.assertTypeDecimal(typ)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		s.decimalTypeError(t)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypePrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	p := s.assertTypePrimitive(s.stack.current().t)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		s.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}
func (s *Scanner) assertCurrentTypeOptionalPrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	typ := s.stack.current().t
	if t, _ := typ.Type.(*Ydb.Type_OptionalType); t != nil {
		typ = t.OptionalType.Item
	}
	if typ == nil {
		return false
	}
	p := s.assertTypePrimitive(typ)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		s.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}

func (s *Scanner) unwrapVariantType(typ *Ydb.Type_VariantType, index uint32) (name string, t *Ydb.Type) {
	i := int(index)
	switch x := typ.VariantType.Type.(type) {
	case *Ydb.VariantType_TupleItems:
		if i >= len(x.TupleItems.Elements) {
			s.errorf("TODO")
			return
		}
		return "", x.TupleItems.Elements[i]

	case *Ydb.VariantType_StructItems:
		if i >= len(x.StructItems.Members) {
			s.errorf("TODO")
			return
		}
		m := x.StructItems.Members[i]
		return m.Name, m.Type

	default:
		panic("ydb/table: unexpected variant items type")
	}
}

func (s *Scanner) isCurrentTypeOptional() bool {
	c := s.stack.current()
	return isOptional(c.t)
}
func (s *Scanner) isCurrentTypeDecimal() bool {
	c := s.stack.current()
	_, ok := c.t.Type.(*Ydb.Type_DecimalType)
	return ok
}

func (s *Scanner) errorf(f string, args ...interface{}) {
	if s.err != nil {
		return
	}
	s.err = fmt.Errorf(f, args...)
}

func (s *Scanner) typeError(act, exp interface{}) {
	s.errorf(
		"unexpected type during scan at %q %s: %s; want %s",
		s.Path(), s.Type(), nameIface(act), nameIface(exp),
	)
}
func (s *Scanner) valueTypeError(act, exp interface{}) {
	s.errorf(
		"unexpected value during scan at %q %s: %s; want %s",
		s.Path(), s.Type(), nameIface(act), nameIface(exp),
	)
}
func (s *Scanner) noValueError() {
	s.errorf(
		"no value at %q",
		s.Path(),
	)
}
func (s *Scanner) noColumnError(name string) {
	s.errorf(
		"no column %q at %q",
		name, s.Path(),
	)
}
func (s *Scanner) boundsError(n, i int) {
	s.errorf(
		"index out of range: %d; have %d",
		i, n,
	)
}
func (s *Scanner) overflowError(i, n interface{}) {
	s.errorf("overflow error: %d overflows capacity of %t", i, n)
}
func (s *Scanner) decimalTypeError(t ydb.Type) {
	s.errorf(
		"unexpected decimal type at %q %s: want %s",
		s.Path(), s.Type(), t,
	)
}
func (s *Scanner) primitiveTypeError(act, exp Ydb.Type_PrimitiveTypeId) {
	s.errorf(
		"unexpected type id at %q %s: %s; want %s",
		s.Path(), s.Type(), act, exp,
	)
}

func (s *Scanner) null() {
	x, _ := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
	}
}
func (s *Scanner) isNull() bool {
	_, yes := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	return yes
}

func (s *Scanner) variant() (v *Ydb.Value, index uint32) {
	v = s.unwrap()
	if v == nil {
		return
	}
	x := s.stack.current() // Is not nil if unwrap succeeded.
	index = x.v.VariantIndex
	return
}

func (s *Scanner) unwrap() (v *Ydb.Value) {
	x, _ := s.stack.currentValue().(*Ydb.Value_NestedValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.NestedValue
}

func (s *Scanner) bool() (v bool) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BoolValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BoolValue
}
func (s *Scanner) int8() (v int8) {
	d := s.int32()
	if d < math.MinInt8 || math.MaxInt8 < d {
		s.overflowError(d, v)
		return
	}
	return int8(d)
}
func (s *Scanner) uint8() (v uint8) {
	d := s.uint32()
	if d > math.MaxUint8 {
		s.overflowError(d, v)
		return
	}
	return uint8(d)
}
func (s *Scanner) int16() (v int16) {
	d := s.int32()
	if d < math.MinInt16 || math.MaxInt16 < d {
		s.overflowError(d, v)
		return
	}
	return int16(d)
}
func (s *Scanner) uint16() (v uint16) {
	d := s.uint32()
	if d > math.MaxUint16 {
		s.overflowError(d, v)
		return
	}
	return uint16(d)
}
func (s *Scanner) int32() (v int32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int32Value
}
func (s *Scanner) uint32() (v uint32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint32Value
}
func (s *Scanner) int64() (v int64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int64Value
}
func (s *Scanner) uint64() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint64Value
}
func (s *Scanner) float() (v float32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_FloatValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.FloatValue
}
func (s *Scanner) double() (v float64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_DoubleValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.DoubleValue
}
func (s *Scanner) bytes() (v []byte) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BytesValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BytesValue
}
func (s *Scanner) text() (v string) {
	x, _ := s.stack.currentValue().(*Ydb.Value_TextValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.TextValue
}
func (s *Scanner) low128() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Low_128)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Low_128
}
func (s *Scanner) uint128() (v [16]byte) {
	c := s.stack.current()
	if c.isEmpty() {
		s.errorf("TODO")
		return
	}
	lo := s.low128()
	hi := c.v.High_128
	return internal.BigEndianUint128(hi, lo)
}

const tinyStack = 8

var emptyStack [tinyStack]item
var emptyItem item

type item struct {
	name string
	i    int // Index in listing types.
	t    *Ydb.Type
	v    *Ydb.Value
}

func (x item) isEmpty() bool {
	return x.v == nil
}

type scanStack struct {
	v [tinyStack]item
	p int8
}

func (s *scanStack) size() int {
	return int(s.p) + 1
}

func (s *scanStack) get(i int) item {
	return s.v[i]
}

func (s *scanStack) reset() {
	s.v = emptyStack
	s.p = 0
}

func (s *scanStack) enter() {
	s.p++
}

func (s *scanStack) leave() {
	s.set(emptyItem)
	if s.p > 0 {
		s.p--
	}
}

func (s *scanStack) set(v item) {
	s.v[s.p] = v
}

func (s *scanStack) parent() item {
	if s.p == 0 {
		return emptyItem
	}
	return s.v[s.p-1]
}

func (s *scanStack) current() item {
	return s.v[s.p]
}

func (s *scanStack) currentValue() interface{} {
	if v := s.current().v; v != nil {
		return v.Value
	}
	return nil
}

func (s *scanStack) currentType() interface{} {
	if t := s.current().t; t != nil {
		return t.Type
	}
	return nil
}

func isOptional(typ *Ydb.Type) bool {
	if typ == nil {
		return false
	}
	_, yes := typ.Type.(*Ydb.Type_OptionalType)
	return yes
}

func isEqualDecimal(d *Ydb.DecimalType, t ydb.Type) bool {
	w := t.(internal.DecimalType)
	return d.Precision == w.Precision && d.Scale == w.Scale
}

func nameIface(v interface{}) string {
	if v == nil {
		return "nil"
	}
	t := reflect.TypeOf(v)
	s := t.String()
	s = strings.TrimPrefix(s, "*Ydb.Value_")
	s = strings.TrimSuffix(s, "Value")
	s = strings.TrimPrefix(s, "*Ydb.Type_")
	s = strings.TrimSuffix(s, "Type")
	return s
}
