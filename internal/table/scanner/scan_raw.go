package scanner

import (
	"bytes"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type rawConverter struct {
	*scanner
}

func (s *rawConverter) String() []byte {
	s.unwrap()

	return s.bytes()
}

func (s *rawConverter) HasItems() bool {
	return s.hasItems()
}

func (s *rawConverter) HasNextItem() bool {
	return s.hasItems() && s.nextItem < len(s.row.Items)
}

func (s *rawConverter) Path() string {
	var buf bytes.Buffer
	_, _ = s.WritePathTo(&buf)

	return buf.String()
}

func (s *rawConverter) WritePathTo(w io.Writer) (int64, error) {
	var (
		n   int64
		err error
	)
	for sp := 0; sp < s.stack.size(); sp++ {
		if sp > 0 {
			var m int
			m, err = io.WriteString(w, ".")
			if err != nil {
				return n, xerrors.WithStackTrace(err)
			}
			n += int64(m)
		}
		x := s.stack.get(sp)
		s := x.name
		if s == "" {
			s = strconv.Itoa(x.i)
		}
		var m int
		m, err = io.WriteString(w, s)
		if err != nil {
			return n, xerrors.WithStackTrace(err)
		}
		n += int64(m)
	}

	return n, nil
}

func (s *rawConverter) Type() types.Type {
	return s.getType()
}

func (s *rawConverter) Bool() bool {
	if s.Err() != nil {
		return false
	}
	s.unwrap()

	return s.bool()
}

func (s *rawConverter) Int8() int8 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.int8()
}

func (s *rawConverter) Uint8() uint8 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.uint8()
}

func (s *rawConverter) Int16() int16 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.int16()
}

func (s *rawConverter) Uint16() uint16 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.uint16()
}

func (s *rawConverter) Int32() int32 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.int32()
}

func (s *rawConverter) Uint32() uint32 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.uint32()
}

func (s *rawConverter) Int64() int64 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.int64()
}

func (s *rawConverter) Uint64() uint64 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.uint64()
}

func (s *rawConverter) Float() float32 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.float()
}

func (s *rawConverter) Double() float64 {
	if s.Err() != nil {
		return 0
	}
	s.unwrap()

	return s.double()
}

func (s *rawConverter) Date() time.Time {
	s.unwrap()

	return value.DateToTime(s.uint32())
}

func (s *rawConverter) Datetime() time.Time {
	s.unwrap()

	return value.DatetimeToTime(s.uint32())
}

func (s *rawConverter) Timestamp() time.Time {
	s.unwrap()

	return value.TimestampToTime(s.uint64())
}

func (s *rawConverter) Interval() time.Duration {
	s.unwrap()

	return value.IntervalToDuration(s.int64())
}

func (s *rawConverter) TzDate() time.Time {
	s.unwrap()
	if s.isNull() {
		return time.Time{}
	}
	src, err := value.TzDateToTime(s.text())
	if err != nil {
		_ = s.errorf(0, "rawConverter.TzDate(): %w", err)
	}

	return src
}

func (s *rawConverter) TzDatetime() time.Time {
	s.unwrap()
	if s.isNull() {
		return time.Time{}
	}
	src, err := value.TzDatetimeToTime(s.text())
	if err != nil {
		_ = s.errorf(0, "rawConverter.TzDatetime(): %w", err)
	}

	return src
}

func (s *rawConverter) TzTimestamp() time.Time {
	s.unwrap()
	if s.isNull() {
		return time.Time{}
	}
	src, err := value.TzTimestampToTime(s.text())
	if err != nil {
		_ = s.errorf(0, "rawConverter.TzTimestamp(): %w", err)
	}

	return src
}

func (s *rawConverter) UTF8() string {
	if s.Err() != nil {
		return ""
	}
	s.unwrap()

	return s.text()
}

func (s *rawConverter) YSON() []byte {
	s.unwrap()

	return s.bytes()
}

func (s *rawConverter) JSON() []byte {
	s.unwrap()

	return xstring.ToBytes(s.text())
}

func (s *rawConverter) JSONDocument() []byte {
	s.unwrap()

	return xstring.ToBytes(s.text())
}

func (s *rawConverter) UUID() [16]byte {
	if s.Err() != nil {
		return [16]byte{}
	}
	s.unwrap()

	return s.uint128()
}

func (s *rawConverter) DyNumber() string {
	if s.Err() != nil {
		return ""
	}
	s.unwrap()

	return s.text()
}

func (s *rawConverter) Any() interface{} {
	return s.any()
}

// Value returns current item under scan as ydb.Value types.
func (s *rawConverter) Value() types.Value {
	if s.Err() != nil {
		return nil
	}
	s.unwrap()

	return s.value()
}

func (s *rawConverter) AssertType(t types.Type) bool {
	return s.assertCurrentTypeIs(t)
}

func (s *rawConverter) Null() {
	if s.Err() != nil || !s.assertCurrentTypeNullable() {
		return
	}
	s.null()
}

func (s *rawConverter) IsNull() bool {
	if s.Err() != nil {
		return false
	}

	return s.isNull()
}

func (s *rawConverter) IsOptional() bool {
	if s.Err() != nil {
		return false
	}

	return s.isCurrentTypeOptional()
}

// --------non-primitive---------

func (s *rawConverter) ListIn() int {
	var v int
	if s.Err() != nil {
		return v
	}
	x := s.stack.current()
	if s.assertTypeList(x.t) != nil {
		return s.itemsIn()
	}

	return v
}

func (s *rawConverter) ListItem(i int) {
	if s.Err() != nil {
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

func (s *rawConverter) ListOut() {
	if s.Err() != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeList(p.t); t != nil {
		s.itemsOut()
	}
}

func (s *rawConverter) TupleIn() int {
	var v int
	if s.Err() != nil {
		return v
	}
	x := s.stack.current()
	if s.assertTypeTuple(x.t) != nil {
		return s.itemsIn()
	}

	return v
}

func (s *rawConverter) TupleItem(i int) {
	if s.Err() != nil {
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

func (s *rawConverter) TupleOut() {
	if s.Err() != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeTuple(p.t); t != nil {
		s.itemsOut()
	}
}

func (s *rawConverter) StructIn() int {
	var v int
	if s.Err() != nil {
		return v
	}
	x := s.stack.current()
	if s.assertTypeStruct(x.t) != nil {
		return s.itemsIn()
	}

	return v
}

func (s *rawConverter) StructField(i int) string {
	var name string
	if s.Err() != nil {
		return name
	}
	p := s.stack.parent()
	if !s.itemsBoundsCheck(p.v.Items, i) {
		return name
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

	return name
}

func (s *rawConverter) StructOut() {
	if s.Err() != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeStruct(p.t); t != nil {
		s.itemsOut()
	}
}

func (s *rawConverter) DictIn() int {
	var v int
	if s.Err() != nil {
		return v
	}
	x := s.stack.current()
	if s.assertTypeDict(x.t) != nil {
		return s.pairsIn()
	}

	return v
}

func (s *rawConverter) DictKey(i int) {
	if s.Err() != nil {
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

func (s *rawConverter) DictPayload(i int) {
	if s.Err() != nil {
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

func (s *rawConverter) DictOut() {
	if s.Err() != nil {
		return
	}
	p := s.stack.parent()
	if t := s.assertTypeDict(p.t); t != nil {
		s.pairsOut()
	}
}

func (s *rawConverter) Variant() (name string, index uint32) { //nolint:nonamedreturns //gocritic more important
	if s.Err() != nil {
		return name, index
	}
	x := s.stack.current()
	t := s.assertTypeVariant(x.t)
	if t == nil {
		return name, index
	}
	v, index := s.variant()
	if v == nil {
		return name, index
	}
	name, typ := s.unwrapVariantType(t, index)
	s.stack.scanItem.v = nil
	s.stack.set(item{
		name: name,
		i:    int(index),
		t:    typ,
		v:    v,
	})

	return name, index
}

func (s *rawConverter) Unwrap() {
	if s.Err() != nil {
		return
	}
	x := s.stack.current()
	t := s.assertTypeOptional(x.t)
	if t == nil {
		return
	}
	v := x.v
	if isOptional(t.OptionalType.Item) {
		v = s.unwrapValue()
	}
	s.stack.enter()
	s.stack.set(item{
		name: "*",
		t:    t.OptionalType.Item,
		v:    v,
	})
}

func (s *rawConverter) Decimal(t types.Type) [16]byte {
	v := [16]byte{}
	if s.Err() != nil {
		return v
	}
	s.unwrap()
	if !s.assertCurrentTypeDecimal(t) {
		return v
	}

	return s.uint128()
}

func (s *rawConverter) UnwrapDecimal() types.Decimal {
	v := types.Decimal{}
	if s.Err() != nil {
		return v
	}
	s.unwrap()
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return v
	}

	return types.Decimal{
		Bytes:     s.uint128(),
		Precision: d.DecimalType.Precision,
		Scale:     d.DecimalType.Scale,
	}
}

func (s *rawConverter) IsDecimal() bool {
	if s.Err() != nil {
		return false
	}

	return s.isCurrentTypeDecimal()
}

func isEqualDecimal(d *Ydb.DecimalType, t types.Type) bool {
	w := t.(*value.DecimalType)

	return d.Precision == w.Precision && d.Scale == w.Scale
}

func (s *rawConverter) isCurrentTypeDecimal() bool {
	c := s.stack.current()
	_, ok := c.t.Type.(*Ydb.Type_DecimalType)

	return ok
}

func (s *rawConverter) unwrapVariantType(typ *Ydb.Type_VariantType, index uint32) (string, *Ydb.Type) {
	i := int(index)
	t := Ydb.Type{}
	switch x := typ.VariantType.Type.(type) {
	case *Ydb.VariantType_TupleItems:
		if i >= len(x.TupleItems.Elements) {
			_ = s.errorf(0, "unimplemented")

			return "", &t
		}

		return "", x.TupleItems.Elements[i]

	case *Ydb.VariantType_StructItems:
		if i >= len(x.StructItems.Members) {
			_ = s.errorf(0, "unimplemented")

			return "", &t
		}
		m := x.StructItems.Members[i]

		return m.Name, m.Type

	default:
		panic("unexpected variant items types")
	}
}

func (s *rawConverter) variant() (*Ydb.Value, uint32) {
	v := s.unwrapValue()
	if v == nil {
		return &Ydb.Value{}, 0
	}
	x := s.stack.current() // Is not nil if unwrapValue succeeded.
	index := x.v.VariantIndex

	return v, index
}

func (s *rawConverter) itemsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()

	return len(x.v.Items)
}

func (s *rawConverter) itemsOut() {
	s.stack.leave()
}

func (s *rawConverter) itemsBoundsCheck(xs []*Ydb.Value, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *rawConverter) pairsIn() int {
	x := s.stack.current()
	if x.isEmpty() {
		return -1
	}
	s.stack.enter()

	return len(x.v.Pairs)
}

func (s *rawConverter) pairsOut() {
	s.stack.leave()
}

func (s *rawConverter) pairsBoundsCheck(xs []*Ydb.ValuePair, i int) bool {
	return s.boundsCheck(len(xs), i)
}

func (s *rawConverter) boundsCheck(n, i int) bool {
	if i < 0 || n <= i {
		s.boundsError(n, i)

		return false
	}

	return true
}

func (s *scanner) assertTypeOptional(typ *Ydb.Type) *Ydb.Type_OptionalType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_OptionalType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_OptionalType{}
}

func (s *rawConverter) assertCurrentTypeNullable() bool {
	c := s.stack.current()
	if isOptional(c.t) {
		return true
	}
	p := s.stack.parent()
	if isOptional(p.t) {
		return true
	}
	_ = s.errorf(
		1,
		"not nullable types at %q: %s (%d %s %s)",
		s.Path(),
		s.Type(),
		s.stack.size(),
		c.t,
		p.t,
	)

	return false
}

func (s *rawConverter) assertCurrentTypeIs(t types.Type) bool {
	c := s.stack.current()
	act := value.TypeFromYDB(c.t)
	if !value.TypesEqual(act, t) {
		_ = s.errorf(
			1,
			"unexpected types at %q %s: %s; want %s",
			s.Path(),
			s.Type(),
			act,
			t,
		)

		return false
	}

	return true
}

func (s *rawConverter) assertCurrentTypeDecimal(t types.Type) bool {
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

func (s *rawConverter) assertTypeList(typ *Ydb.Type) *Ydb.Type_ListType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_ListType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_ListType{}
}

func (s *rawConverter) assertTypeTuple(typ *Ydb.Type) *Ydb.Type_TupleType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_TupleType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_TupleType{}
}

func (s *rawConverter) assertTypeStruct(typ *Ydb.Type) *Ydb.Type_StructType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_StructType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_StructType{}
}

func (s *rawConverter) assertTypeDict(typ *Ydb.Type) *Ydb.Type_DictType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_DictType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_DictType{}
}

func (s *rawConverter) assertTypeDecimal(typ *Ydb.Type) *Ydb.Type_DecimalType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_DecimalType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_DecimalType{}
}

func (s *rawConverter) assertTypeVariant(typ *Ydb.Type) *Ydb.Type_VariantType {
	x := typ.Type
	if t, _ := x.(*Ydb.Type_VariantType); t == nil {
		s.typeError(x, t)
	}

	return &Ydb.Type_VariantType{}
}

func (s *rawConverter) boundsError(n, i int) {
	_ = s.errorf(
		1, "index out of range: %d; have %d",
		i, n,
	)
}

func (s *rawConverter) decimalTypeError(t types.Type) {
	_ = s.errorf(
		1, "unexpected decimal types at %q %s: want %s",
		s.Path(), s.getType(), t,
	)
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
