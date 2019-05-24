package table

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	ydb "github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/internal"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"
)

// Result is a result of a query.
//
// Use NextSet(), NextRow() and NextItem() to advance through the result sets,
// its rows and row's items.
//
//     res, err := s.Execute(ctx, txc, "SELECT ...")
//     defer res.Close()
//     for res.NextSet() {
//         for res.NextRow() {
//             var id int64
//             var name string
//             res.NextItem()
//             id = res.OInt64()  // Optional<Int64> type.
//             name = res.OUTF8() // Optional<Utf8> type.
//         }
//     }
//     if err := res.Err() { // get any error encountered during iteration
//         // handle error
//     }
//
// Note that value getters (res.OInt64() and res.OUTF8() as in the example
// above) may fail the result iteration. That is, if current value under scan
// is not of requested type, then appropriate zero value will be returned from
// getter and res.Err() become non-nil. After that, NextSet(), NextRow() and
// NextItem() will return false.
type Result struct {
	sets []*Ydb.ResultSet
	set  *Ydb.ResultSet
	row  *Ydb.Value

	closed bool

	setCh       chan *Ydb.ResultSet
	setChErr    *error
	setChCancel func()

	stack scanStack

	nextSet        int
	nextRow        int
	nextItem       int
	setColumnIndex map[string]int

	err error
}

// SetCount returns number of result sets.
// Note that it does not work if r is the result of streaming operation.
func (r *Result) SetCount() int {
	return len(r.sets)
}

// RowCount returns the number of rows among the all result sets.
func (r *Result) RowCount() (n int) {
	for _, s := range r.sets {
		n += len(s.Rows)
	}
	return
}

// SetRowCount returns number of rows in the current result set.
func (r *Result) SetRowCount() int {
	if r.set == nil {
		return 0
	}
	return len(r.set.Rows)
}

// SetRowItemCount returns number of items in the current row.
func (r *Result) SetRowItemCount() int {
	if r.row == nil {
		return 0
	}
	return len(r.row.Items)
}

// Close closes the Result, preventing further iteration.
func (r *Result) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.setCh != nil {
		r.setChCancel()
	}
	return nil
}

func (r *Result) inactive() bool {
	return r.err != nil || r.closed
}

// HasNextSet reports whether result set may be advanced.
//
// It may be useful to call HasNextSet() instead of NextSet() to look ahead
// without advancing the result set.
func (r *Result) HasNextSet() bool {
	if r.inactive() || r.nextSet == len(r.sets) {
		return false
	}
	return true
}

// NextSet selects next result set in the result.
// It returns false if there are no more result sets.
func (r *Result) NextSet() bool {
	if !r.HasNextSet() {
		return false
	}
	r.set = r.sets[r.nextSet]
	r.nextSet++

	r.resetSet()

	return true
}

// NextStreamSet selects next result set from the result of streaming operation.
// It returns false if stream is closed or ctx is canceled.
// Note that in case of context cancelation it does not marks whole result as
// failed.
func (r *Result) NextStreamSet(ctx context.Context) bool {
	if r.inactive() || r.setCh == nil {
		return false
	}
	select {
	case s, ok := <-r.setCh:
		if !ok {
			r.err = *r.setChErr
			return false
		}
		r.set = s

	case <-ctx.Done():
		return false
	}

	r.resetSet()

	return true
}

func (r *Result) resetSet() {
	r.row = nil
	r.nextRow = 0
	r.nextItem = 0
	r.setColumnIndex = nil
	r.stack.reset()
}

// HasNextRow reports whether result row may be advanced.
//
// It may be useful to call HasNextRow() instead of NextRow() to look ahead
// without advancing the result rows.
func (r *Result) HasNextRow() bool {
	if r.inactive() ||
		r.set == nil ||
		r.nextRow == len(r.set.Rows) {

		return false
	}
	return true
}

// NextRow selects next row in the current result set.
// It returns false if there are no more rows in the result set.
func (r *Result) NextRow() bool {
	if !r.HasNextRow() {
		return false
	}
	r.row = r.set.Rows[r.nextRow]
	r.nextRow++
	r.nextItem = 0
	r.stack.reset()

	return true
}

// NextItem selects next item to parse in the current row.
// It returns false if there are no more items in the row.
//
// Note that NextItem() differs from NextRow() and NextSet() – if it return
// false it fails the Result such that no further operations may be processed.
// That is, res.Err() becomes non-nil.
func (r *Result) NextItem() bool {
	i := r.nextItem
	if r.inactive() ||
		r.set == nil ||
		r.row == nil ||
		i >= len(r.row.Items) {

		r.noValueError()
		return false
	}
	r.nextItem = i + 1

	r.stack.reset()
	r.stack.set(item{
		name: r.set.Columns[i].Name,
		i:    i,
		t:    r.set.Columns[i].Type,
		v:    r.row.Items[i],
	})

	return true
}

// SeekItem finds the column with given name in the result set and selects
// appropriate item to parse in the current row.
func (r *Result) SeekItem(name string) bool {
	if r.inactive() ||
		r.set == nil ||
		r.row == nil {

		r.errorf("no value for %q column", name)
		return false
	}
	if r.setColumnIndex == nil {
		r.indexSetColumns()
	}
	i, ok := r.setColumnIndex[name]
	if !ok {
		r.errorf("no such column: %q", name)
		return false
	}
	r.nextItem = i + 1

	r.stack.reset()
	r.stack.set(item{
		name: r.set.Columns[i].Name,
		i:    i,
		t:    r.set.Columns[i].Type,
		v:    r.row.Items[i],
	})

	return true
}

func (r *Result) Path() string {
	var buf bytes.Buffer
	r.WritePathTo(&buf)
	return buf.String()
}

func (r *Result) WritePathTo(w io.Writer) (n int64, err error) {
	for sp := 0; sp < r.stack.size(); sp++ {
		if sp > 0 {
			m, err := io.WriteString(w, ".")
			if err != nil {
				return n, err
			}
			n += int64(m)
		}
		x := r.stack.get(sp)
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

func (r *Result) Type() ydb.Type {
	x := r.stack.current()
	if x.isEmpty() {
		return nil
	}
	return internal.TypeFromYDB(x.t)
}

func (r *Result) itemsIn() int {
	x := r.stack.current()
	if x.isEmpty() {
		return -1
	}
	r.stack.enter()
	return len(x.v.Items)
}

func (r *Result) itemsOut() {
	r.stack.leave()
}

func (r *Result) itemsBoundsCheck(xs []*Ydb.Value, i int) bool {
	return r.boundsCheck(len(xs), i)
}

func (r *Result) pairsBoundsCheck(xs []*Ydb.ValuePair, i int) bool {
	return r.boundsCheck(len(xs), i)
}

func (r *Result) boundsCheck(n, i int) bool {
	if i < 0 || n <= i {
		r.errorf(
			"index out of range: %d; have %d",
			i, n,
		)
		return false
	}
	return true
}

func (r *Result) assertTypeList(typ *Ydb.Type) (t *Ydb.Type_ListType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_ListType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeTuple(typ *Ydb.Type) (t *Ydb.Type_TupleType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TupleType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeStruct(typ *Ydb.Type) (t *Ydb.Type_StructType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_StructType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeDict(typ *Ydb.Type) (t *Ydb.Type_DictType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DictType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeDecimal(typ *Ydb.Type) (t *Ydb.Type_DecimalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DecimalType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypePrimitive(typ *Ydb.Type) (t *Ydb.Type_TypeId) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TypeId); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeOptional(typ *Ydb.Type) (t *Ydb.Type_OptionalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_OptionalType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) assertTypeVariant(typ *Ydb.Type) (t *Ydb.Type_VariantType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_VariantType); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *Result) unwrapVariantType(typ *Ydb.Type_VariantType, index uint32) (name string, t *Ydb.Type) {
	i := int(index)
	switch x := typ.VariantType.Type.(type) {
	case *Ydb.VariantType_TupleItems:
		if i >= len(x.TupleItems.Elements) {
			r.errorf("TODO")
			return
		}
		return "", x.TupleItems.Elements[i]

	case *Ydb.VariantType_StructItems:
		if i >= len(x.StructItems.Members) {
			r.errorf("TODO")
			return
		}
		m := x.StructItems.Members[i]
		return m.Name, m.Type

	default:
		panic("ydb/table: unexpected variant items type")
	}
}

func isOptional(typ *Ydb.Type) bool {
	if typ == nil {
		return false
	}
	_, yes := typ.Type.(*Ydb.Type_OptionalType)
	return yes
}

func (r *Result) isCurrentTypeOptional() bool {
	c := r.stack.current()
	return isOptional(c.t)
}

func (r *Result) isCurrentTypeDecimal() bool {
	c := r.stack.current()
	_, ok := c.t.Type.(*Ydb.Type_DecimalType)
	return ok
}

func (r *Result) assertCurrentTypeNullable() bool {
	c := r.stack.current()
	if isOptional(c.t) {
		return true
	}
	p := r.stack.parent()
	if isOptional(p.t) {
		return true
	}
	r.errorf("not nullable type at %q: %s (%d %s %s)", r.Path(), r.Type(), r.stack.size(), c.t, p.t)
	return false
}

func (r *Result) assertCurrentTypeIs(t ydb.Type) bool {
	c := r.stack.current()
	act := internal.TypeFromYDB(c.t)
	if !internal.TypesEqual(act, t) {
		r.errorf(
			"unexpected type at %q %s: %s; want %s",
			r.Path(), r.Type(), act, t,
		)
		return false
	}
	return true
}

func isEqualDecimal(d *Ydb.DecimalType, t ydb.Type) bool {
	w := t.(internal.DecimalType)
	return d.Precision == w.Precision && d.Scale == w.Scale
}

func (r *Result) assertCurrentTypeDecimal(t ydb.Type) bool {
	d := r.assertTypeDecimal(r.stack.current().t)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		r.decimalTypeError(t)
		return false
	}
	return true
}

func (r *Result) assertCurrentTypeOptionalDecimal(t ydb.Type) bool {
	x := r.assertTypeOptional(r.stack.current().t)
	if x == nil {
		return false
	}
	d := r.assertTypeDecimal(x.OptionalType.Item)
	if d == nil {
		return false
	}
	if !isEqualDecimal(d.DecimalType, t) {
		r.decimalTypeError(t)
		return false
	}
	return true
}

func (r *Result) decimalTypeError(t ydb.Type) {
	r.errorf(
		"unexpected decimal type at %q %s: want %s",
		r.Path(), r.Type(), t,
	)
}

func (r *Result) assertCurrentTypePrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	p := r.assertTypePrimitive(r.stack.current().t)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		r.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}

func (r *Result) assertCurrentTypeOptionalPrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	c := r.stack.current()
	x := r.assertTypeOptional(c.t)
	if x == nil {
		return false
	}
	p := r.assertTypePrimitive(x.OptionalType.Item)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		r.primitiveTypeError(p.TypeId, id)
		return false
	}
	return true
}

func (r *Result) primitiveTypeError(act, exp Ydb.Type_PrimitiveTypeId) {
	r.errorf(
		"unexpected type id at %q %s: %s; want %s",
		r.Path(), r.Type(), act, exp,
	)
}

func (r *Result) pairsIn() int {
	x := r.stack.current()
	if x.isEmpty() {
		return -1
	}
	r.stack.enter()
	return len(x.v.Pairs)
}

func (r *Result) pairsOut() {
	r.stack.leave()
}

// ListIn interprets current item under scan as a ydb's list.
// It returns the size of the nested items.
// If current item under scan is not a list type, it returns -1.
func (r *Result) ListIn() (size int) {
	if r.inactive() {
		return 0
	}
	x := r.stack.current()
	if r.assertTypeList(x.t) != nil {
		return r.itemsIn()
	}
	return 0
}

// ListItem selects current item i-th element as an item to scan.
// ListIn() must be called before.
func (r *Result) ListItem(i int) {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if !r.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := r.assertTypeList(p.t); t != nil {
		r.stack.set(item{
			i: i,
			t: t.ListType.Item,
			v: p.v.Items[i],
		})
	}
}

// ListOut leaves list entered before by ListIn() call.
func (r *Result) ListOut() {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if t := r.assertTypeList(p.t); t != nil {
		r.itemsOut()
	}
}

// TupleIn interprets current item under scan as a ydb's tuple.
// It returns the size of the nested items.
func (r *Result) TupleIn() (size int) {
	if r.inactive() {
		return 0
	}
	x := r.stack.current()
	if r.assertTypeTuple(x.t) != nil {
		return r.itemsIn()
	}
	return 0
}

// TupleItem selects current item i-th element as an item to scan.
// Note that TupleIn() must be called before.
// It panics if i is out of bounds.
func (r *Result) TupleItem(i int) {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if !r.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := r.assertTypeTuple(p.t); t != nil {
		r.stack.set(item{
			i: i,
			t: t.TupleType.Elements[i],
			v: p.v.Items[i],
		})
	}
}

// TupleOut leaves tuple entered before by TupleIn() call.
func (r *Result) TupleOut() {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if t := r.assertTypeTuple(p.t); t != nil {
		r.itemsOut()
	}
}

// StructIn interprets current item under scan as a ydb's struct.
// It returns the size of the nested items – the struct fields values.
// If there is no current item under scan it returns -1.
func (r *Result) StructIn() (size int) {
	if r.inactive() {
		return 0
	}
	x := r.stack.current()
	if r.assertTypeStruct(x.t) != nil {
		return r.itemsIn()
	}
	return 0
}

// StructField selects current item i-th field value as an item to scan.
// Note that StructIn() must be called before.
// It panics if i is out of bounds.
func (r *Result) StructField(i int) (name string) {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if !r.itemsBoundsCheck(p.v.Items, i) {
		return
	}
	if t := r.assertTypeStruct(p.t); t != nil {
		m := t.StructType.Members[i]
		name = m.Name
		r.stack.set(item{
			name: m.Name,
			i:    i,
			t:    m.Type,
			v:    p.v.Items[i],
		})
	}
	return
}

// StructOut leaves struct entered before by StructIn() call.
func (r *Result) StructOut() {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if t := r.assertTypeStruct(p.t); t != nil {
		r.itemsOut()
	}
}

// DictIn interprets current item under scan as a ydb's dict.
// It returns the size of the nested items pairs.
// If there is no current item under scan it returns -1.
func (r *Result) DictIn() (size int) {
	if r.inactive() {
		return 0
	}
	x := r.stack.current()
	if r.assertTypeDict(x.t) != nil {
		return r.pairsIn()
	}
	return 0
}

// DictKey selects current item i-th pair key as an item to scan.
// Note that DictIn() must be called before.
// It panics if i is out of bounds.
func (r *Result) DictKey(i int) {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if !r.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := r.assertTypeDict(p.t); t != nil {
		r.stack.set(item{
			i: i,
			t: t.DictType.Key,
			v: p.v.Pairs[i].Key,
		})
	}
}

// DictPayload selects current item i-th pair value as an item to scan.
// Note that DictIn() must be called before.
// It panics if i is out of bounds.
func (r *Result) DictPayload(i int) {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if !r.pairsBoundsCheck(p.v.Pairs, i) {
		return
	}
	if t := r.assertTypeDict(p.t); t != nil {
		r.stack.set(item{
			i: i,
			t: t.DictType.Payload,
			v: p.v.Pairs[i].Payload,
		})
	}
}

// DictOut leaves dict entered before by DictIn() call.
func (r *Result) DictOut() {
	if r.inactive() {
		return
	}
	p := r.stack.parent()
	if t := r.assertTypeDict(p.t); t != nil {
		r.pairsOut()
	}
}

// Variant unwraps current item under scan interpreting it as Variant<T> type.
// It returns non-empty name of a field that is filled for struct-based
// variant.
// It always returns an index of filled field of a T.
func (r *Result) Variant() (name string, index uint32) {
	if r.inactive() {
		return
	}
	x := r.stack.current()
	t := r.assertTypeVariant(x.t)
	if t == nil {
		return
	}
	v, index := r.variant()
	if v == nil {
		return
	}
	name, typ := r.unwrapVariantType(t, index)
	r.stack.set(item{
		name: name,
		i:    int(index),
		t:    typ,
		v:    v,
	})
	return name, index
}

// Unwrap unwraps current item under scan interpreting it as Optional<T> type.
func (r *Result) Unwrap() {
	if r.inactive() {
		return
	}
	x := r.stack.current()
	t := r.assertTypeOptional(x.t)
	if t == nil {
		return
	}
	v := x.v
	if isOptional(t.OptionalType.Item) {
		v = r.unwrap()
	}
	r.stack.enter()
	r.stack.set(item{
		name: "*",
		t:    t.OptionalType.Item,
		v:    v,
	})
}

// Err returns error caused result to be broken.
func (r *Result) Err() error {
	return r.err
}

// ColumnCount returns number of columns in the current result set.
func (r *Result) ColumnCount() int {
	if r.set == nil {
		return 0
	}
	return len(r.set.Columns)
}

// Columns allows to iterate over all columns of the current result set.
func (r *Result) Columns(it func(Column)) {
	if r.set == nil {
		return
	}
	for _, m := range r.set.Columns {
		it(Column{
			Name: m.Name,
			Type: internal.TypeFromYDB(m.Type),
		})
	}
}

// r.set must be initialized.
func (r *Result) indexSetColumns() {
	r.setColumnIndex = make(map[string]int, len(r.set.Columns))
	for i, m := range r.set.Columns {
		r.setColumnIndex[m.Name] = i
	}
}

func (r *Result) error(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *Result) errorf(f string, args ...interface{}) {
	r.error(fmt.Errorf(f, args...))
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

func (r *Result) typeError(act, exp interface{}) {
	r.errorf(
		"unexpected type during scan at %q %s: %s; want %s",
		r.Path(), r.Type(), nameIface(act), nameIface(exp),
	)
}

func (r *Result) valueTypeError(act, exp interface{}) {
	r.errorf(
		"unexpected value during scan at %q %s: %s; want %s",
		r.Path(), r.Type(), nameIface(act), nameIface(exp),
	)
}

func (r *Result) noValueError() {
	r.errorf(
		"no value at %q",
		r.Path(),
	)
}

func (r *Result) overflowError(i, n interface{}) {
	r.errorf("overflow error: %d overflows capacity of %t", i, n)
}

func (r *Result) null() {
	x, _ := r.stack.currentValue().(*Ydb.Value_NullFlagValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
	}
}
func (r *Result) isNull() bool {
	_, yes := r.stack.currentValue().(*Ydb.Value_NullFlagValue)
	return yes
}

func (r *Result) variant() (v *Ydb.Value, index uint32) {
	v = r.unwrap()
	if v == nil {
		return
	}
	x := r.stack.current() // Is not nil if unwrap succeeded.
	index = x.v.VariantIndex
	return
}

func (r *Result) unwrap() (v *Ydb.Value) {
	x, _ := r.stack.currentValue().(*Ydb.Value_NestedValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.NestedValue
}

func (r *Result) bool() (v bool) {
	x, _ := r.stack.currentValue().(*Ydb.Value_BoolValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.BoolValue
}
func (r *Result) int8() (v int8) {
	d := r.int32()
	if d < math.MinInt8 || math.MaxInt8 < d {
		r.overflowError(d, v)
		return
	}
	return int8(d)
}
func (r *Result) uint8() (v uint8) {
	d := r.uint32()
	if d > math.MaxUint8 {
		r.overflowError(d, v)
		return
	}
	return uint8(d)
}
func (r *Result) int16() (v int16) {
	d := r.int32()
	if d < math.MinInt16 || math.MaxInt16 < d {
		r.overflowError(d, v)
		return
	}
	return int16(d)
}
func (r *Result) uint16() (v uint16) {
	d := r.uint32()
	if d > math.MaxUint16 {
		r.overflowError(d, v)
		return
	}
	return uint16(d)
}
func (r *Result) int32() (v int32) {
	x, _ := r.stack.currentValue().(*Ydb.Value_Int32Value)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.Int32Value
}
func (r *Result) uint32() (v uint32) {
	x, _ := r.stack.currentValue().(*Ydb.Value_Uint32Value)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.Uint32Value
}
func (r *Result) int64() (v int64) {
	x, _ := r.stack.currentValue().(*Ydb.Value_Int64Value)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.Int64Value
}
func (r *Result) uint64() (v uint64) {
	x, _ := r.stack.currentValue().(*Ydb.Value_Uint64Value)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.Uint64Value
}
func (r *Result) float() (v float32) {
	x, _ := r.stack.currentValue().(*Ydb.Value_FloatValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.FloatValue
}
func (r *Result) double() (v float64) {
	x, _ := r.stack.currentValue().(*Ydb.Value_DoubleValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.DoubleValue
}
func (r *Result) bytes() (v []byte) {
	x, _ := r.stack.currentValue().(*Ydb.Value_BytesValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.BytesValue
}
func (r *Result) text() (v string) {
	x, _ := r.stack.currentValue().(*Ydb.Value_TextValue)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.TextValue
}
func (r *Result) low128() (v uint64) {
	x, _ := r.stack.currentValue().(*Ydb.Value_Low_128)
	if x == nil {
		r.valueTypeError(r.stack.currentValue(), x)
		return
	}
	return x.Low_128
}
func (r *Result) uint128() (v [16]byte) {
	c := r.stack.current()
	if c.isEmpty() {
		r.errorf("TODO")
		return
	}
	lo := r.low128()
	hi := c.v.High_128
	return internal.BigEndianUint128(hi, lo)
}

func (r *Result) Bool() (v bool) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_BOOL) {
		return
	}
	return r.bool()
}
func (r *Result) Int8() (v int8) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT8) {
		return
	}
	return r.int8()
}
func (r *Result) Uint8() (v uint8) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT8) {
		return
	}
	return r.uint8()
}
func (r *Result) Int16() (v int16) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT16) {
		return
	}
	return r.int16()
}
func (r *Result) Uint16() (v uint16) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT16) {
		return
	}
	return r.uint16()
}
func (r *Result) Int32() (v int32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT32) {
		return
	}
	return r.int32()
}
func (r *Result) Uint32() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT32) {
		return
	}
	return r.uint32()
}
func (r *Result) Int64() (v int64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT64) {
		return
	}
	return r.int64()
}
func (r *Result) Uint64() (v uint64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT64) {
		return
	}
	return r.uint64()
}
func (r *Result) Float() (v float32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_FLOAT) {
		return
	}
	return r.float()
}
func (r *Result) Double() (v float64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DOUBLE) {
		return
	}
	return r.double()
}
func (r *Result) Date() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DATE) {
		return
	}
	return r.uint32()
}
func (r *Result) Datetime() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DATETIME) {
		return
	}
	return r.uint32()
}
func (r *Result) Timestamp() (v uint64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	return r.uint64()
}
func (r *Result) Interval() (v int64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INTERVAL) {
		return
	}
	return r.int64()
}
func (r *Result) TzDate() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	return r.text()
}
func (r *Result) TzDatetime() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	return r.text()
}
func (r *Result) TzTimestamp() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	return r.text()
}
func (r *Result) String() (v []byte) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_STRING) {
		return
	}
	return r.bytes()
}
func (r *Result) UTF8() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UTF8) {
		return
	}
	return r.text()
}
func (r *Result) YSON() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_YSON) {
		return
	}
	return r.text()
}
func (r *Result) JSON() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_JSON) {
		return
	}
	return r.text()
}
func (r *Result) UUID() (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UUID) {
		return
	}
	return r.uint128()
}

// Decimal returns decimal value represented by big-endian 128 bit signed
// integer.
func (r *Result) Decimal(t ydb.Type) (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypeDecimal(t) {
		return
	}
	return r.uint128()
}

// UnwrapDecimal returns decimal value represented by big-endian 128 bit signed
// integer and its type information.
func (r *Result) UnwrapDecimal() (v [16]byte, precision, scale uint32) {
	d := r.assertTypeDecimal(r.stack.current().t)
	if d == nil {
		return
	}
	return r.uint128(), d.DecimalType.Precision, d.DecimalType.Scale
}

func (r *Result) OBool() (v bool) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_BOOL) {
		return
	}
	if r.isNull() {
		return
	}
	return r.bool()
}
func (r *Result) OInt8() (v int8) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int8()
}
func (r *Result) OUint8() (v uint8) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint8()
}
func (r *Result) OInt16() (v int16) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT16) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int16()
}
func (r *Result) OUint16() (v uint16) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT16) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint16()
}
func (r *Result) OInt32() (v int32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT32) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int32()
}
func (r *Result) OUint32() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT32) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}
func (r *Result) OInt64() (v int64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT64) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int64()
}
func (r *Result) OUint64() (v uint64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT64) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint64()
}
func (r *Result) OFloat() (v float32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_FLOAT) {
		return
	}
	if r.isNull() {
		return
	}
	return r.float()
}
func (r *Result) ODouble() (v float64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DOUBLE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.double()
}
func (r *Result) ODate() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}
func (r *Result) ODatetime() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATETIME) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}
func (r *Result) OTimestamp() (v uint64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint64()
}
func (r *Result) OInterval() (v int64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INTERVAL) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int64()
}
func (r *Result) OTzDate() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OTzDatetime() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OTzTimestamp() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OString() (v []byte) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_STRING) {
		return
	}
	if r.isNull() {
		return
	}
	return r.bytes()
}
func (r *Result) OUTF8() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UTF8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OYSON() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_YSON) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OJSON() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}
func (r *Result) OUUID() (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UUID) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint128()
}
func (r *Result) ODecimal(t ydb.Type) (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypeOptionalDecimal(t) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint128()
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
func (r *Result) Any() interface{} {
	x := r.stack.current()
	if r.inactive() || x.isEmpty() {
		return nil
	}

	if r.IsNull() {
		return nil
	}

	t := internal.TypeFromYDB(x.t)
	p, primitive := t.(internal.PrimitiveType)
	if !primitive {
		return nil
	}

	switch p {
	case internal.TypeBool:
		return r.bool()
	case internal.TypeInt8:
		return r.int8()
	case internal.TypeUint8:
		return r.uint8()
	case internal.TypeInt16:
		return r.int16()
	case internal.TypeUint16:
		return r.uint16()
	case internal.TypeInt32:
		return r.int32()
	case internal.TypeFloat:
		return r.float()
	case internal.TypeDouble:
		return r.double()
	case internal.TypeString:
		return r.bytes()
	case internal.TypeUUID:
		return r.uint128()
	case
		internal.TypeUint32,
		internal.TypeDate,
		internal.TypeDatetime:
		return r.uint32()
	case
		internal.TypeUint64,
		internal.TypeTimestamp:
		return r.uint64()
	case
		internal.TypeInt64,
		internal.TypeInterval:
		return r.int64()
	case
		internal.TypeTzDate,
		internal.TypeTzDatetime,
		internal.TypeTzTimestamp,
		internal.TypeUTF8,
		internal.TypeYSON,
		internal.TypeJSON:
		return r.text()
	default:
		panic("ydb/table: unknown primitive type")
	}
}

func (r *Result) AssertType(t ydb.Type) bool {
	return r.assertCurrentTypeIs(t)
}

func (r *Result) Null() {
	if r.inactive() || !r.assertCurrentTypeNullable() {
		return
	}
	r.null()
}

func (r *Result) IsNull() bool {
	if r.inactive() {
		return false
	}
	return r.isNull()
}

func (r *Result) IsOptional() bool {
	if r.inactive() {
		return false
	}
	return r.isCurrentTypeOptional()
}

func (r *Result) IsDecimal() bool {
	if r.inactive() {
		return false
	}
	return r.isCurrentTypeDecimal()
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
