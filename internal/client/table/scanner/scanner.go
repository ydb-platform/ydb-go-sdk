package scanner

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type scanner struct {
	set                      *Ydb.ResultSet
	row                      *Ydb.Value
	converter                *rawConverter
	stack                    scanStack
	nextRow                  int
	nextItem                 int
	ignoreTruncated          bool
	markTruncatedAsRetryable bool

	columnIndexes []int

	errMtx xsync.RWMutex
	err    error
}

// ColumnCount returns number of columns in the current result set.
func (s *scanner) ColumnCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Columns)
}

// Columns allows to iterate over all columns of the current result set.
func (s *scanner) Columns(it func(options.Column)) {
	if s.set == nil {
		return
	}
	for _, m := range s.set.Columns {
		it(options.Column{
			Name: m.Name,
			Type: value.TypeFromYDB(m.Type),
		})
	}
}

// RowCount returns number of rows in the result set.
func (s *scanner) RowCount() int {
	if s.set == nil {
		return 0
	}
	return len(s.set.Rows)
}

// ItemCount returns number of items in the current row.
func (s *scanner) ItemCount() int {
	if s.row == nil {
		return 0
	}
	return len(s.row.Items)
}

// HasNextRow reports whether result row may be advanced.
// It may be useful to call HasNextRow() instead of NextRow() to look ahead
// without advancing the result rows.
func (s *scanner) HasNextRow() bool {
	return s.err == nil && s.set != nil && s.nextRow < len(s.set.Rows)
}

// NextRow selects next row in the current result set.
// It returns false if there are no more rows in the result set.
func (s *scanner) NextRow() bool {
	if !s.HasNextRow() {
		return false
	}
	s.row = s.set.Rows[s.nextRow]
	s.nextRow++
	s.nextItem = 0
	s.stack.reset()

	return true
}

func (s *scanner) preScanChecks(lenValues int) (err error) {
	if s.columnIndexes != nil {
		if len(s.columnIndexes) != lenValues {
			return s.errorf(
				1,
				"scan row failed: count of values and column are different (%d != %d)",
				len(s.columnIndexes),
				lenValues,
			)
		}
	}
	if s.ColumnCount() < lenValues {
		panic(fmt.Sprintf("scan row failed: count of columns less then values (%d < %d)", s.ColumnCount(), lenValues))
	}
	if s.nextItem != 0 {
		panic("scan row failed: double scan per row")
	}
	return s.Err()
}

func (s *scanner) ScanWithDefaults(values ...indexed.Required) (err error) {
	if err = s.preScanChecks(len(values)); err != nil {
		return
	}
	for i := range values {
		if _, ok := values[i].(named.Value); ok {
			panic("dont use NamedValue with ScanWithDefaults. Use ScanNamed instead")
		}
		if s.columnIndexes == nil {
			if err = s.seekItemByID(i); err != nil {
				return
			}
		} else {
			if err = s.seekItemByID(s.columnIndexes[i]); err != nil {
				return
			}
		}
		if s.isCurrentTypeOptional() {
			s.scanOptional(values[i], true)
		} else {
			s.scanRequired(values[i])
		}
	}
	s.nextItem += len(values)
	return s.Err()
}

func (s *scanner) Scan(values ...indexed.RequiredOrOptional) (err error) {
	if err = s.preScanChecks(len(values)); err != nil {
		return
	}
	for i := range values {
		if _, ok := values[i].(named.Value); ok {
			panic("dont use NamedValue with Scan. Use ScanNamed instead")
		}
		if s.columnIndexes == nil {
			if err = s.seekItemByID(i); err != nil {
				return
			}
		} else {
			if err = s.seekItemByID(s.columnIndexes[i]); err != nil {
				return
			}
		}
		if s.isCurrentTypeOptional() {
			s.scanOptional(values[i], false)
		} else {
			s.scanRequired(values[i])
		}
	}
	s.nextItem += len(values)
	return s.Err()
}

func (s *scanner) ScanNamed(namedValues ...named.Value) error {
	if err := s.Err(); err != nil {
		return err
	}
	if s.ColumnCount() < len(namedValues) {
		panic(fmt.Sprintf("scan row failed: count of columns less then values (%d < %d)", s.ColumnCount(), len(namedValues)))
	}
	if s.nextItem != 0 {
		panic("scan row failed: double scan per row")
	}
	for i := range namedValues {
		if err := s.seekItemByName(namedValues[i].Name); err != nil {
			return err
		}
		switch t := namedValues[i].Type; t {
		case named.TypeRequired:
			s.scanRequired(namedValues[i].Value)
		case named.TypeOptional:
			s.scanOptional(namedValues[i].Value, false)
		case named.TypeOptionalWithUseDefault:
			s.scanOptional(namedValues[i].Value, true)
		default:
			panic(fmt.Sprintf("unknown type of named.Value: %d", t))
		}
	}
	s.nextItem += len(namedValues)
	return s.Err()
}

// Truncated returns true if current result set has been truncated by server
func (s *scanner) Truncated() bool {
	if s.set == nil {
		_ = s.errorf(0, "there are no sets in the scanner")
		return false
	}
	return s.set.Truncated
}

// Truncated returns true if current result set has been truncated by server
func (s *scanner) truncated() bool {
	if s.set == nil {
		return false
	}
	return s.set.Truncated
}

// Err returns error caused Scanner to be broken.
func (s *scanner) Err() error {
	s.errMtx.RLock()
	defer s.errMtx.RUnlock()
	if s.err != nil {
		return s.err
	}
	if !s.ignoreTruncated && s.truncated() {
		err := xerrors.Wrap(
			fmt.Errorf("more than %d rows: %w", len(s.set.GetRows()), result.ErrTruncated),
		)
		if s.markTruncatedAsRetryable {
			err = xerrors.Retryable(err)
		}
		return xerrors.WithStackTrace(err)
	}
	return nil
}

// Must not be exported.
func (s *scanner) reset(set *Ydb.ResultSet, columnNames ...string) {
	s.set = set
	s.row = nil
	s.nextRow = 0
	s.nextItem = 0
	s.columnIndexes = nil
	s.setColumnIndexes(columnNames)
	s.stack.reset()
	s.converter = &rawConverter{
		scanner: s,
	}
}

func (s *scanner) path() string {
	buf := xstring.Buffer()
	defer buf.Free()
	_, _ = s.writePathTo(buf)
	return buf.String()
}

func (s *scanner) writePathTo(w io.Writer) (n int64, err error) {
	x := s.stack.current()
	st := x.name
	m, err := io.WriteString(w, st)
	if err != nil {
		return n, xerrors.WithStackTrace(err)
	}
	n += int64(m)
	return n, nil
}

func (s *scanner) getType() types.Type {
	x := s.stack.current()
	if x.isEmpty() {
		return nil
	}
	return value.TypeFromYDB(x.t)
}

func (s *scanner) hasItems() bool {
	return s.err == nil && s.set != nil && s.row != nil
}

func (s *scanner) seekItemByID(id int) error {
	if !s.hasItems() || id >= len(s.set.Columns) {
		return s.notFoundColumnByIndex(id)
	}
	col := s.set.Columns[id]
	s.stack.scanItem.name = col.Name
	s.stack.scanItem.t = col.Type
	s.stack.scanItem.v = s.row.Items[id]
	return nil
}

func (s *scanner) seekItemByName(name string) error {
	if !s.hasItems() {
		return s.notFoundColumnName(name)
	}
	for i, c := range s.set.Columns {
		if name != c.Name {
			continue
		}
		s.stack.scanItem.name = c.Name
		s.stack.scanItem.t = c.Type
		s.stack.scanItem.v = s.row.Items[i]
		return s.Err()
	}
	return s.notFoundColumnName(name)
}

func (s *scanner) setColumnIndexes(columns []string) {
	if columns == nil {
		s.columnIndexes = nil
		return
	}
	s.columnIndexes = make([]int, len(columns))
	for i, col := range columns {
		found := false
		for j, c := range s.set.Columns {
			if c.Name == col {
				s.columnIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			_ = s.noColumnError(col)
			return
		}
	}
}

// Any returns any primitive or optional value.
// Currently, it may return one of these types:
//
//	bool
//	int8
//	uint8
//	int16
//	uint16
//	int32
//	uint32
//	int64
//	uint64
//	float32
//	float64
//	[]byte
//	string
//	[16]byte
//
//nolint:gocyclo
func (s *scanner) any() interface{} {
	x := s.stack.current()
	if s.Err() != nil || x.isEmpty() {
		return nil
	}

	if s.isNull() {
		return nil
	}

	if s.isCurrentTypeOptional() {
		s.unwrap()
		x = s.stack.current()
	}

	t := value.TypeFromYDB(x.t)
	p, primitive := t.(value.PrimitiveType)
	if !primitive {
		return s.value()
	}

	switch p {
	case value.TypeBool:
		return s.bool()
	case value.TypeInt8:
		return s.int8()
	case value.TypeUint8:
		return s.uint8()
	case value.TypeInt16:
		return s.int16()
	case value.TypeUint16:
		return s.uint16()
	case value.TypeInt32:
		return s.int32()
	case value.TypeFloat:
		return s.float()
	case value.TypeDouble:
		return s.double()
	case value.TypeBytes:
		return s.bytes()
	case value.TypeUUID:
		return s.uint128()
	case value.TypeUint32:
		return s.uint32()
	case value.TypeDate:
		return value.DateToTime(s.uint32())
	case value.TypeDatetime:
		return value.DatetimeToTime(s.uint32())
	case value.TypeUint64:
		return s.uint64()
	case value.TypeTimestamp:
		return value.TimestampToTime(s.uint64())
	case value.TypeInt64:
		return s.int64()
	case value.TypeInterval:
		return value.IntervalToDuration(s.int64())
	case value.TypeTzDate:
		src, err := value.TzDateToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.any(): %w", err)
		}
		return src
	case value.TypeTzDatetime:
		src, err := value.TzDatetimeToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.any(): %w", err)
		}
		return src
	case value.TypeTzTimestamp:
		src, err := value.TzTimestampToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.any(): %w", err)
		}
		return src
	case value.TypeText, value.TypeDyNumber:
		return s.text()
	case
		value.TypeYSON,
		value.TypeJSON,
		value.TypeJSONDocument:
		return xstring.ToBytes(s.text())
	default:
		_ = s.errorf(0, "unknown primitive types")
		return nil
	}
}

// Value returns current item under scan as ydb.Value types.
func (s *scanner) value() types.Value {
	x := s.stack.current()
	return value.FromYDB(x.t, x.v)
}

func (s *scanner) isCurrentTypeOptional() bool {
	c := s.stack.current()
	return isOptional(c.t)
}

func (s *scanner) isNull() bool {
	_, yes := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	return yes
}

// unwrap current item under scan interpreting it as Optional<Type> types.
// ignores if type is not optional
func (s *scanner) unwrap() {
	if s.Err() != nil {
		return
	}

	t, _ := s.stack.currentType().(*Ydb.Type_OptionalType)
	if t == nil {
		return
	}

	if isOptional(t.OptionalType.Item) {
		s.stack.scanItem.v = s.unwrapValue()
	}
	s.stack.scanItem.t = t.OptionalType.Item
}

func (s *scanner) unwrapValue() (v *Ydb.Value) {
	x, _ := s.stack.currentValue().(*Ydb.Value_NestedValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.NestedValue
}

func (s *scanner) unwrapDecimal() (v types.Decimal) {
	if s.Err() != nil {
		return
	}
	s.unwrap()
	d := s.assertTypeDecimal(s.stack.current().t)
	if d == nil {
		return
	}
	return types.Decimal{
		Bytes:     s.uint128(),
		Precision: d.DecimalType.Precision,
		Scale:     d.DecimalType.Scale,
	}
}

func (s *scanner) assertTypeDecimal(typ *Ydb.Type) (t *Ydb.Type_DecimalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_DecimalType); t == nil {
		s.typeError(x, t)
	}
	return
}

func (s *scanner) bool() (v bool) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BoolValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BoolValue
}

func (s *scanner) int8() (v int8) {
	d := s.int32()
	if d < math.MinInt8 || math.MaxInt8 < d {
		_ = s.overflowError(d, v)
		return
	}
	return int8(d)
}

func (s *scanner) uint8() (v uint8) {
	d := s.uint32()
	if d > math.MaxUint8 {
		_ = s.overflowError(d, v)
		return
	}
	return uint8(d)
}

func (s *scanner) int16() (v int16) {
	d := s.int32()
	if d < math.MinInt16 || math.MaxInt16 < d {
		_ = s.overflowError(d, v)
		return
	}
	return int16(d)
}

func (s *scanner) uint16() (v uint16) {
	d := s.uint32()
	if d > math.MaxUint16 {
		_ = s.overflowError(d, v)
		return
	}
	return uint16(d)
}

func (s *scanner) int32() (v int32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int32Value
}

func (s *scanner) uint32() (v uint32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint32Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint32Value
}

func (s *scanner) int64() (v int64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Int64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Int64Value
}

func (s *scanner) uint64() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Uint64Value)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Uint64Value
}

func (s *scanner) float() (v float32) {
	x, _ := s.stack.currentValue().(*Ydb.Value_FloatValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.FloatValue
}

func (s *scanner) double() (v float64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_DoubleValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.DoubleValue
}

func (s *scanner) bytes() (v []byte) {
	x, _ := s.stack.currentValue().(*Ydb.Value_BytesValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.BytesValue
}

func (s *scanner) text() (v string) {
	x, _ := s.stack.currentValue().(*Ydb.Value_TextValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.TextValue
}

func (s *scanner) low128() (v uint64) {
	x, _ := s.stack.currentValue().(*Ydb.Value_Low_128)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
		return
	}
	return x.Low_128
}

func (s *scanner) uint128() (v [16]byte) {
	c := s.stack.current()
	if c.isEmpty() {
		_ = s.errorf(0, "not implemented convert to [16]byte")
		return
	}
	lo := s.low128()
	hi := c.v.High_128
	return value.BigEndianUint128(hi, lo)
}

func (s *scanner) null() {
	x, _ := s.stack.currentValue().(*Ydb.Value_NullFlagValue)
	if x == nil {
		s.valueTypeError(s.stack.currentValue(), x)
	}
}

func (s *scanner) setTime(dst *time.Time) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_DATE:
		*dst = value.DateToTime(s.uint32())
	case Ydb.Type_DATETIME:
		*dst = value.DatetimeToTime(s.uint32())
	case Ydb.Type_TIMESTAMP:
		*dst = value.TimestampToTime(s.uint64())
	case Ydb.Type_TZ_DATE:
		src, err := value.TzDateToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.setTime(): %w", err)
		}
		*dst = src
	case Ydb.Type_TZ_DATETIME:
		src, err := value.TzDatetimeToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.setTime(): %w", err)
		}
		*dst = src
	case Ydb.Type_TZ_TIMESTAMP:
		src, err := value.TzTimestampToTime(s.text())
		if err != nil {
			_ = s.errorf(0, "scanner.setTime(): %w", err)
		}
		*dst = src
	default:
		_ = s.errorf(0, "scanner.setTime(): incorrect source types %s", t)
	}
}

func (s *scanner) setString(dst *string) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_UUID:
		src := s.uint128()
		*dst = xstring.FromBytes(src[:])
	case Ydb.Type_UTF8, Ydb.Type_DYNUMBER, Ydb.Type_YSON, Ydb.Type_JSON, Ydb.Type_JSON_DOCUMENT:
		*dst = s.text()
	case Ydb.Type_STRING:
		*dst = xstring.FromBytes(s.bytes())
	default:
		_ = s.errorf(0, "scan row failed: incorrect source types %s", t)
	}
}

func (s *scanner) setByte(dst *[]byte) {
	switch t := s.stack.current().t.GetTypeId(); t {
	case Ydb.Type_UUID:
		src := s.uint128()
		*dst = src[:]
	case Ydb.Type_UTF8, Ydb.Type_DYNUMBER, Ydb.Type_YSON, Ydb.Type_JSON, Ydb.Type_JSON_DOCUMENT:
		*dst = xstring.ToBytes(s.text())
	case Ydb.Type_STRING:
		*dst = s.bytes()
	default:
		_ = s.errorf(0, "scan row failed: incorrect source types %s", t)
	}
}

func (s *scanner) trySetByteArray(v interface{}, optional, def bool) bool {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Ptr {
		if !optional {
			return false
		}
		if s.isNull() {
			rv.Set(reflect.Zero(rv.Type()))
			return true
		}
		if rv.IsZero() {
			nv := reflect.New(rv.Type().Elem())
			rv.Set(nv)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Array {
		return false
	}
	if rv.Type().Elem().Kind() != reflect.Uint8 {
		return false
	}
	if def {
		rv.Set(reflect.Zero(rv.Type()))
		return true
	}
	var dst []byte
	s.setByte(&dst)
	if rv.Len() != len(dst) {
		return false
	}
	reflect.Copy(rv, reflect.ValueOf(dst))
	return true
}

//nolint:gocyclo
func (s *scanner) scanRequired(v interface{}) {
	switch v := v.(type) {
	case *bool:
		*v = s.bool()
	case *int8:
		*v = s.int8()
	case *int16:
		*v = s.int16()
	case *int:
		*v = int(s.int32())
	case *int32:
		*v = s.int32()
	case *int64:
		*v = s.int64()
	case *uint8:
		*v = s.uint8()
	case *uint16:
		*v = s.uint16()
	case *uint32:
		*v = s.uint32()
	case *uint:
		*v = uint(s.uint32())
	case *uint64:
		*v = s.uint64()
	case *float32:
		*v = s.float()
	case *float64:
		*v = s.double()
	case *time.Time:
		s.setTime(v)
	case *time.Duration:
		*v = value.IntervalToDuration(s.int64())
	case *string:
		s.setString(v)
	case *[]byte:
		s.setByte(v)
	case *[16]byte:
		*v = s.uint128()
	case *interface{}:
		*v = s.any()
	case *types.Value:
		*v = s.value()
	case *types.Decimal:
		*v = s.unwrapDecimal()
	case types.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			_ = s.errorf(0, "ydb.Scanner error: %w", err)
		}
	case sql.Scanner:
		err := v.Scan(s.any())
		if err != nil {
			_ = s.errorf(0, "sql.Scanner error: %w", err)
		}
	case json.Unmarshaler:
		var err error
		switch s.getType() {
		case types.TypeJSON:
			err = v.UnmarshalJSON(s.converter.JSON())
		case types.TypeJSONDocument:
			err = v.UnmarshalJSON(s.converter.JSONDocument())
		default:
			_ = s.errorf(0, "ydb required type %T not unsupported for applying to json.Unmarshaler", s.getType())
		}
		if err != nil {
			_ = s.errorf(0, "json.Unmarshaler error: %w", err)
		}
	default:
		ok := s.trySetByteArray(v, false, false)
		if !ok {
			_ = s.errorf(0, "scan row failed: type %T is unknown", v)
		}
	}
}

//nolint:gocyclo
func (s *scanner) scanOptional(v interface{}, defaultValueForOptional bool) {
	if defaultValueForOptional {
		if s.isNull() {
			s.setDefaultValue(v)
		} else {
			s.unwrap()
			s.scanRequired(v)
		}
		return
	}
	switch v := v.(type) {
	case **bool:
		if s.isNull() {
			*v = nil
		} else {
			src := s.bool()
			*v = &src
		}
	case **int8:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int8()
			*v = &src
		}
	case **int16:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int16()
			*v = &src
		}
	case **int32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int32()
			*v = &src
		}
	case **int:
		if s.isNull() {
			*v = nil
		} else {
			src := int(s.int32())
			*v = &src
		}
	case **int64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.int64()
			*v = &src
		}
	case **uint8:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint8()
			*v = &src
		}
	case **uint16:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint16()
			*v = &src
		}
	case **uint32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint32()
			*v = &src
		}
	case **uint:
		if s.isNull() {
			*v = nil
		} else {
			src := uint(s.uint32())
			*v = &src
		}
	case **uint64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint64()
			*v = &src
		}
	case **float32:
		if s.isNull() {
			*v = nil
		} else {
			src := s.float()
			*v = &src
		}
	case **float64:
		if s.isNull() {
			*v = nil
		} else {
			src := s.double()
			*v = &src
		}
	case **time.Time:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src time.Time
			s.setTime(&src)
			*v = &src
		}
	case **time.Duration:
		if s.isNull() {
			*v = nil
		} else {
			src := value.IntervalToDuration(s.int64())
			*v = &src
		}
	case **string:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src string
			s.setString(&src)
			*v = &src
		}
	case **[]byte:
		if s.isNull() {
			*v = nil
		} else {
			s.unwrap()
			var src []byte
			s.setByte(&src)
			*v = &src
		}
	case **[16]byte:
		if s.isNull() {
			*v = nil
		} else {
			src := s.uint128()
			*v = &src
		}
	case **interface{}:
		if s.isNull() {
			*v = nil
		} else {
			src := s.any()
			*v = &src
		}
	case *types.Value:
		*v = s.value()
	case **types.Decimal:
		if s.isNull() {
			*v = nil
		} else {
			src := s.unwrapDecimal()
			*v = &src
		}
	case types.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			_ = s.errorf(0, "ydb.Scanner error: %w", err)
		}
	case sql.Scanner:
		err := v.Scan(s.any())
		if err != nil {
			_ = s.errorf(0, "sql.Scanner error: %w", err)
		}
	case json.Unmarshaler:
		s.unwrap()
		var err error
		switch s.getType() {
		case types.TypeJSON:
			if s.isNull() {
				err = v.UnmarshalJSON(nil)
			} else {
				err = v.UnmarshalJSON(s.converter.JSON())
			}
		case types.TypeJSONDocument:
			if s.isNull() {
				err = v.UnmarshalJSON(nil)
			} else {
				err = v.UnmarshalJSON(s.converter.JSONDocument())
			}
		default:
			_ = s.errorf(0, "ydb optional type %T not unsupported for applying to json.Unmarshaler", s.getType())
		}
		if err != nil {
			_ = s.errorf(0, "json.Unmarshaler error: %w", err)
		}
	default:
		s.unwrap()
		ok := s.trySetByteArray(v, true, false)
		if !ok {
			rv := reflect.TypeOf(v)
			if rv.Kind() == reflect.Ptr && rv.Elem().Kind() == reflect.Ptr {
				_ = s.errorf(0, "scan row failed: type %T is unknown", v)
			} else {
				_ = s.errorf(0, "scan row failed: type %T is not optional! use double pointer or sql.Scanner.", v)
			}
		}
	}
}

func (s *scanner) setDefaultValue(dst interface{}) {
	switch v := dst.(type) {
	case *bool:
		*v = false
	case *int8:
		*v = 0
	case *int16:
		*v = 0
	case *int32:
		*v = 0
	case *int64:
		*v = 0
	case *uint8:
		*v = 0
	case *uint16:
		*v = 0
	case *uint32:
		*v = 0
	case *uint64:
		*v = 0
	case *float32:
		*v = 0
	case *float64:
		*v = 0
	case *time.Time:
		*v = time.Time{}
	case *time.Duration:
		*v = 0
	case *string:
		*v = ""
	case *[]byte:
		*v = nil
	case *[16]byte:
		*v = [16]byte{}
	case *interface{}:
		*v = nil
	case *types.Value:
		*v = s.value()
	case *types.Decimal:
		*v = types.Decimal{}
	case sql.Scanner:
		err := v.Scan(nil)
		if err != nil {
			_ = s.errorf(0, "sql.Scanner error: %w", err)
		}
	case types.Scanner:
		err := v.UnmarshalYDB(s.converter)
		if err != nil {
			_ = s.errorf(0, "ydb.Scanner error: %w", err)
		}
	case json.Unmarshaler:
		err := v.UnmarshalJSON(nil)
		if err != nil {
			_ = s.errorf(0, "json.Unmarshaler error: %w", err)
		}
	default:
		ok := s.trySetByteArray(v, false, true)
		if !ok {
			_ = s.errorf(0, "scan row failed: type %T is unknown", v)
		}
	}
}

func (r *baseResult) SetErr(err error) {
	r.errMtx.WithLock(func() {
		r.err = err
	})
}

func (s *scanner) errorf(depth int, f string, args ...interface{}) error {
	s.errMtx.Lock()
	defer s.errMtx.Unlock()
	if s.err != nil {
		return s.err
	}
	s.err = xerrors.WithStackTrace(fmt.Errorf(f, args...), xerrors.WithSkipDepth(depth+1))
	return s.err
}

func (s *scanner) typeError(act, exp interface{}) {
	_ = s.errorf(
		2,
		"unexpected types during scan at %q %s: %s; want %s",
		s.path(),
		s.getType(),
		nameIface(act),
		nameIface(exp),
	)
}

func (s *scanner) valueTypeError(act, exp interface{}) {
	// unexpected value during scan at \"migration_status\" Int64: NullFlag; want Int64
	_ = s.errorf(
		2,
		"unexpected value during scan at %q %s: %s; want %s",
		s.path(),
		s.getType(),
		nameIface(act),
		nameIface(exp),
	)
}

func (s *scanner) notFoundColumnByIndex(idx int) error {
	return s.errorf(
		2,
		"not found %d column",
		idx,
	)
}

func (s *scanner) notFoundColumnName(name string) error {
	return s.errorf(
		2,
		"not found column '%s'",
		name,
	)
}

func (s *scanner) noColumnError(name string) error {
	return s.errorf(
		2,
		"no column %q",
		name,
	)
}

func (s *scanner) overflowError(i, n interface{}) error {
	return s.errorf(
		2,
		"overflow error: %d overflows capacity of %t",
		i,
		n,
	)
}

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
	v        []item
	p        int
	scanItem item
}

func (s *scanStack) size() int {
	if !s.scanItem.isEmpty() {
		s.set(s.scanItem)
	}
	return s.p + 1
}

func (s *scanStack) get(i int) item {
	return s.v[i]
}

func (s *scanStack) reset() {
	s.scanItem = emptyItem
	s.p = 0
}

func (s *scanStack) enter() {
	// support compatibility
	if !s.scanItem.isEmpty() {
		s.set(s.scanItem)
	}
	s.scanItem = emptyItem
	s.p++
}

func (s *scanStack) leave() {
	s.set(emptyItem)
	if s.p > 0 {
		s.p--
	}
}

func (s *scanStack) set(v item) {
	if s.p == len(s.v) {
		s.v = append(s.v, v)
	} else {
		s.v[s.p] = v
	}
}

func (s *scanStack) parent() item {
	if s.p == 0 {
		return emptyItem
	}
	return s.v[s.p-1]
}

func (s *scanStack) current() item {
	if !s.scanItem.isEmpty() {
		return s.scanItem
	}
	if s.v == nil {
		return emptyItem
	}
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
