package s3internal

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb_S3Internal"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal"
)

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

func (x item) value() interface{} {
	if v := x.v; v != nil {
		return v.Value
	}
	return nil
}

// S3ListingResult is a result of a listing query.
// Result has 2 separate result sets:
//  - Prefixes - strings with common list prefixes
//  - Contents - rowset with full rows selected by request
// S3ListingResult has no methods for iteration through lists, structures, etc.
type S3ListingResult struct {
	res         *Ydb_S3Internal.S3ListingResult
	contentsRow *Ydb.Value

	nextContentsRow     int
	nextContentsItem    int
	contentsColumnIndex map[string]int

	nextPrefixesRow int
	currentPrefix   string

	currentItem item
	isUnwrapped bool

	closed bool
	err    error
}

// Err returns error caused result to be broken.
func (r *S3ListingResult) Err() error {
	return r.err
}

// PrefixesCount returns the number of common prefixes in result.
func (r *S3ListingResult) PrefixesCount() int {
	return len(r.res.CommonPrefixes.Rows)
}

// NextPrefix changes current prefix to next record. Returns false if no more prefixes in result
func (r *S3ListingResult) NextPrefix() bool {
	if r.inactive() || r.nextPrefixesRow == len(r.res.CommonPrefixes.Rows) {
		return false
	}
	prefRow := r.res.CommonPrefixes.Rows[r.nextPrefixesRow]
	prefValue := internal.PrimitiveFromYDB(prefRow.Items[0])
	if prefValue == nil {
		r.prefixTypeError(prefRow.Items[0], prefValue)
		return false
	}
	r.currentPrefix, _ = prefValue.(string)
	r.nextPrefixesRow++
	return true
}

// Prefix returns current selected prefix. Use NextPrefix for iteration
func (r *S3ListingResult) Prefix() string {
	if r.inactive() {
		return ""
	}
	return r.currentPrefix
}

// ContentsCount returns the number of contents rows in result.
func (r *S3ListingResult) ContentsCount() int {
	return len(r.res.Contents.Rows)
}

// ContentsRowItemCount returns number of items in the current contents row.
func (r *S3ListingResult) ContentsRowItemCount() int {
	if r.contentsRow == nil {
		return 0
	}
	return len(r.contentsRow.Items)
}

// ContentsColumnCount returns number of columns in contents result set.
func (r *S3ListingResult) ContentsColumnCount() int {
	return len(r.res.Contents.Columns)
}

// Close closes the S3ListingResult, preventing further iteration.
func (r *S3ListingResult) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return nil
}

// Path return string name of currently selected item
func (r *S3ListingResult) Path() string {
	return r.currentItem.name
}

// WritePathTo writes string name of currently selected item to writer passed in param
func (r *S3ListingResult) WritePathTo(w io.Writer) (int64, error) {
	m, err := io.WriteString(w, r.currentItem.name)
	return int64(m), err
}

// NextContentsRow selects next row in the contents result set.
// It returns false if there are no more rows.
func (r *S3ListingResult) NextContentsRow() bool {
	if r.inactive() || r.nextContentsRow == len(r.res.Contents.Rows) {
		return false
	}
	r.contentsRow = r.res.Contents.Rows[r.nextContentsRow]
	r.nextContentsRow++
	r.nextContentsItem = 0
	r.resetItem()

	return true
}

// NextContentsItem selects next item to parse in the current row.
// It returns false if there are no more items in the row.
// Note that NextContentsItem() differs from NextRow() and NextSet() - if it return
// false it fails the Result such that no further operations may be processed.
// That is, res.Err() becomes non-nil.
func (r *S3ListingResult) NextContentsItem() bool {
	i := r.nextContentsItem
	if r.inactive() ||
		r.contentsRow == nil ||
		i >= len(r.contentsRow.Items) {

		r.noValueError()
		return false
	}
	r.nextContentsItem++

	r.resetItem()
	r.currentItem = item{
		name: r.res.Contents.Columns[i].Name,
		i:    i,
		t:    r.res.Contents.Columns[i].Type,
		v:    r.contentsRow.Items[i],
	}

	return true
}

// SeekContentsItem finds the column with given name in the contents result set and selects
// appropriate item to parse in the current row.
func (r *S3ListingResult) SeekContentsItem(name string) bool {
	if r.inactive() || r.contentsRow == nil {

		r.errorf("no value for %q column", name)
		return false
	}
	if r.contentsColumnIndex == nil {
		r.indexContentsColumns()
	}
	i, ok := r.contentsColumnIndex[name]
	if !ok {
		r.errorf("no such column: %q", name)
		return false
	}
	r.nextContentsItem = i + 1

	r.resetItem()
	r.currentItem = item{
		name: r.res.Contents.Columns[i].Name,
		i:    i,
		t:    r.res.Contents.Columns[i].Type,
		v:    r.contentsRow.Items[i],
	}

	return true
}

// Type returns YDB type of currently selected item
func (r *S3ListingResult) Type() ydb.Type {
	x := r.currentItem
	if x.isEmpty() {
		return nil
	}
	return internal.TypeFromYDB(x.t)
}

// Unwrap unwraps current item under scan interpreting it as Optional<T> type.
func (r *S3ListingResult) Unwrap() {
	if r.inactive() {
		return
	}
	x := r.currentItem
	t := r.assertTypeOptional(x.t)
	if t == nil {
		return
	}
	v := x.v
	if isOptional(t.OptionalType.Item) {
		v = r.unwrap()
	}
	r.isUnwrapped = true
	r.currentItem = item{
		name: "*",
		t:    t.OptionalType.Item,
		v:    v,
	}
}

// Bool returns bool representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Bool() (v bool) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_BOOL) {
		return
	}
	return r.bool()
}

// Int8 returns int8 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Int8() (v int8) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT8) {
		return
	}
	return r.int8()
}

// Uint8 returns uint8 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Uint8() (v uint8) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT8) {
		return
	}
	return r.uint8()
}

// Int16 returns int16 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Int16() (v int16) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT16) {
		return
	}
	return r.int16()
}

// Uint16 returns uint16 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Uint16() (v uint16) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT16) {
		return
	}
	return r.uint16()
}

// Int32 returns int32 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Int32() (v int32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT32) {
		return
	}
	return r.int32()
}

// Uint32 returns uint32 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Uint32() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT32) {
		return
	}
	return r.uint32()
}

// Int64 returns int64 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Int64() (v int64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INT64) {
		return
	}
	return r.int64()
}

// Uint64 returns uint64 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Uint64() (v uint64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UINT64) {
		return
	}
	return r.uint64()
}

// Float returns float32 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Float() (v float32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_FLOAT) {
		return
	}
	return r.float()
}

// Double returns float64 representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Double() (v float64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DOUBLE) {
		return
	}
	return r.double()
}

// Date returns uint32 days from unix epoch for current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Date() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DATE) {
		return
	}
	return r.uint32()
}

// Datetime returns uint32 seconds from unix epoch for current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Datetime() (v uint32) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DATETIME) {
		return
	}
	return r.uint32()
}

// Timestamp returns uint64 microseconds from epoch for current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) Timestamp() (v uint64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	return r.uint64()
}

// Interval returns int64 representation of current item interval in nanoseconds.
// If type does not match then result error is setted.
func (r *S3ListingResult) Interval() (v int64) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_INTERVAL) {
		return
	}
	return r.int64()
}

// TzDate returns string representation of current item date.
// If type does not match then result error is setted.
func (r *S3ListingResult) TzDate() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	return r.text()
}

// TzDatetime returns string representation of current item datetime.
// If type does not match then result error is setted.
func (r *S3ListingResult) TzDatetime() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	return r.text()
}

// TzTimestamp returns string representation of current item timestamp with timezone.
// If type does not match then result error is setted.
func (r *S3ListingResult) TzTimestamp() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	return r.text()
}

// String returns representation of current item as byte slice.
// If type does not match then result error is setted.
func (r *S3ListingResult) String() (v []byte) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_STRING) {
		return
	}
	return r.bytes()
}

// UTF8 returns string representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) UTF8() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UTF8) {
		return
	}
	return r.text()
}

// YSON returns string representation of current item marshaled as YSON.
// If type does not match then result error is setted.
func (r *S3ListingResult) YSON() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_YSON) {
		return
	}
	return r.text()
}

// JSON returns string representation of current item marshaled as JSON.
// If type does not match then result error is setted.
func (r *S3ListingResult) JSON() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_JSON) {
		return
	}
	return r.text()
}

// UUID returns binary uuid representation of current item.
// If type does not match then result error is setted.
func (r *S3ListingResult) UUID() (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_UUID) {
		return
	}
	return r.uint128()
}

// JSONDocument returns string representation of current item marshaled as JSONDocument.
// If type does not match then result error is setted.
func (r *S3ListingResult) JSONDocument() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	return r.text()
}

// DyNumber returns string representation of current item marshaled as DyNumber.
// If type does not match then result error is setted.
func (r *S3ListingResult) DyNumber() (v string) {
	if r.inactive() || !r.assertCurrentTypePrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	return r.text()
}

// OBool returns optional boolean representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OBool() (v bool) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_BOOL) {
		return
	}
	if r.isNull() {
		return
	}
	return r.bool()
}

// OInt8 returns optional int8 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OInt8() (v int8) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int8()
}

// OUint8 returns optional uint8 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUint8() (v uint8) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint8()
}

// OInt16 returns optional int16 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OInt16() (v int16) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT16) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int16()
}

// OUint16 returns optional uint16 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUint16() (v uint16) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT16) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint16()
}

// OInt32 returns optional int32 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OInt32() (v int32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT32) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int32()
}

// OUint32 returns optional uint32 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUint32() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT32) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}

// OInt64 returns optional int64 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OInt64() (v int64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT64) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int64()
}

// OUint64 returns optional uint64 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUint64() (v uint64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT64) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint64()
}

// OFloat returns optional float32 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OFloat() (v float32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_FLOAT) {
		return
	}
	if r.isNull() {
		return
	}
	return r.float()
}

// ODouble returns optional float64 representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) ODouble() (v float64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DOUBLE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.double()
}

// ODate returns optional uint32 days from unix epoch for current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) ODate() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}

// ODatetime returns optional uint32 seconds from unix epoch for current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) ODatetime() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATETIME) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}

// OTimestamp returns optional uint64 microseconds from epoch for current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OTimestamp() (v uint32) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint32()
}

// OInterval returns optional int64 representation of current item interval in nanoseconds.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OInterval() (v int64) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_INTERVAL) {
		return
	}
	if r.isNull() {
		return
	}
	return r.int64()
}

// OTzDate returns optional string representation of current item date.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OTzDate() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OTzDatetime returns optional string representation of current item datetime.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OTzDatetime() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OTzTimestamp returns optional string representation of current item timestamp with timezone.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OTzTimestamp() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OString returns optional representation of current item as bytes slice.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OString() (v []byte) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_STRING) {
		return
	}
	if r.isNull() {
		return
	}
	return r.bytes()
}

// OUTF8 returns optional string representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUTF8() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UTF8) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OYSON returns optional string representation of current item marshaled as YSON.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OYSON() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_YSON) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OJSON returns optional string representation of current item marshaled as JSON.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OJSON() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// OUUID returns optional binary uuid representation of current item.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OUUID() (v [16]byte) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_UUID) {
		return
	}
	if r.isNull() {
		return
	}
	return r.uint128()
}

// OJSONDocument returns optional string representation of current item marshaled as JSONDocument.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) OJSONDocument() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// ODyNumber returns optional string representation of current item marshaled as DyNumber.
// If value is NULL then default type value returned. Use NULL function for check if value is empty.
// If type does not match then result error is setted.
func (r *S3ListingResult) ODyNumber() (v string) {
	if r.inactive() || !r.assertCurrentTypeOptionalPrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	if r.isNull() {
		return
	}
	return r.text()
}

// Any returns any primitive value.
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
func (r *S3ListingResult) Any() interface{} {
	x := r.currentItem
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
		internal.TypeJSON,
		internal.TypeJSONDocument,
		internal.TypeDyNumber:
		return r.text()
	default:
		panic("ydb/table: unknown primitive type")
	}
}

// AssertType check if current item type match passed parameter.
func (r *S3ListingResult) AssertType(t ydb.Type) bool {
	return r.assertCurrentTypeIs(t)
}

// Null checks if current optional value is null and set result error if not.
func (r *S3ListingResult) Null() {
	if r.inactive() || !r.assertCurrentTypeNullable() {
		return
	}
	r.null()
}

// IsNull checks if current optional value is null.
func (r *S3ListingResult) IsNull() bool {
	if r.inactive() {
		return false
	}
	return r.isNull()
}

// IsOptional checks if current item has optional type.
func (r *S3ListingResult) IsOptional() bool {
	if r.inactive() {
		return false
	}
	return r.isCurrentTypeOptional()
}

// Helpers

func isOptional(typ *Ydb.Type) bool {
	if typ == nil {
		return false
	}
	_, yes := typ.Type.(*Ydb.Type_OptionalType)
	return yes
}

func (r *S3ListingResult) resetItem() {
	r.currentItem = emptyItem
	r.isUnwrapped = false
}

func (r *S3ListingResult) inactive() bool {
	return r.err != nil || r.closed
}

func (r *S3ListingResult) indexContentsColumns() {
	r.contentsColumnIndex = make(map[string]int, len(r.res.Contents.Columns))
	for i, m := range r.res.Contents.Columns {
		r.contentsColumnIndex[m.Name] = i
	}
}

func (r *S3ListingResult) noValueError() {
	r.errorf(
		"no value at %q",
		r.Path(),
	)
}

func (r *S3ListingResult) primitiveTypeError(act, exp Ydb.Type_PrimitiveTypeId) {
	r.errorf(
		"unexpected type id at %q %s: %s; want %s",
		r.Path(), r.Type(), act, exp,
	)
}

func (r *S3ListingResult) error(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *S3ListingResult) errorf(f string, args ...interface{}) {
	r.error(fmt.Errorf(f, args...))
}

func (r *S3ListingResult) assertTypePrimitive(typ *Ydb.Type) (t *Ydb.Type_TypeId) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_TypeId); t == nil {
		r.typeError(x, t)
	}
	return
}
func (r *S3ListingResult) assertTypeOptional(typ *Ydb.Type) (t *Ydb.Type_OptionalType) {
	x := typ.Type
	if t, _ = x.(*Ydb.Type_OptionalType); t == nil {
		r.typeError(x, t)
	}
	return
}

//	*Type_DecimalType
//	*Type_OptionalType
//	*Type_VariantType
//	*Type_VoidType

func (r *S3ListingResult) isCurrentTypeOptional() bool {
	c := r.currentItem
	return isOptional(c.t)
}

func (r *S3ListingResult) assertCurrentTypeNullable() bool {
	c := r.currentItem
	if isOptional(c.t) {
		return true
	}
	if r.isUnwrapped {
		return true
	}
	r.errorf("not nullable type at %q: %s (%s)", r.Path(), r.Type(), c.t)
	return false
}

func (r *S3ListingResult) assertCurrentTypeIs(t ydb.Type) bool {
	c := r.currentItem
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

func (r *S3ListingResult) assertCurrentTypeOptionalPrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	c := r.currentItem
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
	}
	return true
}

func (r *S3ListingResult) assertCurrentTypePrimitive(id Ydb.Type_PrimitiveTypeId) bool {
	p := r.assertTypePrimitive(r.currentItem.t)
	if p == nil {
		return false
	}
	if p.TypeId != id {
		r.primitiveTypeError(p.TypeId, id)
	}
	return true
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

func (r *S3ListingResult) typeError(act, exp interface{}) {
	r.errorf(
		"unexpected type during scan at %q %s: %s; want %s",
		r.Path(), r.Type(), nameIface(act), nameIface(exp),
	)
}

func (r *S3ListingResult) overflowError(i, n interface{}) {
	r.errorf("overflow error: %d overflows capacity of %t", i, n)
}

func (r *S3ListingResult) null() {
	x, _ := r.currentItem.value().(*Ydb.Value_NullFlagValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
	}
}

func (r *S3ListingResult) isNull() bool {
	_, yes := r.currentItem.value().(*Ydb.Value_NullFlagValue)
	return yes
}

func (r *S3ListingResult) variant() (v *Ydb.Value, index uint32) {
	v = r.unwrap()
	if v == nil {
		return
	}
	x := r.currentItem // Is not nil if unwrap succeeded.
	index = x.v.VariantIndex
	return
}

func (r *S3ListingResult) unwrap() (v *Ydb.Value) {
	x, _ := r.currentItem.value().(*Ydb.Value_NestedValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.NestedValue
}

func (r *S3ListingResult) valueTypeError(act, exp interface{}) {
	r.errorf(
		"unexpected value during scan at %q %s: %s; want %s",
		r.Path(), r.Type(), nameIface(act), nameIface(exp),
	)
}

func (r *S3ListingResult) prefixTypeError(act, exp interface{}) {
	r.errorf(
		"unexpected prefix value: %s; want %s", nameIface(act), nameIface(exp),
	)
}

// Result item casts
func (r *S3ListingResult) bool() (v bool) {
	x, _ := r.currentItem.value().(*Ydb.Value_BoolValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.BoolValue
}
func (r *S3ListingResult) int8() (v int8) {
	d := r.int32()
	if d < math.MinInt8 || math.MaxInt8 < d {
		r.overflowError(d, v)
		return
	}
	return int8(d)
}
func (r *S3ListingResult) uint8() (v uint8) {
	d := r.uint32()
	if d > math.MaxUint8 {
		r.overflowError(d, v)
		return
	}
	return uint8(d)
}
func (r *S3ListingResult) int16() (v int16) {
	d := r.int32()
	if d < math.MinInt16 || math.MaxInt16 < d {
		r.overflowError(d, v)
		return
	}
	return int16(d)
}
func (r *S3ListingResult) uint16() (v uint16) {
	d := r.uint32()
	if d > math.MaxUint16 {
		r.overflowError(d, v)
		return
	}
	return uint16(d)
}
func (r *S3ListingResult) int32() (v int32) {
	x, _ := r.currentItem.value().(*Ydb.Value_Int32Value)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.Int32Value
}
func (r *S3ListingResult) uint32() (v uint32) {
	x, _ := r.currentItem.value().(*Ydb.Value_Uint32Value)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.Uint32Value
}
func (r *S3ListingResult) int64() (v int64) {
	x, _ := r.currentItem.value().(*Ydb.Value_Int64Value)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.Int64Value
}
func (r *S3ListingResult) uint64() (v uint64) {
	x, _ := r.currentItem.value().(*Ydb.Value_Uint64Value)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.Uint64Value
}
func (r *S3ListingResult) float() (v float32) {
	x, _ := r.currentItem.value().(*Ydb.Value_FloatValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.FloatValue
}
func (r *S3ListingResult) double() (v float64) {
	x, _ := r.currentItem.value().(*Ydb.Value_DoubleValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.DoubleValue
}
func (r *S3ListingResult) bytes() (v []byte) {
	x, _ := r.currentItem.value().(*Ydb.Value_BytesValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.BytesValue
}
func (r *S3ListingResult) text() (v string) {
	x, _ := r.currentItem.value().(*Ydb.Value_TextValue)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.TextValue
}
func (r *S3ListingResult) low128() (v uint64) {
	x, _ := r.currentItem.value().(*Ydb.Value_Low_128)
	if x == nil {
		r.valueTypeError(r.currentItem.value(), x)
		return
	}
	return x.Low_128
}
func (r *S3ListingResult) uint128() (v [16]byte) {
	c := r.currentItem
	if c.isEmpty() {
		r.errorf("TODO")
		return
	}
	lo := r.low128()
	hi := c.v.High_128
	return internal.BigEndianUint128(hi, lo)
}
