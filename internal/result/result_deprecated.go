package result

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal"
)

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Bool() (v bool) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_BOOL) {
		return
	}
	return s.bool()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Int8() (v int8) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT8) {
		return
	}
	return s.int8()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Uint8() (v uint8) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT8) {
		return
	}
	return s.uint8()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Int16() (v int16) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT16) {
		return
	}
	return s.int16()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Uint16() (v uint16) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT16) {
		return
	}
	return s.uint16()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Int32() (v int32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT32) {
		return
	}
	return s.int32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Uint32() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT32) {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Int64() (v int64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INT64) {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Uint64() (v uint64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UINT64) {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Float() (v float32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_FLOAT) {
		return
	}
	return s.float()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Double() (v float64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DOUBLE) {
		return
	}
	return s.double()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Date() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATE) {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Datetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DATETIME) {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Timestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Interval() (v int64) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_INTERVAL) {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) TzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) TzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) TzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) String() (v []byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_STRING) {
		return
	}
	return s.bytes()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) UTF8() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UTF8) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) YSON() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_YSON) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) JSON() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) UUID() (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_UUID) {
		return
	}
	return s.uint128()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) JSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) DyNumber() (v string) {
	if s.err != nil || !s.assertCurrentTypePrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OBool() (v bool) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_BOOL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bool()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OInt8() (v int8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int8()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUint8() (v uint8) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint8()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OInt16() (v int16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int16()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUint16() (v uint16) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT16) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint16()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OInt32() (v int32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUint32() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT32) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OInt64() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUint64() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UINT64) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OFloat() (v float32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_FLOAT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.float()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) ODouble() (v float64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DOUBLE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.double()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) ODate() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) ODatetime() (v uint32) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint32()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OTimestamp() (v uint64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OInterval() (v int64) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_INTERVAL) {
		return
	}
	if s.isNull() {
		return
	}
	return s.int64()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OTzDate() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATE) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OTzDatetime() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_DATETIME) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OTzTimestamp() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_TZ_TIMESTAMP) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OString() (v []byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_STRING) {
		return
	}
	if s.isNull() {
		return
	}
	return s.bytes()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUTF8() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UTF8) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OYSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_YSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OJSON() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OUUID() (v [16]byte) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_UUID) {
		return
	}
	if s.isNull() {
		return
	}
	return s.uint128()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) OJSONDocument() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_JSON_DOCUMENT) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) ODyNumber() (v string) {
	if s.err != nil || !s.assertCurrentTypeOptionalPrimitive(Ydb.Type_DYNUMBER) {
		return
	}
	if s.isNull() {
		return
	}
	return s.text()
}

// Value returns current item under scan as ydb.Value type.
// Deprecated: Use Scan instead
// These methods will be removed at next major release
func (s *Scanner) Value() ydb.Value {
	if s.err != nil {
		return nil
	}
	x := s.stack.current()
	return internal.ValueFromYDB(x.t, x.v)
}
