package value

import (
	"bytes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/exp/allocator"
)

type PrimitiveType uint

func (v PrimitiveType) toString(buffer *bytes.Buffer) {
	buffer.WriteString(primitiveString[v])
}

func (v PrimitiveType) String() string {
	buf := bytesPool.Get()
	defer bytesPool.Put(buf)
	v.toString(buf)
	return buf.String()
}

const (
	TypeUnknown PrimitiveType = iota
	TypeBool
	TypeInt8
	TypeUint8
	TypeInt16
	TypeUint16
	TypeInt32
	TypeUint32
	TypeInt64
	TypeUint64
	TypeFloat
	TypeDouble
	TypeDate
	TypeDatetime
	TypeTimestamp
	TypeInterval
	TypeTzDate
	TypeTzDatetime
	TypeTzTimestamp
	TypeString
	TypeUTF8
	TypeYSON
	TypeJSON
	TypeUUID
	TypeJSONDocument
	TypeDyNumber
)

var primitive = [...]*Ydb.Type{
	TypeBool:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
	TypeInt8:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8}},
	TypeUint8:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8}},
	TypeInt16:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}},
	TypeUint16:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16}},
	TypeInt32:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
	TypeUint32:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}},
	TypeInt64:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
	TypeUint64:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
	TypeFloat:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}},
	TypeDouble:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
	TypeDate:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}},
	TypeDatetime:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME}},
	TypeTimestamp:    {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}},
	TypeInterval:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INTERVAL}},
	TypeTzDate:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATE}},
	TypeTzDatetime:   {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATETIME}},
	TypeTzTimestamp:  {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_TIMESTAMP}},
	TypeString:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
	TypeUTF8:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
	TypeYSON:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_YSON}},
	TypeJSON:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON}},
	TypeUUID:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID}},
	TypeJSONDocument: {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT}},
	TypeDyNumber:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DYNUMBER}},
}

var primitiveString = [...]string{
	TypeUnknown:      "<unknown>",
	TypeBool:         "Bool",
	TypeInt8:         "Int8",
	TypeUint8:        "Uint8",
	TypeInt16:        "Int16",
	TypeUint16:       "Uint16",
	TypeInt32:        "Int32",
	TypeUint32:       "Uint32",
	TypeInt64:        "Int64",
	TypeUint64:       "Uint64",
	TypeFloat:        "Float",
	TypeDouble:       "Double",
	TypeDate:         "Date",
	TypeDatetime:     "Datetime",
	TypeTimestamp:    "Timestamp",
	TypeInterval:     "Interval",
	TypeTzDate:       "TzDate",
	TypeTzDatetime:   "TzDatetime",
	TypeTzTimestamp:  "TzTimestamp",
	TypeString:       "String",
	TypeUTF8:         "Utf8",
	TypeYSON:         "Yson",
	TypeJSON:         "Json",
	TypeUUID:         "Uuid",
	TypeJSONDocument: "JsonDocument",
	TypeDyNumber:     "DyNumber",
}

func (v PrimitiveType) equalsTo(rhs T) bool {
	vv, ok := rhs.(PrimitiveType)
	if !ok {
		return false
	}
	return v == vv
}

func (v PrimitiveType) toYDB(*allocator.Allocator) *Ydb.Type {
	return primitive[v]
}

func Primitive(t PrimitiveType) PrimitiveType {
	return t
}
