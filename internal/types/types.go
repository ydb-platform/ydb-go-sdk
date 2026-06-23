package types

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

type Type interface {
	Yql() string
	String() string

	ToYDB() *Ydb.Type
	equalsTo(rhs Type) bool
}

func TypeToYDB(t Type) *Ydb.Type {
	return t.ToYDB()
}

func TypeFromYDB(x *Ydb.Type) Type {
	switch x.WhichType() {
	case Ydb.Type_TypeId_case:
		return primitiveTypeFromYDB(x.GetTypeId())

	case Ydb.Type_OptionalType_case:
		return NewOptional(TypeFromYDB(x.GetOptionalType().GetItem()))

	case Ydb.Type_ListType_case:
		return NewList(TypeFromYDB(x.GetListType().GetItem()))

	case Ydb.Type_DecimalType_case:
		d := x.GetDecimalType()

		return NewDecimal(d.GetPrecision(), d.GetScale())

	case Ydb.Type_TupleType_case:
		t := x.GetTupleType()

		return NewTuple(FromYDB(t.GetElements())...)

	case Ydb.Type_StructType_case:
		s := x.GetStructType()

		return NewStruct(StructFields(s.GetMembers())...)

	case Ydb.Type_DictType_case:
		keyType, valueType := TypeFromYDB(x.GetDictType().GetKey()), TypeFromYDB(x.GetDictType().GetPayload())
		if valueType.equalsTo(NewVoid()) {
			return NewSet(keyType)
		}

		return NewDict(keyType, valueType)

	case Ydb.Type_VariantType_case:
		t := x.GetVariantType()
		switch t.WhichType() {
		case Ydb.VariantType_TupleItems_case:
			return NewVariantTuple(FromYDB(t.GetTupleItems().GetElements())...)
		case Ydb.VariantType_StructItems_case:
			return NewVariantStruct(StructFields(t.GetStructItems().GetMembers())...)
		default:
			panic("ydb: unknown variant type")
		}

	case Ydb.Type_VoidType_case:
		return NewVoid()

	case Ydb.Type_NullType_case:
		return NewNull()

	case Ydb.Type_PgType_case:
		return &PgType{
			OID: x.GetPgType().GetOid(),
		}

	case Ydb.Type_EmptyListType_case:
		return NewEmptyList()

	case Ydb.Type_EmptyDictType_case:
		return NewEmptyDict()

	default:
		panic(fmt.Sprintf("ydb: unknown type %v", x.WhichType()))
	}
}

//nolint:funlen
func primitiveTypeFromYDB(t Ydb.Type_PrimitiveTypeId) Type {
	switch t {
	case Ydb.Type_BOOL:
		return Bool
	case Ydb.Type_INT8:
		return Int8
	case Ydb.Type_UINT8:
		return Uint8
	case Ydb.Type_INT16:
		return Int16
	case Ydb.Type_UINT16:
		return Uint16
	case Ydb.Type_INT32:
		return Int32
	case Ydb.Type_UINT32:
		return Uint32
	case Ydb.Type_INT64:
		return Int64
	case Ydb.Type_UINT64:
		return Uint64
	case Ydb.Type_FLOAT:
		return Float
	case Ydb.Type_DOUBLE:
		return Double
	case Ydb.Type_DATE:
		return Date
	case Ydb.Type_DATE32:
		return Date32
	case Ydb.Type_DATETIME:
		return Datetime
	case Ydb.Type_DATETIME64:
		return Datetime64
	case Ydb.Type_TIMESTAMP:
		return Timestamp
	case Ydb.Type_TIMESTAMP64:
		return Timestamp64
	case Ydb.Type_INTERVAL:
		return Interval
	case Ydb.Type_INTERVAL64:
		return Interval64
	case Ydb.Type_TZ_DATE:
		return TzDate
	case Ydb.Type_TZ_DATETIME:
		return TzDatetime
	case Ydb.Type_TZ_TIMESTAMP:
		return TzTimestamp
	case Ydb.Type_STRING:
		return Bytes
	case Ydb.Type_UTF8:
		return Text
	case Ydb.Type_YSON:
		return YSON
	case Ydb.Type_JSON:
		return JSON
	case Ydb.Type_UUID:
		return UUID
	case Ydb.Type_JSON_DOCUMENT:
		return JSONDocument
	case Ydb.Type_DYNUMBER:
		return DyNumber
	default:
		panic(fmt.Sprintf("ydb: unexpected type %v", t))
	}
}

func FromYDB(es []*Ydb.Type) []Type {
	ts := make([]Type, len(es))
	for i, el := range es {
		ts[i] = TypeFromYDB(el)
	}

	return ts
}

func Equal(lhs, rhs Type) bool {
	return lhs.equalsTo(rhs)
}

type Decimal struct {
	precision uint32
	scale     uint32
}

func (v *Decimal) Precision() uint32 {
	return v.precision
}

func (v *Decimal) Scale() uint32 {
	return v.scale
}

func (v *Decimal) String() string {
	return v.Yql()
}

func (v *Decimal) Name() string {
	return "Decimal"
}

func (v *Decimal) Yql() string {
	return fmt.Sprintf("%s(%d,%d)", v.Name(), v.precision, v.scale)
}

func (v *Decimal) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*Decimal)

	return ok && *v == *vv
}

func (v *Decimal) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		DecimalType: Ydb.DecimalType_builder{
			Precision: v.precision,
			Scale:     v.scale,
		}.Build(),
	}.Build()
}

func NewDecimal(precision, scale uint32) *Decimal {
	return &Decimal{
		precision: precision,
		scale:     scale,
	}
}

type Dict struct {
	keyType   Type
	valueType Type
}

func (v *Dict) KeyType() Type {
	return v.keyType
}

func (v *Dict) ValueType() Type {
	return v.valueType
}

func (v *Dict) String() string {
	return v.Yql()
}

func (v *Dict) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Dict<")
	buffer.WriteString(v.keyType.Yql())
	buffer.WriteByte(',')
	buffer.WriteString(v.valueType.Yql())
	buffer.WriteByte('>')

	return buffer.String()
}

func (v *Dict) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*Dict)
	if !ok {
		return false
	}

	if !v.keyType.equalsTo(vv.keyType) {
		return false
	}

	if !v.valueType.equalsTo(vv.valueType) {
		return false
	}

	return true
}

func (v *Dict) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		DictType: Ydb.DictType_builder{
			Key:     v.keyType.ToYDB(),
			Payload: v.valueType.ToYDB(),
		}.Build(),
	}.Build()
}

func NewDict(key, value Type) (v *Dict) {
	return &Dict{
		keyType:   key,
		valueType: value,
	}
}

type EmptyList struct{}

func (v EmptyList) Yql() string {
	return "EmptyList"
}

func (v EmptyList) String() string {
	return v.Yql()
}

func (EmptyList) equalsTo(rhs Type) bool {
	_, ok := rhs.(EmptyList)

	return ok
}

func (v EmptyList) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		EmptyListType: structpb.NullValue_NULL_VALUE.Enum(),
	}.Build()
}

func NewEmptyList() EmptyList {
	return EmptyList{}
}

type EmptyDict struct{}

func (v EmptyDict) String() string {
	return v.Yql()
}

func (v EmptyDict) Yql() string {
	return "EmptyDict"
}

func (EmptyDict) equalsTo(rhs Type) bool {
	_, ok := rhs.(EmptyDict)

	return ok
}

func (v EmptyDict) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		EmptyDictType: structpb.NullValue_NULL_VALUE.Enum(),
	}.Build()
}

func EmptySet() EmptyDict {
	return EmptyDict{}
}

func NewEmptyDict() EmptyDict {
	return EmptyDict{}
}

type List struct {
	itemType Type
}

func (v *List) ItemType() Type {
	return v.itemType
}

func (v *List) String() string {
	return v.Yql()
}

func (v *List) Yql() string {
	return "List<" + v.itemType.Yql() + ">"
}

func (v *List) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*List)
	if !ok {
		return false
	}

	return v.itemType.equalsTo(vv.itemType)
}

func (v *List) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		ListType: Ydb.ListType_builder{
			Item: v.itemType.ToYDB(),
		}.Build(),
	}.Build()
}

func NewList(t Type) *List {
	return &List{
		itemType: t,
	}
}

type Set struct {
	itemType Type
}

func (v *Set) ItemType() Type {
	return v.itemType
}

func (v *Set) String() string {
	return v.Yql()
}

func (v *Set) Yql() string {
	return "Set<" + v.itemType.Yql() + ">"
}

func (v *Set) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*Set)
	if !ok {
		return false
	}

	return v.itemType.equalsTo(vv.itemType)
}

func (v *Set) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		DictType: Ydb.DictType_builder{
			Key:     v.itemType.ToYDB(),
			Payload: _voidType,
		}.Build(),
	}.Build()
}

func NewSet(t Type) *Set {
	return &Set{
		itemType: t,
	}
}

type Optional struct {
	innerType Type
}

func (v Optional) IsOptional() {}

func (v Optional) InnerType() Type {
	return v.innerType
}

func (v Optional) String() string {
	return v.Yql()
}

func (v Optional) Yql() string {
	return "Optional<" + v.innerType.Yql() + ">"
}

func (v Optional) equalsTo(rhs Type) bool {
	vv, ok := rhs.(Optional)
	if !ok {
		return false
	}

	return v.innerType.equalsTo(vv.innerType)
}

func (v Optional) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		OptionalType: Ydb.OptionalType_builder{
			Item: v.innerType.ToYDB(),
		}.Build(),
	}.Build()
}

func NewOptional(t Type) Optional {
	return Optional{
		innerType: t,
	}
}

type PgType struct {
	OID uint32
}

func (v PgType) String() string {
	return v.Yql()
}

func (v PgType) Yql() string {
	return fmt.Sprintf("PgType(%v)", v.OID)
}

func (v PgType) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{PgType: Ydb.PgType_builder{
		Oid: v.OID,
	}.Build()}.Build()
}

func (v PgType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(PgType)
	if !ok {
		return false
	}

	return v.OID == vv.OID
}

type Primitive uint

func (v Primitive) String() string {
	return v.Yql()
}

func (v Primitive) Yql() string {
	return primitiveString[v]
}

const (
	Unknown Primitive = iota
	Bool
	Int8
	Uint8
	Int16
	Uint16
	Int32
	Uint32
	Int64
	Uint64
	Float
	Double
	Date
	Date32
	Datetime
	Datetime64
	Timestamp
	Timestamp64
	Interval
	Interval64
	TzDate
	TzDatetime
	TzTimestamp
	Bytes
	Text
	YSON
	JSON
	UUID
	JSONDocument
	DyNumber
)

var primitive = [...]*Ydb.Type{
	Bool:         Ydb.Type_builder{TypeId: Ydb.Type_BOOL.Enum()}.Build(),
	Int8:         Ydb.Type_builder{TypeId: Ydb.Type_INT8.Enum()}.Build(),
	Uint8:        Ydb.Type_builder{TypeId: Ydb.Type_UINT8.Enum()}.Build(),
	Int16:        Ydb.Type_builder{TypeId: Ydb.Type_INT16.Enum()}.Build(),
	Uint16:       Ydb.Type_builder{TypeId: Ydb.Type_UINT16.Enum()}.Build(),
	Int32:        Ydb.Type_builder{TypeId: Ydb.Type_INT32.Enum()}.Build(),
	Uint32:       Ydb.Type_builder{TypeId: Ydb.Type_UINT32.Enum()}.Build(),
	Int64:        Ydb.Type_builder{TypeId: Ydb.Type_INT64.Enum()}.Build(),
	Uint64:       Ydb.Type_builder{TypeId: Ydb.Type_UINT64.Enum()}.Build(),
	Float:        Ydb.Type_builder{TypeId: Ydb.Type_FLOAT.Enum()}.Build(),
	Double:       Ydb.Type_builder{TypeId: Ydb.Type_DOUBLE.Enum()}.Build(),
	Date:         Ydb.Type_builder{TypeId: Ydb.Type_DATE.Enum()}.Build(),
	Date32:       Ydb.Type_builder{TypeId: Ydb.Type_DATE32.Enum()}.Build(),
	Datetime:     Ydb.Type_builder{TypeId: Ydb.Type_DATETIME.Enum()}.Build(),
	Datetime64:   Ydb.Type_builder{TypeId: Ydb.Type_DATETIME64.Enum()}.Build(),
	Timestamp:    Ydb.Type_builder{TypeId: Ydb.Type_TIMESTAMP.Enum()}.Build(),
	Timestamp64:  Ydb.Type_builder{TypeId: Ydb.Type_TIMESTAMP64.Enum()}.Build(),
	Interval:     Ydb.Type_builder{TypeId: Ydb.Type_INTERVAL.Enum()}.Build(),
	Interval64:   Ydb.Type_builder{TypeId: Ydb.Type_INTERVAL64.Enum()}.Build(),
	TzDate:       Ydb.Type_builder{TypeId: Ydb.Type_TZ_DATE.Enum()}.Build(),
	TzDatetime:   Ydb.Type_builder{TypeId: Ydb.Type_TZ_DATETIME.Enum()}.Build(),
	TzTimestamp:  Ydb.Type_builder{TypeId: Ydb.Type_TZ_TIMESTAMP.Enum()}.Build(),
	Bytes:        Ydb.Type_builder{TypeId: Ydb.Type_STRING.Enum()}.Build(),
	Text:         Ydb.Type_builder{TypeId: Ydb.Type_UTF8.Enum()}.Build(),
	YSON:         Ydb.Type_builder{TypeId: Ydb.Type_YSON.Enum()}.Build(),
	JSON:         Ydb.Type_builder{TypeId: Ydb.Type_JSON.Enum()}.Build(),
	UUID:         Ydb.Type_builder{TypeId: Ydb.Type_UUID.Enum()}.Build(),
	JSONDocument: Ydb.Type_builder{TypeId: Ydb.Type_JSON_DOCUMENT.Enum()}.Build(),
	DyNumber:     Ydb.Type_builder{TypeId: Ydb.Type_DYNUMBER.Enum()}.Build(),
}

var primitiveString = [...]string{
	Unknown:      "<unknown>",
	Bool:         "Bool",
	Int8:         "Int8",
	Uint8:        "Uint8",
	Int16:        "Int16",
	Uint16:       "Uint16",
	Int32:        "Int32",
	Uint32:       "Uint32",
	Int64:        "Int64",
	Uint64:       "Uint64",
	Float:        "Float",
	Double:       "Double",
	Date:         "Date",
	Date32:       "Date32",
	Datetime:     "Datetime",
	Datetime64:   "Datetime64",
	Timestamp:    "Timestamp",
	Timestamp64:  "Timestamp64",
	Interval:     "Interval",
	Interval64:   "Interval64",
	TzDate:       "TzDate",
	TzDatetime:   "TzDatetime",
	TzTimestamp:  "TzTimestamp",
	Bytes:        "String",
	Text:         "Utf8",
	YSON:         "Yson",
	JSON:         "Json",
	UUID:         "Uuid",
	JSONDocument: "JsonDocument",
	DyNumber:     "DyNumber",
}

func (v Primitive) equalsTo(rhs Type) bool {
	vv, ok := rhs.(Primitive)
	if !ok {
		return false
	}

	return v == vv
}

func (v Primitive) ToYDB() *Ydb.Type {
	return primitive[v]
}

type (
	StructField struct {
		Name string
		T    Type
	}
	Struct struct {
		fields []StructField
	}
)

func (v *Struct) Field(i int) StructField {
	return v.fields[i]
}

func (v *Struct) Fields() []StructField {
	return v.fields
}

func (v *Struct) String() string {
	return v.Yql()
}

func (v *Struct) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Struct<")

	for i := range v.fields {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteString("'" + v.fields[i].Name + "'")
		buffer.WriteByte(':')
		buffer.WriteString(v.fields[i].T.Yql())
	}

	buffer.WriteByte('>')

	return buffer.String()
}

func (v *Struct) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*Struct)
	if !ok {
		return false
	}

	if len(v.fields) != len(vv.fields) {
		return false
	}

	for i := range v.fields {
		if v.fields[i].Name != vv.fields[i].Name {
			return false
		}

		if !v.fields[i].T.equalsTo(vv.fields[i].T) {
			return false
		}
	}

	return true
}

func (v *Struct) ToYDB() *Ydb.Type {
	members := make([]*Ydb.StructMember, len(v.fields))
	for i := range v.fields {
		members[i] = Ydb.StructMember_builder{
			Name: v.fields[i].Name,
			Type: v.fields[i].T.ToYDB(),
		}.Build()
	}

	return Ydb.Type_builder{
		StructType: Ydb.StructType_builder{
			Members: members,
		}.Build(),
	}.Build()
}

func NewStruct(fields ...StructField) (v *Struct) {
	return &Struct{
		fields: fields,
	}
}

func StructFields(ms []*Ydb.StructMember) []StructField {
	fs := make([]StructField, len(ms))
	for i, m := range ms {
		fs[i] = StructField{
			Name: m.GetName(),
			T:    TypeFromYDB(m.GetType()),
		}
	}

	return fs
}

type Tuple struct {
	innerTypes []Type
}

func (v *Tuple) InnerTypes() []Type {
	return v.innerTypes
}

func (v *Tuple) ItemType(i int) Type {
	return v.innerTypes[i]
}

func (v *Tuple) String() string {
	return v.Yql()
}

func (v *Tuple) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Tuple<")

	for i, t := range v.innerTypes {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteString(t.Yql())
	}

	buffer.WriteByte('>')

	return buffer.String()
}

func (v *Tuple) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*Tuple)
	if !ok {
		return false
	}

	if len(v.innerTypes) != len(vv.innerTypes) {
		return false
	}

	for i := range v.innerTypes {
		if !v.innerTypes[i].equalsTo(vv.innerTypes[i]) {
			return false
		}
	}

	return true
}

func (v *Tuple) ToYDB() *Ydb.Type {
	var items []*Ydb.Type
	if v != nil {
		for _, inner := range v.innerTypes {
			items = append(items, inner.ToYDB())
		}
	}

	return Ydb.Type_builder{
		TupleType: Ydb.TupleType_builder{
			Elements: items,
		}.Build(),
	}.Build()
}

func NewTuple(items ...Type) (v *Tuple) {
	return &Tuple{
		innerTypes: items,
	}
}

type VariantStruct struct {
	*Struct
}

func (v *VariantStruct) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Variant<")

	for i := range v.fields {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteString("'" + v.fields[i].Name + "'")
		buffer.WriteByte(':')
		buffer.WriteString(v.fields[i].T.Yql())
	}

	buffer.WriteByte('>')

	return buffer.String()
}

func (v *VariantStruct) equalsTo(rhs Type) bool {
	switch t := rhs.(type) {
	case *VariantStruct:
		return v.Struct.equalsTo(t.Struct)
	case *Struct:
		return v.Struct.equalsTo(t)
	default:
		return false
	}
}

func (v *VariantStruct) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		VariantType: Ydb.VariantType_builder{
			StructItems: v.Struct.ToYDB().GetStructType(),
		}.Build(),
	}.Build()
}

func NewVariantStruct(fields ...StructField) *VariantStruct {
	return &VariantStruct{
		Struct: NewStruct(fields...),
	}
}

type VariantTuple struct {
	*Tuple
}

func (v *VariantTuple) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Variant<")

	for i, t := range v.innerTypes {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteString(t.Yql())
	}

	buffer.WriteByte('>')

	return buffer.String()
}

func (v *VariantTuple) equalsTo(rhs Type) bool {
	switch t := rhs.(type) {
	case *VariantTuple:
		return v.Tuple.equalsTo(t.Tuple)
	case *Tuple:
		return v.Tuple.equalsTo(t)
	default:
		return false
	}
}

func (v *VariantTuple) ToYDB() *Ydb.Type {
	return Ydb.Type_builder{
		VariantType: Ydb.VariantType_builder{
			TupleItems: v.Tuple.ToYDB().GetTupleType(),
		}.Build(),
	}.Build()
}

func NewVariantTuple(items ...Type) *VariantTuple {
	return &VariantTuple{
		Tuple: NewTuple(items...),
	}
}

type Void struct{}

func (v Void) String() string {
	return v.Yql()
}

func (v Void) Yql() string {
	return "Void"
}

var _voidType = Ydb.Type_builder{
	VoidType: structpb.NullValue_NULL_VALUE.Enum(),
}.Build()

func (v Void) equalsTo(rhs Type) bool {
	_, ok := rhs.(Void)

	return ok
}

func (Void) ToYDB() *Ydb.Type {
	return _voidType
}

func NewVoid() Void {
	return Void{}
}

type Null struct{}

func (v Null) String() string {
	return v.Yql()
}

func (v Null) Yql() string {
	return "Null"
}

var _nullType = Ydb.Type_builder{
	NullType: structpb.NullValue_NULL_VALUE.Enum(),
}.Build()

func (v Null) equalsTo(rhs Type) bool {
	_, ok := rhs.(Null)

	return ok
}

func (Null) ToYDB() *Ydb.Type {
	return _nullType
}

func NewNull() Null {
	return Null{}
}

var _ Type = (*protobufType)(nil)

type protobufType struct {
	pb *Ydb.Type
}

func (v protobufType) Yql() string {
	return FromYDB([]*Ydb.Type{
		v.pb,
	})[0].Yql()
}

func (v protobufType) String() string {
	return v.Yql()
}

func (v protobufType) ToYDB() *Ydb.Type {
	return v.pb
}

func (v protobufType) equalsTo(rhs Type) bool {
	switch t := rhs.(type) {
	case *protobufType:
		return proto.Equal(v.pb, t.pb)
	default:
		return false
	}
}

func FromProtobuf(pb *Ydb.Type) *protobufType {
	return &protobufType{pb: pb}
}