package types

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type Type interface {
	Yql() string
	String() string

	ToYDB(a *allocator.Allocator) *Ydb.Type
	equalsTo(rhs Type) bool
}

func TypeToYDB(t Type, a *allocator.Allocator) *Ydb.Type {
	return t.ToYDB(a)
}

func TypeFromYDB(x *Ydb.Type) Type {
	switch v := x.GetType().(type) {
	case *Ydb.Type_TypeId:
		return primitiveTypeFromYDB(v.TypeId)

	case *Ydb.Type_OptionalType:
		return NewOptional(TypeFromYDB(v.OptionalType.GetItem()))

	case *Ydb.Type_ListType:
		return NewList(TypeFromYDB(v.ListType.GetItem()))

	case *Ydb.Type_DecimalType:
		d := v.DecimalType

		return NewDecimal(d.GetPrecision(), d.GetScale())

	case *Ydb.Type_TupleType:
		t := v.TupleType

		return NewTuple(FromYDB(t.GetElements())...)

	case *Ydb.Type_StructType:
		s := v.StructType

		return NewStruct(StructFields(s.GetMembers())...)

	case *Ydb.Type_DictType:
		keyType, valueType := TypeFromYDB(v.DictType.GetKey()), TypeFromYDB(v.DictType.GetPayload())
		if valueType.equalsTo(NewVoid()) {
			return NewSet(keyType)
		}

		return NewDict(keyType, valueType)

	case *Ydb.Type_VariantType:
		t := v.VariantType
		switch x := t.GetType().(type) {
		case *Ydb.VariantType_TupleItems:
			return NewVariantTuple(FromYDB(x.TupleItems.GetElements())...)
		case *Ydb.VariantType_StructItems:
			return NewVariantStruct(StructFields(x.StructItems.GetMembers())...)
		default:
			panic("ydb: unknown variant type")
		}

	case *Ydb.Type_VoidType:
		return NewVoid()

	case *Ydb.Type_NullType:
		return NewNull()

	case *Ydb.Type_PgType:
		return &PgType{
			OID: x.GetPgType().GetOid(),
		}

	default:
		panic("ydb: unknown type")
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
	case Ydb.Type_DATETIME:
		return Datetime
	case Ydb.Type_TIMESTAMP:
		return Timestamp
	case Ydb.Type_INTERVAL:
		return Interval
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
		panic("ydb: unexpected type")
	}
}

func FromYDB(es []*Ydb.Type) []Type {
	ts := make([]Type, len(es))
	for i, el := range es {
		ts[i] = TypeFromYDB(el)
	}

	return ts
}

func Equal(a, b Type) bool {
	return a.equalsTo(b)
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

func (v *Decimal) ToYDB(a *allocator.Allocator) *Ydb.Type {
	decimal := a.Decimal()

	decimal.Scale = v.scale
	decimal.Precision = v.precision

	typeDecimal := a.TypeDecimal()
	typeDecimal.DecimalType = decimal

	t := a.Type()
	t.Type = typeDecimal

	return t
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

func (v *Dict) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeDict := a.TypeDict()

	typeDict.DictType = a.Dict()

	typeDict.DictType.Key = v.keyType.ToYDB(a)
	typeDict.DictType.Payload = v.valueType.ToYDB(a)

	t.Type = typeDict

	return t
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

func (EmptyList) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	t.Type = a.TypeEmptyList()

	return t
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

func (EmptyDict) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	t.Type = a.TypeEmptyDict()

	return t
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

func (v *List) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	list := a.List()

	list.Item = v.itemType.ToYDB(a)

	typeList := a.TypeList()
	typeList.ListType = list

	t.Type = typeList

	return t
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

func (v *Set) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeDict := a.TypeDict()

	typeDict.DictType = a.Dict()

	typeDict.DictType.Key = v.itemType.ToYDB(a)
	typeDict.DictType.Payload = _voidType

	t.Type = typeDict

	return t
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

func (v Optional) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeOptional := a.TypeOptional()

	typeOptional.OptionalType = a.Optional()

	typeOptional.OptionalType.Item = v.innerType.ToYDB(a)

	t.Type = typeOptional

	return t
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

func (v PgType) ToYDB(a *allocator.Allocator) *Ydb.Type {
	//nolint:godox
	// TODO: make allocator
	return &Ydb.Type{Type: &Ydb.Type_PgType{
		PgType: &Ydb.PgType{
			Oid: v.OID,
		},
	}}
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
	Datetime
	Timestamp
	Interval
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
	Bool:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}},
	Int8:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8}},
	Uint8:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8}},
	Int16:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}},
	Uint16:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16}},
	Int32:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
	Uint32:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}},
	Int64:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
	Uint64:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
	Float:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}},
	Double:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}},
	Date:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}},
	Datetime:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME}},
	Timestamp:    {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}},
	Interval:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INTERVAL}},
	TzDate:       {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATE}},
	TzDatetime:   {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_DATETIME}},
	TzTimestamp:  {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TZ_TIMESTAMP}},
	Bytes:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
	Text:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
	YSON:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_YSON}},
	JSON:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON}},
	UUID:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UUID}},
	JSONDocument: {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT}},
	DyNumber:     {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DYNUMBER}},
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
	Datetime:     "Datetime",
	Timestamp:    "Timestamp",
	Interval:     "Interval",
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

func (v Primitive) ToYDB(*allocator.Allocator) *Ydb.Type {
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

func (v *Struct) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeStruct := a.TypeStruct()

	typeStruct.StructType = a.Struct()

	for i := range v.fields {
		structMember := a.StructMember()
		structMember.Name = v.fields[i].Name
		structMember.Type = v.fields[i].T.ToYDB(a)
		typeStruct.StructType.Members = append(
			typeStruct.StructType.GetMembers(),
			structMember,
		)
	}

	t.Type = typeStruct

	return t
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

func (v *Tuple) ToYDB(a *allocator.Allocator) *Ydb.Type {
	var items []Type
	if v != nil {
		items = v.innerTypes
	}
	t := a.Type()

	typeTuple := a.TypeTuple()

	typeTuple.TupleType = a.Tuple()

	for _, vv := range items {
		typeTuple.TupleType.Elements = append(typeTuple.TupleType.GetElements(), vv.ToYDB(a))
	}

	t.Type = typeTuple

	return t
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

func (v *VariantStruct) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeVariant := a.TypeVariant()

	typeVariant.VariantType = a.Variant()

	structItems := a.VariantStructItems()

	val, ok := v.Struct.ToYDB(a).GetType().(*Ydb.Type_StructType)
	if !ok {
		panic(fmt.Sprintf("unsupported type conversion from %T to *Ydb.Type_StructType", val))
	}
	structItems.StructItems = val.StructType

	typeVariant.VariantType.Type = structItems

	t.Type = typeVariant

	return t
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

func (v *VariantTuple) ToYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeVariant := a.TypeVariant()

	typeVariant.VariantType = a.Variant()

	tupleItems := a.VariantTupleItems()

	val, ok := v.Tuple.ToYDB(a).GetType().(*Ydb.Type_TupleType)
	if !ok {
		panic(fmt.Sprintf("unsupported type conversion from %T to *Ydb.Type_TupleType", val))
	}
	tupleItems.TupleItems = val.TupleType

	typeVariant.VariantType.Type = tupleItems

	t.Type = typeVariant

	return t
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

var _voidType = &Ydb.Type{
	Type: &Ydb.Type_VoidType{},
}

func (v Void) equalsTo(rhs Type) bool {
	_, ok := rhs.(Void)

	return ok
}

func (Void) ToYDB(*allocator.Allocator) *Ydb.Type {
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

var _nullType = &Ydb.Type{
	Type: &Ydb.Type_NullType{},
}

func (v Null) equalsTo(rhs Type) bool {
	_, ok := rhs.(Null)

	return ok
}

func (Null) ToYDB(*allocator.Allocator) *Ydb.Type {
	return _nullType
}

func NewNull() Null {
	return Null{}
}
