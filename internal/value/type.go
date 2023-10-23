package value

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

type Type interface {
	Yql() string
	String() string

	toYDB(a *allocator.Allocator) *Ydb.Type
	equalsTo(rhs Type) bool
}

func TypeToYDB(t Type, a *allocator.Allocator) *Ydb.Type {
	return t.toYDB(a)
}

func TypeFromYDB(x *Ydb.Type) Type {
	switch v := x.Type.(type) {
	case *Ydb.Type_TypeId:
		return primitiveTypeFromYDB(v.TypeId)

	case *Ydb.Type_OptionalType:
		return Optional(TypeFromYDB(v.OptionalType.Item))

	case *Ydb.Type_ListType:
		return List(TypeFromYDB(v.ListType.Item))

	case *Ydb.Type_DecimalType:
		d := v.DecimalType
		return Decimal(d.Precision, d.Scale)

	case *Ydb.Type_TupleType:
		t := v.TupleType
		return Tuple(TypesFromYDB(t.Elements)...)

	case *Ydb.Type_StructType:
		s := v.StructType
		return Struct(StructFields(s.Members)...)

	case *Ydb.Type_DictType:
		keyType, valueType := TypeFromYDB(v.DictType.Key), TypeFromYDB(v.DictType.Payload)
		if valueType.equalsTo(Void()) {
			return Set(keyType)
		}
		return Dict(keyType, valueType)

	case *Ydb.Type_VariantType:
		t := v.VariantType
		switch x := t.Type.(type) {
		case *Ydb.VariantType_TupleItems:
			return VariantTuple(TypesFromYDB(x.TupleItems.Elements)...)
		case *Ydb.VariantType_StructItems:
			return VariantStruct(StructFields(x.StructItems.Members)...)
		default:
			panic("ydb: unknown variant type")
		}

	case *Ydb.Type_VoidType:
		return Void()

	case *Ydb.Type_NullType:
		return Null()

	default:
		panic("ydb: unknown type")
	}
}

func primitiveTypeFromYDB(t Ydb.Type_PrimitiveTypeId) Type {
	switch t {
	case Ydb.Type_BOOL:
		return TypeBool
	case Ydb.Type_INT8:
		return TypeInt8
	case Ydb.Type_UINT8:
		return TypeUint8
	case Ydb.Type_INT16:
		return TypeInt16
	case Ydb.Type_UINT16:
		return TypeUint16
	case Ydb.Type_INT32:
		return TypeInt32
	case Ydb.Type_UINT32:
		return TypeUint32
	case Ydb.Type_INT64:
		return TypeInt64
	case Ydb.Type_UINT64:
		return TypeUint64
	case Ydb.Type_FLOAT:
		return TypeFloat
	case Ydb.Type_DOUBLE:
		return TypeDouble
	case Ydb.Type_DATE:
		return TypeDate
	case Ydb.Type_DATETIME:
		return TypeDatetime
	case Ydb.Type_TIMESTAMP:
		return TypeTimestamp
	case Ydb.Type_INTERVAL:
		return TypeInterval
	case Ydb.Type_TZ_DATE:
		return TypeTzDate
	case Ydb.Type_TZ_DATETIME:
		return TypeTzDatetime
	case Ydb.Type_TZ_TIMESTAMP:
		return TypeTzTimestamp
	case Ydb.Type_STRING:
		return TypeBytes
	case Ydb.Type_UTF8:
		return TypeText
	case Ydb.Type_YSON:
		return TypeYSON
	case Ydb.Type_JSON:
		return TypeJSON
	case Ydb.Type_UUID:
		return TypeUUID
	case Ydb.Type_JSON_DOCUMENT:
		return TypeJSONDocument
	case Ydb.Type_DYNUMBER:
		return TypeDyNumber
	default:
		panic("ydb: unexpected type")
	}
}

func TypesFromYDB(es []*Ydb.Type) []Type {
	ts := make([]Type, len(es))
	for i, el := range es {
		ts[i] = TypeFromYDB(el)
	}
	return ts
}

func TypesEqual(a, b Type) bool {
	return a.equalsTo(b)
}

type DecimalType struct {
	Precision uint32
	Scale     uint32
}

func (v *DecimalType) String() string {
	return v.Yql()
}

func (v *DecimalType) Name() string {
	return "Decimal"
}

func (v *DecimalType) Yql() string {
	return fmt.Sprintf("%s(%d,%d)", v.Name(), v.Precision, v.Scale)
}

func (v *DecimalType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*DecimalType)
	return ok && *v == *vv
}

func (v *DecimalType) toYDB(a *allocator.Allocator) *Ydb.Type {
	decimal := a.Decimal()

	decimal.Scale = v.Scale
	decimal.Precision = v.Precision

	typeDecimal := a.TypeDecimal()
	typeDecimal.DecimalType = decimal

	t := a.Type()
	t.Type = typeDecimal

	return t
}

func Decimal(precision, scale uint32) *DecimalType {
	return &DecimalType{
		Precision: precision,
		Scale:     scale,
	}
}

type dictType struct {
	keyType   Type
	valueType Type
}

func (v *dictType) String() string {
	return v.Yql()
}

func (v *dictType) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("Dict<")
	buffer.WriteString(v.keyType.Yql())
	buffer.WriteByte(',')
	buffer.WriteString(v.valueType.Yql())
	buffer.WriteByte('>')
	return buffer.String()
}

func (v *dictType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*dictType)
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

func (v *dictType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeDict := a.TypeDict()

	typeDict.DictType = a.Dict()

	typeDict.DictType.Key = v.keyType.toYDB(a)
	typeDict.DictType.Payload = v.valueType.toYDB(a)

	t.Type = typeDict

	return t
}

func Dict(key, value Type) (v *dictType) {
	return &dictType{
		keyType:   key,
		valueType: value,
	}
}

type emptyListType struct{}

func (v emptyListType) Yql() string {
	return "EmptyList"
}

func (v emptyListType) String() string {
	return v.Yql()
}

func (emptyListType) equalsTo(rhs Type) bool {
	_, ok := rhs.(emptyListType)
	return ok
}

func (emptyListType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	t.Type = a.TypeEmptyList()

	return t
}

func EmptyList() emptyListType {
	return emptyListType{}
}

type emptyDictType struct{}

func (v emptyDictType) String() string {
	return v.Yql()
}

func (v emptyDictType) Yql() string {
	return "EmptyDict"
}

func (emptyDictType) equalsTo(rhs Type) bool {
	_, ok := rhs.(emptyDictType)
	return ok
}

func (emptyDictType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	t.Type = a.TypeEmptyDict()

	return t
}

func EmptySet() emptyDictType {
	return emptyDictType{}
}

func EmptyDict() emptyDictType {
	return emptyDictType{}
}

type listType struct {
	itemType Type
}

func (v *listType) String() string {
	return v.Yql()
}

func (v *listType) Yql() string {
	return "List<" + v.itemType.Yql() + ">"
}

func (v *listType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*listType)
	if !ok {
		return false
	}
	return v.itemType.equalsTo(vv.itemType)
}

func (v *listType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	list := a.List()

	list.Item = v.itemType.toYDB(a)

	typeList := a.TypeList()
	typeList.ListType = list

	t.Type = typeList

	return t
}

func List(t Type) *listType {
	return &listType{
		itemType: t,
	}
}

type setType struct {
	itemType Type
}

func (v *setType) String() string {
	return v.Yql()
}

func (v *setType) Yql() string {
	return "Set<" + v.itemType.Yql() + ">"
}

func (v *setType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*setType)
	if !ok {
		return false
	}
	return v.itemType.equalsTo(vv.itemType)
}

func (v *setType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeDict := a.TypeDict()

	typeDict.DictType = a.Dict()

	typeDict.DictType.Key = v.itemType.toYDB(a)
	typeDict.DictType.Payload = _voidType

	t.Type = typeDict

	return t
}

func Set(t Type) *setType {
	return &setType{
		itemType: t,
	}
}

type optionalType struct {
	innerType Type
}

func (v optionalType) IsOptional() {}

func (v optionalType) InnerType() Type {
	return v.innerType
}

func (v optionalType) String() string {
	return v.Yql()
}

func (v optionalType) Yql() string {
	return "Optional<" + v.innerType.Yql() + ">"
}

func (v optionalType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(optionalType)
	if !ok {
		return false
	}
	return v.innerType.equalsTo(vv.innerType)
}

func (v optionalType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeOptional := a.TypeOptional()

	typeOptional.OptionalType = a.Optional()

	typeOptional.OptionalType.Item = v.innerType.toYDB(a)

	t.Type = typeOptional

	return t
}

func Optional(t Type) optionalType {
	return optionalType{
		innerType: t,
	}
}

type PrimitiveType uint

func (v PrimitiveType) String() string {
	return v.Yql()
}

func (v PrimitiveType) Yql() string {
	return primitiveString[v]
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
	TypeBytes
	TypeText
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
	TypeBytes:        {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
	TypeText:         {Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
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
	TypeBytes:        "String",
	TypeText:         "Utf8",
	TypeYSON:         "Yson",
	TypeJSON:         "Json",
	TypeUUID:         "Uuid",
	TypeJSONDocument: "JsonDocument",
	TypeDyNumber:     "DyNumber",
}

func (v PrimitiveType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(PrimitiveType)
	if !ok {
		return false
	}
	return v == vv
}

func (v PrimitiveType) toYDB(*allocator.Allocator) *Ydb.Type {
	return primitive[v]
}

type (
	StructField struct {
		Name string
		T    Type
	}
	StructType struct {
		fields []StructField
	}
)

func (v *StructType) String() string {
	return v.Yql()
}

func (v *StructType) Yql() string {
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

func (v *StructType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*StructType)
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

func (v *StructType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeStruct := a.TypeStruct()

	typeStruct.StructType = a.Struct()

	for i := range v.fields {
		structMember := a.StructMember()
		structMember.Name = v.fields[i].Name
		structMember.Type = v.fields[i].T.toYDB(a)
		typeStruct.StructType.Members = append(
			typeStruct.StructType.Members,
			structMember,
		)
	}

	t.Type = typeStruct

	return t
}

func Struct(fields ...StructField) (v *StructType) {
	return &StructType{
		fields: fields,
	}
}

func StructFields(ms []*Ydb.StructMember) []StructField {
	fs := make([]StructField, len(ms))
	for i, m := range ms {
		fs[i] = StructField{
			Name: m.Name,
			T:    TypeFromYDB(m.Type),
		}
	}
	return fs
}

type TupleType struct {
	items []Type
}

func (v *TupleType) String() string {
	return v.Yql()
}

func (v *TupleType) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("Tuple<")
	for i, t := range v.items {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(t.Yql())
	}
	buffer.WriteByte('>')
	return buffer.String()
}

func (v *TupleType) equalsTo(rhs Type) bool {
	vv, ok := rhs.(*TupleType)
	if !ok {
		return false
	}
	if len(v.items) != len(vv.items) {
		return false
	}
	for i := range v.items {
		if !v.items[i].equalsTo(vv.items[i]) {
			return false
		}
	}
	return true
}

func (v *TupleType) toYDB(a *allocator.Allocator) *Ydb.Type {
	var items []Type
	if v != nil {
		items = v.items
	}
	t := a.Type()

	typeTuple := a.TypeTuple()

	typeTuple.TupleType = a.Tuple()

	for _, vv := range items {
		typeTuple.TupleType.Elements = append(typeTuple.TupleType.Elements, vv.toYDB(a))
	}

	t.Type = typeTuple

	return t
}

func Tuple(items ...Type) (v *TupleType) {
	return &TupleType{
		items: items,
	}
}

type variantStructType struct {
	*StructType
}

func (v *variantStructType) Yql() string {
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

func (v *variantStructType) equalsTo(rhs Type) bool {
	switch t := rhs.(type) {
	case *variantStructType:
		return v.StructType.equalsTo(t.StructType)
	case *StructType:
		return v.StructType.equalsTo(t)
	default:
		return false
	}
}

func (v *variantStructType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeVariant := a.TypeVariant()

	typeVariant.VariantType = a.Variant()

	structItems := a.VariantStructItems()
	structItems.StructItems = v.StructType.toYDB(a).Type.(*Ydb.Type_StructType).StructType

	typeVariant.VariantType.Type = structItems

	t.Type = typeVariant

	return t
}

func VariantStruct(fields ...StructField) *variantStructType {
	return &variantStructType{
		StructType: Struct(fields...),
	}
}

type variantTupleType struct {
	*TupleType
}

func (v *variantTupleType) Yql() string {
	buffer := xstring.Buffer()
	defer buffer.Free()
	buffer.WriteString("Variant<")
	for i, t := range v.items {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(t.Yql())
	}
	buffer.WriteByte('>')
	return buffer.String()
}

func (v *variantTupleType) equalsTo(rhs Type) bool {
	switch t := rhs.(type) {
	case *variantTupleType:
		return v.TupleType.equalsTo(t.TupleType)
	case *TupleType:
		return v.TupleType.equalsTo(t)
	default:
		return false
	}
}

func (v *variantTupleType) toYDB(a *allocator.Allocator) *Ydb.Type {
	t := a.Type()

	typeVariant := a.TypeVariant()

	typeVariant.VariantType = a.Variant()

	tupleItems := a.VariantTupleItems()
	tupleItems.TupleItems = v.TupleType.toYDB(a).Type.(*Ydb.Type_TupleType).TupleType

	typeVariant.VariantType.Type = tupleItems

	t.Type = typeVariant

	return t
}

func VariantTuple(items ...Type) *variantTupleType {
	return &variantTupleType{
		TupleType: Tuple(items...),
	}
}

type voidType struct{}

func (v voidType) String() string {
	return v.Yql()
}

func (v voidType) Yql() string {
	return "Void"
}

var _voidType = &Ydb.Type{
	Type: &Ydb.Type_VoidType{},
}

func (v voidType) equalsTo(rhs Type) bool {
	_, ok := rhs.(voidType)
	return ok
}

func (voidType) toYDB(*allocator.Allocator) *Ydb.Type {
	return _voidType
}

func Void() voidType {
	return voidType{}
}

type nullType struct{}

func (v nullType) String() string {
	return v.Yql()
}

func (v nullType) Yql() string {
	return "Null"
}

var _nullType = &Ydb.Type{
	Type: &Ydb.Type_NullType{},
}

func (v nullType) equalsTo(rhs Type) bool {
	_, ok := rhs.(nullType)
	return ok
}

func (nullType) toYDB(*allocator.Allocator) *Ydb.Type {
	return _nullType
}

func Null() nullType {
	return nullType{}
}
