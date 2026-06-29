// Package types provides helpers for YDB types and values.
//
// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
package types

import roottypes "github.com/ydb-platform/ydb-go-sdk/v3/types"

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Type instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Type = roottypes.Type

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructOption instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type StructOption = roottypes.StructOption

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DefaultDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DefaultDecimal = roottypes.DefaultDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Equal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Equal = roottypes.Equal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.List instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var List = roottypes.List

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Tuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Tuple = roottypes.Tuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructField instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var StructField = roottypes.StructField

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Struct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Struct = roottypes.Struct

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Dict instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Dict = roottypes.Dict

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantStruct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantStruct = roottypes.VariantStruct

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantTuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantTuple = roottypes.VariantTuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Void instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Void = roottypes.Void

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Optional instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Optional = roottypes.Optional

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalType instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalType = roottypes.DecimalType

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalTypeFromDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalTypeFromDecimal = roottypes.DecimalTypeFromDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.WriteTypeStringTo instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var WriteTypeStringTo = roottypes.WriteTypeStringTo

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.RawValue instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type RawValue = roottypes.RawValue

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Scanner instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Scanner = roottypes.Scanner

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.... instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
const (
	TypeUnknown = roottypes.TypeUnknown
	TypeBool = roottypes.TypeBool
	TypeInt8 = roottypes.TypeInt8
	TypeUint8 = roottypes.TypeUint8
	TypeInt16 = roottypes.TypeInt16
	TypeUint16 = roottypes.TypeUint16
	TypeInt32 = roottypes.TypeInt32
	TypeUint32 = roottypes.TypeUint32
	TypeInt64 = roottypes.TypeInt64
	TypeUint64 = roottypes.TypeUint64
	TypeFloat = roottypes.TypeFloat
	TypeDouble = roottypes.TypeDouble
	TypeDate = roottypes.TypeDate
	TypeDatetime = roottypes.TypeDatetime
	TypeDatetime64 = roottypes.TypeDatetime64
	TypeTimestamp = roottypes.TypeTimestamp
	TypeTimestamp64 = roottypes.TypeTimestamp64
	TypeInterval = roottypes.TypeInterval
	TypeTzDate = roottypes.TypeTzDate
	TypeTzDatetime = roottypes.TypeTzDatetime
	TypeTzTimestamp = roottypes.TypeTzTimestamp
	TypeString = roottypes.TypeString
	TypeBytes = roottypes.TypeBytes
	TypeUTF8 = roottypes.TypeUTF8
	TypeText = roottypes.TypeText
	TypeYSON = roottypes.TypeYSON
	TypeJSON = roottypes.TypeJSON
	TypeUUID = roottypes.TypeUUID
	TypeJSONDocument = roottypes.TypeJSONDocument
	TypeDyNumber = roottypes.TypeDyNumber
)
