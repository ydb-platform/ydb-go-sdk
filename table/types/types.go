// Package types provides helpers for YDB types and values.
//
// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
package types

import "github.com/ydb-platform/ydb-go-sdk/v3/types"

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Type instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Type = types.Type

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructOption instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type StructOption = types.StructOption

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DefaultDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DefaultDecimal = types.DefaultDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Equal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Equal = types.Equal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.List instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var List = types.List

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Tuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Tuple = types.Tuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructField instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var StructField = types.StructField

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Struct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Struct = types.Struct

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Dict instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Dict = types.Dict

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantStruct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantStruct = types.VariantStruct

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantTuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantTuple = types.VariantTuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Void instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Void = types.Void

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Optional instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Optional = types.Optional

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalType instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalType = types.DecimalType

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalTypeFromDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalTypeFromDecimal = types.DecimalTypeFromDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.WriteTypeStringTo instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var WriteTypeStringTo = types.WriteTypeStringTo //nolint:staticcheck // backward compatibility re-export

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.RawValue instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type RawValue = types.RawValue

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Scanner instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Scanner = types.Scanner

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.... instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
const (
	TypeUnknown      = types.TypeUnknown
	TypeBool         = types.TypeBool
	TypeInt8         = types.TypeInt8
	TypeUint8        = types.TypeUint8
	TypeInt16        = types.TypeInt16
	TypeUint16       = types.TypeUint16
	TypeInt32        = types.TypeInt32
	TypeUint32       = types.TypeUint32
	TypeInt64        = types.TypeInt64
	TypeUint64       = types.TypeUint64
	TypeFloat        = types.TypeFloat
	TypeDouble       = types.TypeDouble
	TypeDate         = types.TypeDate
	TypeDatetime     = types.TypeDatetime
	TypeDatetime64   = types.TypeDatetime64
	TypeTimestamp    = types.TypeTimestamp
	TypeTimestamp64  = types.TypeTimestamp64
	TypeInterval     = types.TypeInterval
	TypeTzDate       = types.TypeTzDate
	TypeTzDatetime   = types.TypeTzDatetime
	TypeTzTimestamp  = types.TypeTzTimestamp
	TypeString       = types.TypeString
	TypeBytes        = types.TypeBytes
	TypeUTF8         = types.TypeUTF8
	TypeText         = types.TypeText
	TypeYSON         = types.TypeYSON
	TypeJSON         = types.TypeJSON
	TypeUUID         = types.TypeUUID
	TypeJSONDocument = types.TypeJSONDocument
	TypeDyNumber     = types.TypeDyNumber
)
