// Package types provides helpers for YDB types and values.
//
// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
//
// Note: StructOption is not a type alias to types.StructOption.
// Migrate StructField and Struct (or VariantStruct) to types together.
package types

import (
	"bytes"

	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	public "github.com/ydb-platform/ydb-go-sdk/v3/types"
)

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Type instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Type = public.Type

type tStructType struct {
	fields []internal.StructField
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructOption instead.
// StructOption is not a type alias to types.StructOption; migrate StructField and Struct (or VariantStruct) together.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type StructOption func(*tStructType)

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DefaultDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DefaultDecimal = public.DefaultDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Equal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Equal = public.Equal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.List instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var List = public.List

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Tuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Tuple = public.Tuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructField instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func StructField(name string, t Type) StructOption {
	return func(s *tStructType) {
		s.fields = append(s.fields, internal.StructField{
			Name: name,
			T:    t,
		})
	}
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Struct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func Struct(opts ...StructOption) Type {
	var s tStructType
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}

	return internal.NewStruct(s.fields...)
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Dict instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Dict = public.Dict

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantStruct instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func VariantStruct(opts ...StructOption) Type {
	var s tStructType
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}

	return internal.NewVariantStruct(s.fields...)
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantTuple instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantTuple = public.VariantTuple

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Void instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Void = public.Void

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Optional instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Optional = public.Optional

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalType instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalType = public.DecimalType

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DecimalTypeFromDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DecimalTypeFromDecimal = public.DecimalTypeFromDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.WriteTypeStringTo instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WriteTypeStringTo(buf *bytes.Buffer, t Type) {
	public.WriteTypeStringTo(buf, t)
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.RawValue instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type RawValue = public.RawValue

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Scanner instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
type Scanner = public.Scanner

// Deprecated: use the corresponding constant from github.com/ydb-platform/ydb-go-sdk/v3/types instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
const (
	TypeUnknown      = public.TypeUnknown
	TypeBool         = public.TypeBool
	TypeInt8         = public.TypeInt8
	TypeUint8        = public.TypeUint8
	TypeInt16        = public.TypeInt16
	TypeUint16       = public.TypeUint16
	TypeInt32        = public.TypeInt32
	TypeUint32       = public.TypeUint32
	TypeInt64        = public.TypeInt64
	TypeUint64       = public.TypeUint64
	TypeFloat        = public.TypeFloat
	TypeDouble       = public.TypeDouble
	TypeDate         = public.TypeDate
	TypeDate32       = public.TypeDate32
	TypeDatetime     = public.TypeDatetime
	TypeDatetime64   = public.TypeDatetime64
	TypeTimestamp    = public.TypeTimestamp
	TypeTimestamp64  = public.TypeTimestamp64
	TypeInterval     = public.TypeInterval
	TypeInterval64   = public.TypeInterval64
	TypeTzDate       = public.TypeTzDate
	TypeTzDatetime   = public.TypeTzDatetime
	TypeTzTimestamp  = public.TypeTzTimestamp
	TypeString       = public.TypeString
	TypeBytes        = public.TypeBytes
	TypeUTF8         = public.TypeUTF8
	TypeText         = public.TypeText
	TypeYSON         = public.TypeYSON
	TypeJSON         = public.TypeJSON
	TypeUUID         = public.TypeUUID
	TypeJSONDocument = public.TypeJSONDocument
	TypeDyNumber     = public.TypeDyNumber
)
