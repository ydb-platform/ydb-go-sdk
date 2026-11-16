package types

import public "github.com/ydb-platform/ydb-go-sdk/v3/types"

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.CastTo instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var CastTo = public.CastTo

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.IsOptional instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var IsOptional = public.IsOptional

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.IsNull instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var IsNull = public.IsNull

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.Unwrap instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var Unwrap = public.Unwrap

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.ToDecimal instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var ToDecimal = public.ToDecimal

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.ListItems instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var ListItems = public.ListItems

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.TupleItems instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var TupleItems = public.TupleItems

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.StructFields instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var StructFields = public.StructFields

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.VariantValue instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var VariantValue = public.VariantValue

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DictValues instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func DictFields(v Value) (map[Value]Value, error) {
	return DictValues(v)
}

// Deprecated: use github.com/ydb-platform/ydb-go-sdk/v3/types.DictValues instead.
// Will be removed at next major release.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
var DictValues = public.DictValues
