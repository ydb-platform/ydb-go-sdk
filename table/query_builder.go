package table

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type queryBuilder struct {
	pragmas []string
	query   string
	params  []ParameterOption
}

func NewQuery(query string) queryBuilder {
	return queryBuilder{
		query: query,
	}
}

func (builder queryBuilder) WithPragma(pragma string) queryBuilder {
	builder.pragmas = append(builder.pragmas, pragma)
	return builder
}

func (builder queryBuilder) WithTablePathPrefix(tablePathPrefix string) queryBuilder {
	return builder.WithPragma(fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\")", tablePathPrefix))
}

func (builder queryBuilder) Query() string {
	buffer := allocator.Buffers.Get()
	defer allocator.Buffers.Put(buffer)
	for _, pragma := range builder.pragmas {
		buffer.WriteString(pragma)
		buffer.WriteString(";\n")
	}
	for _, param := range builder.params {
		buffer.WriteString("DECLARE ")
		buffer.WriteString(param.Name())
		buffer.WriteString(" AS ")
		buffer.WriteString(param.Value().Type().Yql())
		buffer.WriteString(";\n")
	}
	buffer.WriteString(builder.query)
	return buffer.String()
}

func (builder queryBuilder) WithInt64Param(name string, v int64) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Int64Value(v)))
	return builder
}

func (builder queryBuilder) WithInt32Param(name string, v int32) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Int32Value(v)))
	return builder
}

func (builder queryBuilder) WithInt16Param(name string, v int16) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Int16Value(v)))
	return builder
}

func (builder queryBuilder) WithInt8Param(name string, v int8) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Int8Value(v)))
	return builder
}

func (builder queryBuilder) WithUint64Param(name string, v uint64) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Uint64Value(v)))
	return builder
}

func (builder queryBuilder) WithUint32Param(name string, v uint32) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Uint32Value(v)))
	return builder
}

func (builder queryBuilder) WithUint16Param(name string, v uint16) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Uint16Value(v)))
	return builder
}

func (builder queryBuilder) WithUint8Param(name string, v uint8) queryBuilder {
	builder.params = append(builder.params, ValueParam(name, types.Uint8Value(v)))
	return builder
}
