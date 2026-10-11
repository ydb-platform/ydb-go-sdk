package query

import (
	"bytes"
	"fmt"
	"math"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wirevalue"
)

// wirePart holds the original response bytes while exposing rows without
// constructing a protobuf Value tree. Only FORMAT_VALUE is supported.
type wirePart struct {
	meta        *Ydb_Query.ExecuteQueryResponsePart
	frame       []byte
	rows        []rowSpan
	rowObjects  []Row
	columns     []*Ydb.Column
	columnTypes []types.Type
}

var _ operation.Status = (*wirePart)(nil)

type rowSpan struct {
	start uint32
	end   uint32
}

// Meta returns the standard response metadata without materializing Rows.
func (p *wirePart) Meta() *Ydb_Query.ExecuteQueryResponsePart { return p.meta }

// GetStatus exposes the response status to the SDK transport.
func (p *wirePart) GetStatus() Ydb.StatusIds_StatusCode { return p.meta.GetStatus() }

// GetIssues exposes response issues to the SDK transport.
func (p *wirePart) GetIssues() []*Ydb_Issue.IssueMessage { return p.meta.GetIssues() }

// RowCount returns the number of rows in this response part.
func (p *wirePart) RowCount() int { return len(p.rows) }

func (p *wirePart) rowBytes(index int) []byte {
	span := p.rows[index]

	return p.frame[span.start:span.end]
}

func (p *wirePart) row(index int, columns []*Ydb.Column) *Row {
	if p.rowObjects == nil {
		p.columns = columns
		p.columnTypes = make([]types.Type, len(columns))
		for i, column := range columns {
			p.columnTypes[i] = types.TypeFromYDB(column.GetType())
		}
		p.rowObjects = make([]Row, len(p.rows))
		for i := range p.rowObjects {
			p.rowObjects[i] = Row{part: p, index: i}
		}
	}

	return &p.rowObjects[index]
}

// decodeWirePart copies and decodes a FORMAT_VALUE response part.
func decodeWirePart(data []byte) (*wirePart, error) {
	// A row may outlive the next RecvMsg, so the part must own its wire bytes.
	return decodeOwnedPart(bytes.Clone(data))
}

func decodeOwnedPart(frame []byte) (*wirePart, error) {
	if len(frame) > math.MaxUint32 {
		return nil, fmt.Errorf("wire value decoder: response part exceeds 4 GiB")
	}
	part := &wirePart{meta: new(Ydb_Query.ExecuteQueryResponsePart), frame: frame}
	metadata := make([]byte, 0, 256)
	offset := 0
	for len(frame) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(frame)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(field, wireType, frame[tagLen:])
		if valueLen < 0 {
			return nil, protowire.ParseError(valueLen)
		}
		if field == wirevalue.PartResultSetField && wireType == wirevalue.PartResultSetWireType {
			resultSetBytes, n := protowire.ConsumeBytes(frame[tagLen:])
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			base := offset + tagLen + n - len(resultSetBytes)
			resultSetMetadata, err := stripWireValueRows(resultSetBytes, part, base)
			if err != nil {
				return nil, err
			}
			metadata = protowire.AppendTag(metadata, wirevalue.PartResultSetField, wirevalue.PartResultSetWireType)
			metadata = protowire.AppendBytes(metadata, resultSetMetadata)
		} else {
			metadata = append(metadata, frame[:tagLen+valueLen]...)
		}
		offset += tagLen + valueLen
		frame = frame[tagLen+valueLen:]
	}
	if err := proto.Unmarshal(metadata, part.Meta()); err != nil {
		return nil, err
	}
	resultSet := part.Meta().GetResultSet()
	if resultSet != nil && resultSet.GetFormat() != Ydb.ResultSet_FORMAT_VALUE &&
		resultSet.GetFormat() != Ydb.ResultSet_FORMAT_UNSPECIFIED {
		return nil, fmt.Errorf("wire value decoder: unsupported result set format %v", resultSet.GetFormat())
	}

	return part, nil
}

func stripWireValueRows(data []byte, part *wirePart, base int) ([]byte, error) {
	metadata := make([]byte, 0, 128)
	offset := 0
	for len(data) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(field, wireType, data[tagLen:])
		if valueLen < 0 {
			return nil, protowire.ParseError(valueLen)
		}
		if field == wirevalue.ResultSetRowsField && wireType == wirevalue.ResultSetRowsWireType {
			row, n := protowire.ConsumeBytes(data[tagLen:])
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			start := base + offset + tagLen + n - len(row)
			part.rows = append(part.rows, rowSpan{start: uint32(start), end: uint32(start + len(row))})
		} else {
			metadata = append(metadata, data[:tagLen+valueLen]...)
		}
		offset += tagLen + valueLen
		data = data[tagLen+valueLen:]
	}

	return metadata, nil
}

func scanRowBytes(r *Row, dst []any) error {
	columns := r.part.columns
	if len(dst) != len(columns) {
		return scanner.Indexed(r.scannerData()).Scan(dst...)
	}
	cells := rowCells{data: r.part.rowBytes(r.index)}
	index := 0
	for cells.Next() {
		if index < len(dst) {
			columnType := columns[index].GetType()
			if err := scanWireValueDestination(columnType, r.part.columnTypes[index], cells.Cell(), dst[index]); err != nil {
				return fmt.Errorf("scan error on column index %d: %w", index, err)
			}
		}
		index++
	}
	if err := cells.Err(); err != nil {
		return err
	}
	if index < len(dst) {
		return fmt.Errorf("wire value decoder: row has %d cells, want %d", index, len(dst))
	}

	return nil
}

func (r *Row) ColumnValue(column int) value.Value {
	cell, err := r.cell(column)
	if err != nil {
		return nil
	}
	v, err := value.FromWire(r.part.columnTypes[column], cell)
	if err != nil {
		return nil
	}

	return v
}

func (r *Row) ScanColumn(column int, dst any) error {
	cell, err := r.cell(column)
	if err != nil {
		return err
	}

	return scanWireValueDestination(r.part.columns[column].GetType(), r.part.columnTypes[column], cell, dst)
}

func scanWireValueDestination(columnType *Ydb.Type, decodedType types.Type, cell []byte, dst any) error {
	parsed, err := wirevalue.Parse(cell)
	if err != nil {
		return err
	}
	if scanWireValueCell(columnType, parsed, dst) {
		return nil
	}
	v, err := value.FromCell(decodedType, parsed)
	if err != nil {
		return err
	}

	return value.CastTo(v, dst)
}

func (r *Row) cell(column int) ([]byte, error) {
	if column < 0 || column >= len(r.part.columns) {
		return nil, fmt.Errorf("wire value decoder: column %d out of range", column)
	}
	cells := rowCells{data: r.part.rowBytes(r.index)}
	index := 0
	for cells.Next() {
		if index == column {
			return cells.Cell(), nil
		}
		index++
	}
	if err := cells.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("wire value decoder: missing column %d", column)
}

type rowCells struct {
	data []byte
	cell []byte
	err  error
}

func (c *rowCells) Next() bool {
	for len(c.data) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(c.data)
		if tagLen < 0 {
			c.err = protowire.ParseError(tagLen)

			return false
		}
		valueLen := protowire.ConsumeFieldValue(field, wireType, c.data[tagLen:])
		if valueLen < 0 {
			c.err = protowire.ParseError(valueLen)

			return false
		}
		value := c.data[tagLen : tagLen+valueLen]
		c.data = c.data[tagLen+valueLen:]
		if field == wirevalue.ValueItemsField && wireType == wirevalue.ValueItemsWireType {
			c.cell, _ = protowire.ConsumeBytes(value)

			return true
		}
	}

	return false
}

func (c *rowCells) Cell() []byte { return c.cell }

func (c *rowCells) Err() error { return c.err }

func scanWireValueCell(columnType *Ydb.Type, cell wirevalue.Cell, dst any) bool {
	primitive, optional := wireValuePrimitive(columnType)
	if cell.Kind() == wirevalue.ValueNullField && optional {
		return clearOptionalDestination(dst)
	}
	if primitive == Ydb.Type_UINT64 && cell.Kind() == wirevalue.ValueUint64Field {
		if p, ok := dst.(*uint64); ok && p != nil {
			*p = cell.Uint64()

			return true
		}
	}
	if optional {
		return scanOptionalCell(primitive, cell, dst)
	}

	return false
}

func scanOptionalCell(primitive Ydb.Type_PrimitiveTypeId, cell wirevalue.Cell, dst any) bool {
	switch primitive {
	case Ydb.Type_INT32:
		if cell.Kind() == wirevalue.ValueInt32Field {
			return assignOptional(dst, int32(cell.Uint32()))
		}
	case Ydb.Type_BOOL:
		if cell.Kind() == wirevalue.ValueBoolField {
			return assignOptional(dst, cell.Uint64() != 0)
		}
	case Ydb.Type_DOUBLE:
		if cell.Kind() == wirevalue.ValueDoubleField {
			return assignOptional(dst, cell.Float64())
		}
	case Ydb.Type_UTF8:
		if cell.Kind() == wirevalue.ValueTextField {
			return assignOptional(dst, string(cell.Bytes()))
		}
	case Ydb.Type_STRING:
		if cell.Kind() == wirevalue.ValueBytesField {
			return assignOptional(dst, bytes.Clone(cell.Bytes()))
		}
	}

	return false
}

func assignOptional[T any](dst any, v T) bool {
	p, ok := dst.(**T)
	if !ok || p == nil {
		return false
	}
	if *p == nil {
		*p = new(T)
	}
	**p = v

	return true
}

func clearOptionalDestination(dst any) bool {
	switch p := dst.(type) {
	case **int32:
		if p == nil {
			return false
		}
		*p = nil
	case **bool:
		if p == nil {
			return false
		}
		*p = nil
	case **float64:
		if p == nil {
			return false
		}
		*p = nil
	case **string:
		if p == nil {
			return false
		}
		*p = nil
	case **[]byte:
		if p == nil {
			return false
		}
		*p = nil
	default:

		return false
	}

	return true
}

func wireValuePrimitive(t *Ydb.Type) (Ydb.Type_PrimitiveTypeId, bool) {
	if optional := t.GetOptionalType(); optional != nil {
		return optional.GetItem().GetTypeId(), true
	}

	return t.GetTypeId(), false
}
