package query

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

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
	rowObjects  []wireRow
	columns     []*Ydb.Column
	columnTypes []atomic.Value
	indexOnce   sync.Once
	columnIndex map[string]int
}

var _ operation.Status = (*wirePart)(nil)

type rowSpan struct {
	start uint32
	end   uint32
}

type indexedWireRow struct {
	*wireRow

	mu    sync.Mutex
	cells rowCells
	spans []rowSpan
}

func (r *indexedWireRow) ColumnIndex(name string) (int, bool) {
	p := r.part
	p.indexOnce.Do(func() {
		p.columnIndex = make(map[string]int, len(p.columns))
		for index, column := range p.columns {
			if _, exists := p.columnIndex[column.GetName()]; !exists {
				p.columnIndex[column.GetName()] = index
			}
		}
	})
	index, found := p.columnIndex[name]

	return index, found
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

func (p *wirePart) columnType(index int) types.Type {
	if t, ok := p.columnTypes[index].Load().(types.Type); ok {
		return t
	}
	t := types.TypeFromYDB(p.columns[index].GetType())
	p.columnTypes[index].Store(t)

	return t
}

func (p *wirePart) row(index int, columns []*Ydb.Column) *wireRow {
	if p.rowObjects == nil {
		p.columns = columns
		p.columnTypes = make([]atomic.Value, len(columns))
		p.rowObjects = make([]wireRow, len(p.rows))
		for i := range p.rowObjects {
			p.rowObjects[i] = wireRow{part: p, index: i}
		}
	}

	return &p.rowObjects[index]
}

// decodeOwnedPart takes ownership of frame. Rows can reference it after the next RecvMsg.
func decodeOwnedPart(frame []byte, meta *Ydb_Query.ExecuteQueryResponsePart) (*wirePart, error) {
	// Row offsets must fit in rowSpan's uint32 fields.
	if uint(len(frame)) > math.MaxUint32 {
		return nil, fmt.Errorf("wire value decoder: response part exceeds 4 GiB")
	}
	part := &wirePart{meta: meta, frame: frame}
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

func scanRowBytes(r *wireRow, dst []any) error {
	columns := r.part.columns
	if len(dst) != len(columns) {
		return scanner.Indexed(&r.scannerRow().data).Scan(dst...)
	}
	cells := rowCells{data: r.part.rowBytes(r.index)}
	index := 0
	for cells.Next() {
		if index < len(dst) {
			columnType := columns[index].GetType()
			if err := scanWireValueDestination(columnType, r.part.columnType(index), cells.Cell(), dst[index]); err != nil {
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

// ColumnValue follows [value.FromYDB] and panics if a value cannot be decoded.
func (r *indexedWireRow) ColumnValue(column int) value.Value {
	cell, err := r.cell(column)
	if err != nil {
		panic(fmt.Errorf("scan error on column index %d: %w", column, err))
	}
	v, err := value.FromWire(r.part.columnType(column), cell)
	if err != nil {
		panic(fmt.Errorf("scan error on column index %d: %w", column, err))
	}

	return v
}

func (r *indexedWireRow) ScanColumn(column int, dst any) error {
	cell, err := r.cell(column)
	if err != nil {
		return err
	}

	return scanWireValueDestination(r.part.columns[column].GetType(), r.part.columnType(column), cell, dst)
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

func (r *indexedWireRow) cell(column int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if column < 0 || column >= len(r.part.columns) {
		return nil, fmt.Errorf("wire value decoder: column %d out of range", column)
	}
	row := r.part.rowBytes(r.index)
	for len(r.spans) <= column && r.cells.Err() == nil && r.cells.Next() {
		end := len(row) - len(r.cells.data)
		r.spans = append(r.spans, rowSpan{
			start: uint32(end - len(r.cells.Cell())), end: uint32(end),
		})
	}
	if column < len(r.spans) {
		span := r.spans[column]

		return row[span.start:span.end], nil
	}
	if err := r.cells.Err(); err != nil {
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
			// ConsumeFieldValue validated the length prefix and payload above.
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
		if p, ok := dst.(**string); ok && p != nil && cell.Kind() == wirevalue.ValueTextField {
			v := string(cell.Bytes())
			*p = &v

			return true
		}
	case Ydb.Type_STRING:
		if p, ok := dst.(**[]byte); ok && p != nil && cell.Kind() == wirevalue.ValueBytesField {
			v := bytes.Clone(cell.Bytes())
			*p = &v

			return true
		}
	}

	return false
}

func assignOptional[T any](dst any, v T) bool {
	p, ok := dst.(**T)
	if !ok || p == nil {
		return false
	}
	*p = new(T)
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
