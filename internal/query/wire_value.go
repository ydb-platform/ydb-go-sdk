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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// wirePart holds the original response bytes while exposing rows without
// constructing a protobuf Value tree. Only FORMAT_VALUE is supported.
type wirePart struct {
	meta       *Ydb_Query.ExecuteQueryResponsePart
	frame      []byte
	rows       []rowSpan
	rowObjects []Row
	columns    []*Ydb.Column
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
		p.rowObjects = make([]Row, len(p.rows))
		for i := range p.rowObjects {
			p.rowObjects[i] = Row{part: p, index: i}
		}
	}

	return &p.rowObjects[index]
}

// MaterializeRows builds protobuf rows for consumers of the typed Recv API.
func (p *wirePart) MaterializeRows() error {
	if len(p.rows) == 0 {
		return nil
	}
	resultSet := p.meta.GetResultSet()
	for _, span := range p.rows {
		row := new(Ydb.Value)
		if err := proto.Unmarshal(p.frame[span.start:span.end], row); err != nil {
			return err
		}
		resultSet.Rows = append(resultSet.Rows, row)
	}

	return nil
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
		if field == 4 && wireType == protowire.BytesType {
			resultSetBytes, n := protowire.ConsumeBytes(frame[tagLen:])
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			base := offset + tagLen + n - len(resultSetBytes)
			resultSetMetadata, err := stripWireValueRows(resultSetBytes, part, base)
			if err != nil {
				return nil, err
			}
			metadata = protowire.AppendTag(metadata, 4, protowire.BytesType)
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
		if field == 2 && wireType == protowire.BytesType {
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
	data := r.part.rowBytes(r.index)
	index := 0
	for len(data) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(field, wireType, data[tagLen:])
		if valueLen < 0 {
			return protowire.ParseError(valueLen)
		}
		if field == 12 && wireType == protowire.BytesType {
			cell, n := protowire.ConsumeBytes(data[tagLen:])
			if n < 0 {
				return protowire.ParseError(n)
			}
			if index < len(dst) {
				if err := scanWireValueDestination(columns[index].GetType(), cell, dst[index]); err != nil {
					return fmt.Errorf("scan error on column index %d: %w", index, err)
				}
			}
			index++
		}
		data = data[tagLen+valueLen:]
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
	var v Ydb.Value
	if err := proto.Unmarshal(cell, &v); err != nil {
		return nil
	}

	return value.FromYDB(r.part.columns[column].GetType(), &v)
}

func (r *Row) ScanColumn(column int, dst any) error {
	cell, err := r.cell(column)
	if err != nil {
		return err
	}

	return scanWireValueDestination(r.part.columns[column].GetType(), cell, dst)
}

func scanWireValueDestination(columnType *Ydb.Type, cell []byte, dst any) error {
	if done, err := scanWireValueCell(columnType, cell, dst); done || err != nil {
		return err
	}
	var v Ydb.Value
	if err := proto.Unmarshal(cell, &v); err != nil {
		return err
	}

	return value.CastTo(value.FromYDB(columnType, &v), dst)
}

func (r *Row) cell(column int) ([]byte, error) {
	if column < 0 || column >= len(r.part.columns) {
		return nil, fmt.Errorf("wire value decoder: column %d out of range", column)
	}
	data := r.part.rowBytes(r.index)
	index := 0
	for len(data) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(field, wireType, data[tagLen:])
		if valueLen < 0 {
			return nil, protowire.ParseError(valueLen)
		}
		if field == 12 && wireType == protowire.BytesType {
			cell, n := protowire.ConsumeBytes(data[tagLen:])
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			if index == column {
				return cell, nil
			}
			index++
		}
		data = data[tagLen+valueLen:]
	}

	return nil, fmt.Errorf("wire value decoder: missing column %d", column)
}

//nolint:gocyclo,funlen // Keep scalar wire parsing and destination assignment in one allocation-free path.
func scanWireValueCell(columnType *Ydb.Type, cell []byte, dst any) (bool, error) {
	primitive, optional := wireValuePrimitive(columnType)
	var kind protowire.Number
	var numeric uint64
	var payload []byte
	for len(cell) > 0 {
		field, wireType, tagLen := protowire.ConsumeTag(cell)
		if tagLen < 0 {
			return true, protowire.ParseError(tagLen)
		}
		data := cell[tagLen:]
		valueLen := protowire.ConsumeFieldValue(field, wireType, data)
		if valueLen < 0 {
			return true, protowire.ParseError(valueLen)
		}
		switch wireType {
		case protowire.VarintType:
			if field == 1 || field == 10 {
				numeric, _ = protowire.ConsumeVarint(data)
				kind = field
			}
		case protowire.Fixed32Type:
			if field == 2 || field == 3 || field == 6 {
				v, _ := protowire.ConsumeFixed32(data)
				numeric, kind = uint64(v), field
			}
		case protowire.Fixed64Type:
			if field == 4 || field == 5 || field == 7 || field == 15 {
				numeric, _ = protowire.ConsumeFixed64(data)
				kind = field
			}
		case protowire.BytesType:
			if field == 8 || field == 9 || field == 11 {
				payload, _ = protowire.ConsumeBytes(data)
				kind = field
			}
		}
		cell = cell[tagLen+valueLen:]
	}
	//nolint:nestif // A null optional must clear each supported destination type.
	if kind == 10 && optional {
		switch p := dst.(type) {
		case **int32:
			if p == nil {
				return false, nil
			}
			*p = nil
		case **bool:
			if p == nil {
				return false, nil
			}
			*p = nil
		case **float64:
			if p == nil {
				return false, nil
			}
			*p = nil
		case **string:
			if p == nil {
				return false, nil
			}
			*p = nil
		case **[]byte:
			if p == nil {
				return false, nil
			}
			*p = nil
		default:
			return false, nil
		}

		return true, nil
	}
	switch primitive {
	case Ydb.Type_UINT64:
		if kind == 5 {
			if p, ok := dst.(*uint64); ok && p != nil {
				*p = numeric

				return true, nil
			}
		}
	case Ydb.Type_INT32:
		if kind == 2 {
			if p, ok := dst.(**int32); ok && optional && p != nil {
				if *p == nil {
					*p = new(int32)
				}
				**p = int32(numeric)

				return true, nil
			}
		}
	case Ydb.Type_BOOL:
		if kind == 1 {
			if p, ok := dst.(**bool); ok && optional && p != nil {
				if *p == nil {
					*p = new(bool)
				}
				**p = numeric != 0

				return true, nil
			}
		}
	case Ydb.Type_DOUBLE:
		if kind == 7 {
			if p, ok := dst.(**float64); ok && optional && p != nil {
				if *p == nil {
					*p = new(float64)
				}
				**p = math.Float64frombits(numeric)

				return true, nil
			}
		}
	case Ydb.Type_UTF8:
		if kind == 9 {
			if p, ok := dst.(**string); ok && optional && p != nil {
				if *p == nil {
					*p = new(string)
				}
				**p = string(payload)

				return true, nil
			}
		}
	case Ydb.Type_STRING:
		if kind == 8 {
			if p, ok := dst.(**[]byte); ok && optional && p != nil {
				if *p == nil {
					*p = new([]byte)
				}
				**p = bytes.Clone(payload)

				return true, nil
			}
		}
	}

	return false, nil
}

func wireValuePrimitive(t *Ydb.Type) (Ydb.Type_PrimitiveTypeId, bool) {
	if optional := t.GetOptionalType(); optional != nil {
		return optional.GetItem().GetTypeId(), true
	}

	return t.GetTypeId(), false
}
