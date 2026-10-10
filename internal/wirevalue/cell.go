package wirevalue

//go:generate go run gen_fields.go

import (
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// Cell views one encoded YDB Value and borrows the response part's bytes.
type Cell struct {
	raw     []byte
	bytes   []byte
	number  uint64
	high128 uint64
	variant uint32
	kind    protowire.Number
}

// Parse reads a cell without constructing a protobuf message tree.
func Parse(data []byte) (Cell, error) {
	cell := Cell{raw: data}
	f := fields{data: data}
	for f.next() {
		switch {
		case f.number == ValueVariantIndexField && f.kind == ValueVariantIndexWireType:
			v, _ := protowire.ConsumeVarint(f.value)
			cell.variant = uint32(v)
		case f.number == ValueHigh128Field && f.kind == ValueHigh128WireType:
			cell.high128, _ = protowire.ConsumeFixed64(f.value)
		case scalarField(f.number, f.kind):
			cell.kind = f.number
			switch f.kind {
			case protowire.VarintType:
				cell.number, _ = protowire.ConsumeVarint(f.value)
			case protowire.Fixed32Type:
				v, _ := protowire.ConsumeFixed32(f.value)
				cell.number = uint64(v)
			case protowire.Fixed64Type:
				cell.number, _ = protowire.ConsumeFixed64(f.value)
			case protowire.BytesType:
				cell.bytes, _ = protowire.ConsumeBytes(f.value)
			}
		}
	}

	return cell, f.err
}

func scalarField(number protowire.Number, kind protowire.Type) bool {
	switch number {
	case ValueBoolField, ValueNullField:
		return kind == protowire.VarintType
	case ValueInt32Field, ValueUint32Field, ValueFloatField:
		return kind == protowire.Fixed32Type
	case ValueInt64Field, ValueUint64Field, ValueDoubleField, ValueLow128Field:
		return kind == protowire.Fixed64Type
	case ValueBytesField, ValueTextField, ValueNestedField:
		return kind == protowire.BytesType
	default:

		return false
	}
}

// Kind returns the field containing the scalar or nested value.
func (c Cell) Kind() protowire.Number { return c.kind }

// Uint64 returns the raw integer or float bits.
func (c Cell) Uint64() uint64 { return c.number }

// Uint32 returns the low 32 bits of the raw number.
func (c Cell) Uint32() uint32 { return uint32(c.number) }

// Float32 interprets the raw bits as a float.
func (c Cell) Float32() float32 { return math.Float32frombits(c.Uint32()) }

// Float64 interprets the raw bits as a float.
func (c Cell) Float64() float64 { return math.Float64frombits(c.Uint64()) }

// Bytes returns the scalar bytes without copying them.
func (c Cell) Bytes() []byte { return c.bytes }

// High128 returns the separate high half of a UUID or decimal.
func (c Cell) High128() uint64 { return c.high128 }

// VariantIndex returns the selected variant member.
func (c Cell) VariantIndex() uint32 { return c.variant }

// Nested parses the nested_value field.
func (c Cell) Nested() (Cell, error) { return Parse(c.bytes) }

// Items iterates list, tuple, or struct members without allocating a slice.
func (c Cell) Items() Items { return Items{fields: fields{data: c.raw}} }

// Pairs iterates dictionary or set entries without allocating a slice.
func (c Cell) Pairs() Pairs { return Pairs{fields: fields{data: c.raw}} }

type fields struct {
	data   []byte
	value  []byte
	number protowire.Number
	kind   protowire.Type
	err    error
}

func (f *fields) next() bool {
	if len(f.data) == 0 || f.err != nil {
		return false
	}
	number, kind, tagLen := protowire.ConsumeTag(f.data)
	if tagLen < 0 {
		f.err = protowire.ParseError(tagLen)

		return false
	}
	valueLen := protowire.ConsumeFieldValue(number, kind, f.data[tagLen:])
	if valueLen < 0 {
		f.err = protowire.ParseError(valueLen)

		return false
	}
	f.number, f.kind = number, kind
	f.value = f.data[tagLen : tagLen+valueLen]
	f.data = f.data[tagLen+valueLen:]

	return true
}

// Items iterates repeated Value.items fields.
type Items struct {
	fields fields
	cell   Cell
}

// Next advances to the next item.
func (i *Items) Next() bool {
	for i.fields.next() {
		if i.fields.number == ValueItemsField && i.fields.kind == ValueItemsWireType {
			data, _ := protowire.ConsumeBytes(i.fields.value)
			i.cell, i.fields.err = Parse(data)

			return i.fields.err == nil
		}
	}

	return false
}

// Cell returns the current item.
func (i Items) Cell() Cell { return i.cell }

// Err returns the last wire format error.
func (i Items) Err() error { return i.fields.err }

// Pairs iterates repeated Value.pairs fields.
type Pairs struct {
	fields  fields
	key     Cell
	payload Cell
}

// Next advances to the next pair.
func (p *Pairs) Next() bool {
	for p.fields.next() {
		if p.fields.number == ValuePairsField && p.fields.kind == ValuePairsWireType {
			data, _ := protowire.ConsumeBytes(p.fields.value)
			p.key, p.payload, p.fields.err = parsePair(data)

			return p.fields.err == nil
		}
	}

	return false
}

// Key returns the current key.
func (p Pairs) Key() Cell { return p.key }

// Payload returns the current payload.
func (p Pairs) Payload() Cell { return p.payload }

// Err returns the last wire format error.
func (p Pairs) Err() error { return p.fields.err }

func parsePair(data []byte) (key, payload Cell, err error) {
	f := fields{data: data}
	for f.next() {
		if f.kind != protowire.BytesType {
			continue
		}
		v, _ := protowire.ConsumeBytes(f.value)
		switch f.number {
		case PairKeyField:
			key, err = Parse(v)
		case PairPayloadField:
			payload, err = Parse(v)
		}
		if err != nil {
			return Cell{}, Cell{}, err
		}
	}

	return key, payload, f.err
}
