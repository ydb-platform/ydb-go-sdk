package value

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"
)

type (
	DefaultValue struct {
		underlyingValue any
	}

	DefaultLiteralValue struct {
		Value
	}

	DefaultSequenceValue struct {
		Name       *string
		MinValue   *int64
		MaxValue   *int64
		StartValue *int64
		Cache      *uint64
		Increment  *int64
		Cycle      *bool
		SetVal     *SequenceSetVal
	}

	SequenceSetVal struct {
		NextValue *int64
		NextUsed  *bool
	}
)

func GetDefaultFromYDB(column *Ydb_Table.ColumnMeta) *DefaultValue {
	if column == nil {
		return nil
	}

	var underlyingValue any
	if protoLiteral := column.GetFromLiteral(); protoLiteral != nil {
		underlyingValue = DefaultLiteralValue{
			Value: FromYDB(protoLiteral.GetType(), protoLiteral.GetValue()),
		}
	}
	if protoSeq := column.GetFromSequence(); protoSeq != nil {
		seq := DefaultSequenceValue{
			Name:       proto.ValueOrNil(protoSeq.HasName(), protoSeq.GetName),
			MinValue:   proto.ValueOrNil(protoSeq.HasMinValue(), protoSeq.GetMinValue),
			MaxValue:   proto.ValueOrNil(protoSeq.HasMaxValue(), protoSeq.GetMaxValue),
			StartValue: proto.ValueOrNil(protoSeq.HasStartValue(), protoSeq.GetStartValue),
			Cache:      proto.ValueOrNil(protoSeq.HasCache(), protoSeq.GetCache),
			Increment:  proto.ValueOrNil(protoSeq.HasIncrement(), protoSeq.GetIncrement),
			Cycle:      proto.ValueOrNil(protoSeq.HasCycle(), protoSeq.GetCycle),
		}
		if setVal := protoSeq.GetSetVal(); setVal != nil {
			seq.SetVal = &SequenceSetVal{
				NextValue: proto.ValueOrNil(setVal.HasNextValue(), setVal.GetNextValue),
				NextUsed:  proto.ValueOrNil(setVal.HasNextUsed(), setVal.GetNextUsed),
			}
		}
		underlyingValue = seq
	}

	if underlyingValue == nil {
		return nil
	}

	return &DefaultValue{
		underlyingValue: underlyingValue,
	}
}

func (d *DefaultValue) Literal() *DefaultLiteralValue {
	literal, ok := d.underlyingValue.(DefaultLiteralValue)
	if ok {
		return &literal
	}

	return nil
}

func (d *DefaultValue) Sequence() *DefaultSequenceValue {
	seq, ok := d.underlyingValue.(DefaultSequenceValue)
	if ok {
		return &seq
	}

	return nil
}

func (d *DefaultValue) ToYDB(targetColumn *Ydb_Table.ColumnMeta) {
	if targetColumn == nil {
		return
	}

	switch value := d.underlyingValue.(type) {
	case DefaultLiteralValue:
		targetColumn.SetFromLiteral(value.toYDB())
	case DefaultSequenceValue:
		targetColumn.SetFromSequence(value.toYDB())
	}
}

func (l *DefaultLiteralValue) toYDB() *Ydb.TypedValue {
	return ToYDB(l.Value)
}

func (s *DefaultSequenceValue) toYDB() *Ydb_Table.SequenceDescription {
	protoSeq := Ydb_Table.SequenceDescription_builder{
		Name:       s.Name,
		MinValue:   s.MinValue,
		MaxValue:   s.MaxValue,
		StartValue: s.StartValue,
		Cache:      s.Cache,
		Increment:  s.Increment,
		Cycle:      s.Cycle,
	}.Build()

	if s.SetVal != nil {
		protoSeq.SetSetVal(Ydb_Table.SequenceDescription_SetVal_builder{
			NextValue: s.SetVal.NextValue,
			NextUsed:  s.SetVal.NextUsed,
		}.Build())
	}

	return protoSeq
}