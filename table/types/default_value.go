package types

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	DefaultValue struct {
		underlyingValue any
	}

	DefaultLiteralValue struct {
		value.Value
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
			Value: value.FromYDB(protoLiteral.GetType(), protoLiteral.GetValue()),
		}
	}
	if protoSeq := column.GetFromSequence(); protoSeq != nil {
		seq := DefaultSequenceValue{
			Name:       protoSeq.Name,
			MinValue:   protoSeq.MinValue,
			MaxValue:   protoSeq.MaxValue,
			StartValue: protoSeq.StartValue,
			Cache:      protoSeq.Cache,
			Increment:  protoSeq.Increment,
			Cycle:      protoSeq.Cycle,
		}
		if setVal := protoSeq.GetSetVal(); setVal != nil {
			seq.SetVal = &SequenceSetVal{
				NextValue: setVal.NextValue,
				NextUsed:  setVal.NextUsed,
			}
		}
		underlyingValue = seq
	}

	return &DefaultValue{
		underlyingValue: underlyingValue,
	}
}

func (d *DefaultValue) Literal() value.Value {
	literal, ok := d.underlyingValue.(DefaultLiteralValue)
	if ok {
		return literal
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
		targetColumn.DefaultValue = value.toYDB()
	case DefaultSequenceValue:
		targetColumn.DefaultValue = value.toYDB()
	}
}

func (l *DefaultLiteralValue) toYDB() *Ydb_Table.ColumnMeta_FromLiteral {
	return &Ydb_Table.ColumnMeta_FromLiteral{
		FromLiteral: value.ToYDB(l.Value),
	}
}

func (s *DefaultSequenceValue) toYDB() *Ydb_Table.ColumnMeta_FromSequence {
	protoSeq := &Ydb_Table.SequenceDescription{
		Name:       s.Name,
		MinValue:   s.MinValue,
		MaxValue:   s.MaxValue,
		StartValue: s.StartValue,
		Cache:      s.Cache,
		Increment:  s.Increment,
		Cycle:      s.Cycle,
	}

	if s.SetVal != nil {
		protoSeq.SetVal = &Ydb_Table.SequenceDescription_SetVal{
			NextValue: s.SetVal.NextValue,
			NextUsed:  s.SetVal.NextUsed,
		}
	}

	return &Ydb_Table.ColumnMeta_FromSequence{
		FromSequence: protoSeq,
	}
}
