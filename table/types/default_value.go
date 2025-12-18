package types

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

type (
	DefaultValue any

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

func FromYDB(column *Ydb_Table.ColumnMeta) DefaultValue {
	if column == nil {
		return nil
	}

	if literal := column.GetFromLiteral(); literal != nil {
		return DefaultLiteralValue{
			Value: value.FromYDB(literal.GetType(), literal.GetValue()),
		}
	}

	if seq := column.GetFromSequence(); seq != nil {
		result := DefaultSequenceValue{
			Name:       seq.Name,
			MinValue:   seq.MinValue,
			MaxValue:   seq.MaxValue,
			StartValue: seq.StartValue,
			Cache:      seq.Cache,
			Increment:  seq.Increment,
			Cycle:      seq.Cycle,
		}

		if setVal := seq.GetSetVal(); setVal != nil {
			result.SetVal = &SequenceSetVal{
				NextValue: setVal.NextValue,
				NextUsed:  setVal.NextUsed,
			}
		}

		return result
	}

	return nil
}

func (l *DefaultLiteralValue) ToYDB() *Ydb_Table.ColumnMeta_FromLiteral {
	if l == nil {
		return nil
	}

	return &Ydb_Table.ColumnMeta_FromLiteral{
		FromLiteral: value.ToYDB(l.Value),
	}
}

func (s *DefaultSequenceValue) ToYDB() *Ydb_Table.ColumnMeta_FromSequence {
	if s == nil {
		return nil
	}

	seq := &Ydb_Table.SequenceDescription{
		Name:       s.Name,
		MinValue:   s.MinValue,
		MaxValue:   s.MaxValue,
		StartValue: s.StartValue,
		Cache:      s.Cache,
		Increment:  s.Increment,
		Cycle:      s.Cycle,
	}

	if s.SetVal != nil {
		seq.SetVal = &Ydb_Table.SequenceDescription_SetVal{
			NextValue: s.SetVal.NextValue,
			NextUsed:  s.SetVal.NextUsed,
		}
	}

	return &Ydb_Table.ColumnMeta_FromSequence{
		FromSequence: seq,
	}
}
