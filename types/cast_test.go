package types

import (
	"math/big"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsOptional(t *testing.T) {
	for _, tt := range []struct {
		t          Type
		isOptional bool
		innerType  Type
	}{
		{
			t:          nil,
			isOptional: false,
			innerType:  nil,
		},
		{
			t:          TypeBool,
			isOptional: false,
			innerType:  nil,
		},
		{
			t:          Optional(TypeBool),
			isOptional: true,
			innerType:  TypeBool,
		},
		{
			t:          Optional(Optional(TypeBool)),
			isOptional: true,
			innerType:  Optional(TypeBool),
		},
	} {
		t.Run("", func(t *testing.T) {
			isOptional, innerType := IsOptional(tt.t)
			require.Equal(t, tt.isOptional, isOptional)
			require.Equal(t, tt.innerType, innerType)
		})
	}
}

func TestToDecimal(t *testing.T) {
	for _, tt := range []struct {
		v   Value
		d   *Decimal
		err bool
	}{
		{
			v: DecimalValue(&Decimal{
				Bytes:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
				Precision: 22,
				Scale:     9,
			}),
			d: &Decimal{
				Bytes:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
				Precision: 22,
				Scale:     9,
			},
			err: false,
		},
		{
			v: DecimalValueFromBigInt(big.NewInt(123456789), 22, 9),
			d: &Decimal{
				Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 91, 205, 21},
				Precision: 22,
				Scale:     9,
			},
			err: false,
		},
		{
			v:   Uint64Value(0),
			d:   nil,
			err: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			d, err := ToDecimal(tt.v)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.d, d)
			}
		})
	}
}

func TestDecimalParse(t *testing.T) {
	for i, tt := range []struct {
		raw     string
		literal string
		err     bool
	}{
		{
			raw:     "-1234567.890123456",
			literal: `Decimal("-1234567.890123456",22,9)`,
			err:     false,
		},
		{
			raw:     "not a decimal",
			literal: ``,
			err:     true,
		},
	} {
		t.Run(strconv.Itoa(i)+"."+tt.raw, func(t *testing.T) {
			dec, err := DecimalValueFromString(tt.raw, 22, 9)
			if err != nil {
				require.True(t, tt.err)
				require.Nil(t, dec)
			} else {
				require.Equal(t, tt.literal, dec.Yql())
			}
		})
	}
}
