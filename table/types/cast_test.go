package types

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

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
