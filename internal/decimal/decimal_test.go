package decimal

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromBytes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		bts       []byte
		precision uint32
		scale     uint32
		format    map[bool]string
	}{
		{
			bts:       uint128(0xffffffffffffffff, 0xffffffffffffffff),
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "-0.000000001",
				true:  "-0.000000001",
			},
		},
		{
			bts:       uint128(0xffffffffffffffff, 0),
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "-18446744073.709551616",
				true:  "-18446744073.709551616",
			},
		},
		{
			bts:       uint128(0x4000000000000000, 0),
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "inf",
				true:  "inf",
			},
		},
		{
			bts:       uint128(0x8000000000000000, 0),
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "-inf",
				true:  "-inf",
			},
		},
		{
			bts:       uint128s(1000000000),
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "1.000000000",
				true:  "1",
			},
		},
		{
			bts:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 250, 240, 128},
			precision: 22,
			scale:     9,
			format: map[bool]string{
				false: "0.050000000",
				true:  "0.05",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			x := FromBytes(tt.bts, tt.precision)
			p := Append(nil, x)
			y := FromBytes(p, tt.precision)
			if x.Cmp(y) != 0 {
				t.Errorf(
					"parsed bytes serialized to different value: %v; want %v",
					x, y,
				)
			}
			require.Equal(t, tt.format[false], Format(x, tt.precision, tt.scale, false))
			require.Equal(t, tt.format[true], Format(x, tt.precision, tt.scale, true))
		})
	}
}

func uint128(hi, lo uint64) []byte {
	p := make([]byte, 16)
	binary.BigEndian.PutUint64(p[:8], hi)
	binary.BigEndian.PutUint64(p[8:], lo)

	return p
}

func uint128s(lo uint64) []byte {
	return uint128(0, lo)
}

func TestParseDecimal(t *testing.T) {
	for _, tt := range []struct {
		s   string
		n   *big.Int
		exp uint32
		err error
	}{
		{
			s:   "123456789",
			n:   big.NewInt(123456789),
			exp: 0,
		},
		{
			s:   "123.456",
			n:   big.NewInt(123456),
			exp: 3,
		},
		{
			s:   "0.123456789",
			n:   big.NewInt(123456789),
			exp: 9,
		},
		{
			s:   ".123456789",
			n:   big.NewInt(123456789),
			exp: 9,
		},
		{
			s:   "-123456789",
			n:   big.NewInt(-123456789),
			exp: 0,
		},
		{
			s:   "-123.456",
			n:   big.NewInt(-123456),
			exp: 3,
		},
		{
			s:   "-0.123456789",
			n:   big.NewInt(-123456789),
			exp: 9,
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			n, exp, err := ParseDecimal(tt.s)
			if tt.err != nil {
				require.ErrorIs(t, tt.err, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.n, n)
				require.Equal(t, tt.exp, exp)
			}
		})
	}
}
