package decimal

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
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
		err bool
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
		{
			s:   "invalid",
			err: true,
		},
		{
			s:   "123.invalid",
			err: true,
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			n, exp, err := ParseDecimal(tt.s)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.n, n)
				require.Equal(t, tt.exp, exp)
			}
		})
	}
}

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		name      string
		s         string
		precision uint32
		scale     uint32
		expected  *big.Int
		err       bool
	}{
		{
			name:      "empty string",
			s:         "",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(0),
		},
		{
			name:      "positive integer",
			s:         "123",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(123000000000),
		},
		{
			name:      "negative integer",
			s:         "-123",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(-123000000000),
		},
		{
			name:      "positive with plus sign",
			s:         "+123",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(123000000000),
		},
		{
			name:      "decimal number",
			s:         "123.456",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(123456000000),
		},
		{
			name:      "decimal with trailing zeros truncated",
			s:         "123.4567890123",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(123456789012),
		},
		{
			name:      "inf lowercase",
			s:         "inf",
			precision: 22,
			scale:     9,
			expected:  Inf(),
		},
		{
			name:      "inf uppercase",
			s:         "INF",
			precision: 22,
			scale:     9,
			expected:  Inf(),
		},
		{
			name:      "inf mixed case",
			s:         "InF",
			precision: 22,
			scale:     9,
			expected:  Inf(),
		},
		{
			name:      "negative inf",
			s:         "-inf",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(0).Neg(Inf()),
		},
		{
			name:      "positive inf with plus",
			s:         "+inf",
			precision: 22,
			scale:     9,
			expected:  Inf(),
		},
		{
			name:      "nan lowercase",
			s:         "nan",
			precision: 22,
			scale:     9,
			expected:  NaN(),
		},
		{
			name:      "nan uppercase",
			s:         "NAN",
			precision: 22,
			scale:     9,
			expected:  NaN(),
		},
		{
			name:      "nan mixed case",
			s:         "NaN",
			precision: 22,
			scale:     9,
			expected:  NaN(),
		},
		{
			name:      "negative nan",
			s:         "-nan",
			precision: 22,
			scale:     9,
			expected:  big.NewInt(0).Neg(NaN()),
		},
		{
			name:      "scale greater than precision",
			s:         "123",
			precision: 5,
			scale:     10,
			err:       true,
		},
		{
			name:      "double dot syntax error",
			s:         "123..456",
			precision: 22,
			scale:     9,
			err:       true,
		},
		{
			name:      "invalid character",
			s:         "12a34",
			precision: 22,
			scale:     9,
			err:       true,
		},
		{
			name:      "invalid character after dot",
			s:         "12.3a4",
			precision: 22,
			scale:     9,
			err:       true,
		},
		{
			name:      "overflow to infinity",
			s:         "9999999999999999999999999",
			precision: 10,
			scale:     0,
			expected:  Inf(),
		},
		{
			name:      "negative overflow to negative infinity",
			s:         "-9999999999999999999999999",
			precision: 10,
			scale:     0,
			expected:  big.NewInt(0).Neg(Inf()),
		},
		{
			name:      "rounding up when digit > 5",
			s:         "1.236",
			precision: 22,
			scale:     2,
			expected:  big.NewInt(124),
		},
		{
			name:      "rounding with digit = 5 and odd last",
			s:         "1.235",
			precision: 22,
			scale:     2,
			expected:  big.NewInt(124),
		},
		{
			name:      "rounding with digit = 5 and trailing non-zero",
			s:         "1.2451",
			precision: 22,
			scale:     2,
			expected:  big.NewInt(125),
		},
		{
			name:      "rounding with digit = 5 and even last no trailing",
			s:         "1.245",
			precision: 22,
			scale:     2,
			expected:  big.NewInt(124), // banker's rounding - even stays
		},
		{
			name:      "invalid digit in rounding sequence",
			s:         "1.23a",
			precision: 22,
			scale:     2,
			err:       true,
		},
		{
			name:      "rounding causes overflow to infinity",
			s:         "9999999999.99999999999",
			precision: 10,
			scale:     0,
			expected:  Inf(),
		},
		{
			name:      "invalid char in trailing digits after 5 with even last digit",
			s:         "1.245x",
			precision: 22,
			scale:     2,
			err:       true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.s, tt.precision, tt.scale)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 0, tt.expected.Cmp(result), "expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsInfNaNErr(t *testing.T) {
	t.Run("IsInf", func(t *testing.T) {
		require.True(t, IsInf(Inf()))
		require.True(t, IsInf(big.NewInt(0).Neg(Inf())))
		require.False(t, IsInf(big.NewInt(123)))
		require.False(t, IsInf(NaN()))
	})

	t.Run("IsNaN", func(t *testing.T) {
		require.True(t, IsNaN(NaN()))
		require.True(t, IsNaN(big.NewInt(0).Neg(NaN())))
		require.False(t, IsNaN(big.NewInt(123)))
		require.False(t, IsNaN(Inf()))
	})

	t.Run("IsErr", func(t *testing.T) {
		require.True(t, IsErr(Err()))
		require.False(t, IsErr(big.NewInt(123)))
		require.False(t, IsErr(Inf()))
		require.False(t, IsErr(NaN()))
	})
}

func TestInfNaNErr(t *testing.T) {
	t.Run("Inf returns copy", func(t *testing.T) {
		i1 := Inf()
		i2 := Inf()
		require.Equal(t, 0, i1.Cmp(i2))
		i1.SetInt64(0)
		require.NotEqual(t, 0, Inf().Cmp(i1))
	})

	t.Run("NaN returns copy", func(t *testing.T) {
		n1 := NaN()
		n2 := NaN()
		require.Equal(t, 0, n1.Cmp(n2))
		n1.SetInt64(0)
		require.NotEqual(t, 0, NaN().Cmp(n1))
	})

	t.Run("Err returns copy", func(t *testing.T) {
		e1 := Err()
		e2 := Err()
		require.Equal(t, 0, e1.Cmp(e2))
		e1.SetInt64(0)
		require.NotEqual(t, 0, Err().Cmp(e1))
	})
}

func TestFromInt128(t *testing.T) {
	t.Run("simple positive", func(t *testing.T) {
		var p [16]byte
		binary.BigEndian.PutUint64(p[8:], 1000000000)
		result := FromInt128(p, 22)
		require.Equal(t, 0, big.NewInt(1000000000).Cmp(result))
	})

	t.Run("zero bytes", func(t *testing.T) {
		var p [16]byte
		result := FromInt128(p, 22)
		require.Equal(t, 0, big.NewInt(0).Cmp(result))
	})
}

func TestBigIntToByte(t *testing.T) {
	t.Run("normal value", func(t *testing.T) {
		x := big.NewInt(123456789)
		p := BigIntToByte(x, 22)
		result := FromInt128(p, 22)
		require.Equal(t, 0, x.Cmp(result))
	})

	t.Run("negative value", func(t *testing.T) {
		x := big.NewInt(-123456789)
		p := BigIntToByte(x, 22)
		result := FromInt128(p, 22)
		require.Equal(t, 0, x.Cmp(result))
	})

	t.Run("overflow positive becomes inf", func(t *testing.T) {
		x := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(25), nil)
		p := BigIntToByte(x, 22)
		result := FromInt128(p, 22)
		require.True(t, IsInf(result))
	})

	t.Run("overflow negative becomes neginf", func(t *testing.T) {
		x := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(25), nil)
		x.Neg(x)
		p := BigIntToByte(x, 22)
		result := FromInt128(p, 22)
		require.True(t, IsInf(result))
		require.True(t, result.Sign() < 0)
	})

	t.Run("inf stays inf", func(t *testing.T) {
		x := Inf()
		p := BigIntToByte(x, 22)
		result := FromInt128(p, 22)
		require.True(t, IsInf(result))
	})

	t.Run("nan converted to bytes", func(t *testing.T) {
		x := NaN()
		p := BigIntToByte(x, 22)
		// NaN is larger than any precision, so FromInt128 will interpret as inf
		result := FromInt128(p, 22)
		require.True(t, IsInf(result))
	})

	t.Run("err converted to bytes", func(t *testing.T) {
		x := Err()
		p := BigIntToByte(x, 22)
		// Err is larger than any precision, so FromInt128 will interpret as inf
		result := FromInt128(p, 22)
		require.True(t, IsInf(result))
	})
}

func TestFormat(t *testing.T) {
	for _, tt := range []struct {
		name              string
		x                 *big.Int
		precision         uint32
		scale             uint32
		trimTrailingZeros bool
		expected          string
	}{
		{
			name:      "nil value",
			x:         nil,
			precision: 22,
			scale:     9,
			expected:  "0",
		},
		{
			name:      "zero precision returns nan",
			x:         big.NewInt(123),
			precision: 0,
			scale:     0,
			expected:  "nan",
		},
		{
			name:      "negative zero precision returns -nan",
			x:         big.NewInt(-123),
			precision: 0,
			scale:     0,
			expected:  "-nan",
		},
		{
			name:      "positive inf",
			x:         Inf(),
			precision: 22,
			scale:     9,
			expected:  "inf",
		},
		{
			name:      "negative inf",
			x:         big.NewInt(0).Neg(Inf()),
			precision: 22,
			scale:     9,
			expected:  "-inf",
		},
		{
			name:      "positive nan",
			x:         NaN(),
			precision: 22,
			scale:     9,
			expected:  "nan",
		},
		{
			name:      "negative nan",
			x:         big.NewInt(0).Neg(NaN()),
			precision: 22,
			scale:     9,
			expected:  "-nan",
		},
		{
			name:      "simple integer",
			x:         big.NewInt(123000000000),
			precision: 22,
			scale:     9,
			expected:  "123.000000000",
		},
		{
			name:              "simple integer with trim",
			x:                 big.NewInt(123000000000),
			precision:         22,
			scale:             9,
			trimTrailingZeros: true,
			expected:          "123",
		},
		{
			name:      "negative number",
			x:         big.NewInt(-123456000000),
			precision: 22,
			scale:     9,
			expected:  "-123.456000000",
		},
		{
			name:              "negative number with trim",
			x:                 big.NewInt(-123456000000),
			precision:         22,
			scale:             9,
			trimTrailingZeros: true,
			expected:          "-123.456",
		},
		{
			name:      "zero",
			x:         big.NewInt(0),
			precision: 22,
			scale:     9,
			expected:  "0.000000000",
		},
		{
			name:              "zero with trim still shows zeros due to scale handling",
			x:                 big.NewInt(0),
			precision:         22,
			scale:             9,
			trimTrailingZeros: true,
			expected:          "0.000000000",
		},
		{
			name:      "small decimal",
			x:         big.NewInt(1),
			precision: 22,
			scale:     9,
			expected:  "0.000000001",
		},
		{
			name:      "precision exhausted returns error tag",
			x:         big.NewInt(0).Exp(big.NewInt(10), big.NewInt(50), nil),
			precision: 10,
			scale:     0,
			expected:  "<error>",
		},
		{
			name:      "precision exhausted in scale fill returns error tag",
			x:         big.NewInt(0),
			precision: 2,
			scale:     10,
			expected:  "<error>",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Format(tt.x, tt.precision, tt.scale, tt.trimTrailingZeros)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestFromBytesEdgeCases(t *testing.T) {
	t.Run("empty bytes", func(t *testing.T) {
		result := FromBytes([]byte{}, 22)
		require.Equal(t, big.NewInt(0), result)
	})

	t.Run("positive overflow to inf", func(t *testing.T) {
		bts := uint128(0x7fffffffffffffff, 0xffffffffffffffff)
		result := FromBytes(bts, 10)
		require.True(t, IsInf(result))
		require.True(t, result.Sign() > 0)
	})
}

func TestDecimalType(t *testing.T) {
	t.Run("ToDecimal", func(t *testing.T) {
		original := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			Precision: 22,
			Scale:     9,
		}
		result := ToDecimal(original)
		require.Equal(t, original.Bytes, result.Bytes)
		require.Equal(t, original.Precision, result.Precision)
		require.Equal(t, original.Scale, result.Scale)
	})

	t.Run("Decimal method", func(t *testing.T) {
		d := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			Precision: 22,
			Scale:     9,
		}
		bytes, precision, scale := d.Decimal()
		require.Equal(t, d.Bytes, bytes)
		require.Equal(t, d.Precision, precision)
		require.Equal(t, d.Scale, scale)
	})

	t.Run("String", func(t *testing.T) {
		d := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 154, 202, 0},
			Precision: 22,
			Scale:     9,
		}
		result := d.String()
		require.Equal(t, "1.000000000", result)
	})

	t.Run("Format method", func(t *testing.T) {
		d := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 154, 202, 0},
			Precision: 22,
			Scale:     9,
		}
		require.Equal(t, "1.000000000", d.Format(false))
		require.Equal(t, "1", d.Format(true))
	})

	t.Run("BigInt", func(t *testing.T) {
		d := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 154, 202, 0},
			Precision: 22,
			Scale:     9,
		}
		result := d.BigInt()
		require.Equal(t, big.NewInt(1000000000), result)
	})
}

type testDecimalInterface struct {
	bytes     [16]byte
	precision uint32
	scale     uint32
}

func (t *testDecimalInterface) Decimal() ([16]byte, uint32, uint32) {
	return t.bytes, t.precision, t.scale
}

type testValuer struct {
	value any
	err   error
}

func (t *testValuer) Value() (driver.Value, error) {
	return t.value, t.err
}

func TestDecimalScan(t *testing.T) {
	t.Run("scan from Interface", func(t *testing.T) {
		iface := &testDecimalInterface{
			bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			precision: 22,
			scale:     9,
		}
		var d Decimal
		err := d.Scan(iface)
		require.NoError(t, err)
		require.Equal(t, iface.bytes, d.Bytes)
		require.Equal(t, iface.precision, d.Precision)
		require.Equal(t, iface.scale, d.Scale)
	})

	t.Run("scan from *Decimal", func(t *testing.T) {
		original := &Decimal{
			Bytes:     [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
			Precision: 22,
			Scale:     9,
		}
		var d Decimal
		err := d.Scan(original)
		require.NoError(t, err)
		require.Equal(t, original.Bytes, d.Bytes)
		require.Equal(t, original.Precision, d.Precision)
		require.Equal(t, original.Scale, d.Scale)
	})

	t.Run("scan from string", func(t *testing.T) {
		var d Decimal
		err := d.Scan("123.456")
		require.NoError(t, err)
		require.NotEmpty(t, d.Bytes)
	})

	t.Run("scan from invalid string", func(t *testing.T) {
		var d Decimal
		err := d.Scan("invalid")
		require.Error(t, err)
	})

	t.Run("scan from driver.Valuer", func(t *testing.T) {
		valuer := &testValuer{value: "123.456", err: nil}
		var d Decimal
		err := d.Scan(valuer)
		require.NoError(t, err)
	})

	t.Run("scan from driver.Valuer with error", func(t *testing.T) {
		valuer := &testValuer{value: nil, err: errors.New("valuer error")}
		var d Decimal
		err := d.Scan(valuer)
		require.Error(t, err)
	})

	t.Run("scan from driver.Valuer with invalid value", func(t *testing.T) {
		valuer := &testValuer{value: "invalid", err: nil}
		var d Decimal
		err := d.Scan(valuer)
		require.Error(t, err)
	})

	t.Run("scan from unsupported type", func(t *testing.T) {
		var d Decimal
		err := d.Scan(12345)
		require.Error(t, err)
	})
}

func TestParseError(t *testing.T) {
	t.Run("Error method", func(t *testing.T) {
		pe := &ParseError{
			Err:   errors.New("test error"),
			Input: "test input",
		}
		errStr := pe.Error()
		require.Contains(t, errStr, "test input")
		require.Contains(t, errStr, "test error")
	})

	t.Run("Unwrap method", func(t *testing.T) {
		innerErr := errors.New("inner error")
		pe := &ParseError{
			Err:   innerErr,
			Input: "test input",
		}
		require.Equal(t, innerErr, pe.Unwrap())
	})

	t.Run("syntax error through Parse", func(t *testing.T) {
		_, err := Parse("12a34", 22, 9)
		require.Error(t, err)
		var pe *ParseError
		require.True(t, errors.As(err, &pe))
	})

	t.Run("precision error through Parse", func(t *testing.T) {
		_, err := Parse("123", 5, 10)
		require.Error(t, err)
		var pe *ParseError
		require.True(t, errors.As(err, &pe))
		require.Contains(t, pe.Error(), "precision")
	})
}
