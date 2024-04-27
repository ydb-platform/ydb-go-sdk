package decimal

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromBytes(t *testing.T) {
	for _, test := range []struct {
		name      string
		bts       []byte
		precision uint32
		scale     uint32
	}{
		{
			bts:       uint128(0xffffffffffffffff, 0xffffffffffffffff),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0xffffffffffffffff, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0x4000000000000000, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128(0x8000000000000000, 0),
			precision: 22,
			scale:     9,
		},
		{
			bts:       uint128s(1000000000),
			precision: 22,
			scale:     9,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			x := FromBytes(test.bts, test.precision, test.scale)
			p := Append(nil, x)
			y := FromBytes(p, test.precision, test.scale)
			if x.Cmp(y) != 0 {
				t.Errorf(
					"parsed bytes serialized to different value: %v; want %v",
					x, y,
				)
			}
			t.Logf(
				"%s %s",
				Format(x, test.precision, test.scale),
				Format(y, test.precision, test.scale),
			)
		})
	}
}

func TestSetSpecialValue(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedS   string
		expectedNeg bool
		expectedV   *big.Int
	}{
		{
			name:        "Positive infinity",
			input:       "inf",
			expectedS:   "inf",
			expectedNeg: false,
			expectedV:   inf,
		},
		{
			name:        "Negative infinity",
			input:       "-inf",
			expectedS:   "inf",
			expectedNeg: true,
			expectedV:   neginf,
		},
		{
			name:        "Positive NaN",
			input:       "nan",
			expectedS:   "nan",
			expectedNeg: false,
			expectedV:   nan,
		},
		{
			name:        "Negative NaN",
			input:       "-nan",
			expectedS:   "nan",
			expectedNeg: true,
			expectedV:   negnan,
		},
		{
			name:        "Regular number",
			input:       "123",
			expectedS:   "123",
			expectedNeg: false,
			expectedV:   nil,
		},
		{
			name:        "Negative regular number",
			input:       "-123",
			expectedS:   "123",
			expectedNeg: true,
			expectedV:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := big.NewInt(0)
			gotS, gotNeg, gotV := setSpecialValue(tt.input, v)
			require.Equal(t, tt.expectedS, gotS)
			require.Equal(t, tt.expectedNeg, gotNeg)
			if tt.expectedV != nil {
				require.Equal(t, 0, tt.expectedV.Cmp(gotV))
			} else {
				require.Nil(t, gotV)
			}
		})
	}
}

func TestPrepareValue(t *testing.T) {
	tests := []struct {
		name          string
		input         *big.Int
		expectedValue *big.Int
		expectedNeg   bool
	}{
		{
			name:          "Positive value",
			input:         big.NewInt(123),
			expectedValue: big.NewInt(123),
			expectedNeg:   false,
		},
		{
			name:          "Negative value",
			input:         big.NewInt(-123),
			expectedValue: big.NewInt(123),
			expectedNeg:   true,
		},
		{
			name:          "Zero value",
			input:         big.NewInt(0),
			expectedValue: big.NewInt(0),
			expectedNeg:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, neg := abs(tt.input)
			require.Equal(t, tt.expectedValue, value)
			require.Equal(t, tt.expectedNeg, neg)
		})
	}
}

func TestParseNumber(t *testing.T) {
	// Mock or define these as per your actual implementation.
	tests := []struct {
		name      string
		s         string
		wantValue *big.Int
		precision uint32
		scale     uint32
		neg       bool
		wantErr   bool
	}{
		{
			name:      "Valid number without decimal",
			s:         "123",
			precision: 3,
			scale:     0,
			neg:       false,
			wantValue: big.NewInt(123),
			wantErr:   false,
		},
		{
			name:      "Valid number with decimal",
			s:         "123.45",
			precision: 5,
			scale:     2,
			neg:       false,
			wantValue: big.NewInt(12345),
			wantErr:   false,
		},
		{
			name:      "Valid negative number",
			s:         "123",
			precision: 3,
			scale:     0,
			neg:       true,
			wantValue: big.NewInt(-123),
			wantErr:   false,
		},
		{
			name:      "Syntax error with non-digit",
			s:         "123a",
			precision: 4,
			scale:     0,
			neg:       false,
			wantValue: nil,
			wantErr:   true,
		},
		{
			name:      "Multiple decimal points",
			s:         "12.3.4",
			precision: 5,
			scale:     2,
			neg:       false,
			wantValue: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := big.NewInt(0)
			gotValue, gotErr := parseNumber(tt.s, v, tt.precision, tt.scale, tt.neg)
			if tt.wantErr {
				require.Error(t, gotErr)
			} else {
				require.NoError(t, gotErr)
				require.Equal(t, 0, tt.wantValue.Cmp(gotValue))
			}
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

func TestParse(t *testing.T) {
	tests := []struct {
		name      string
		s         string
		precision uint32
		scale     uint32
	}{
		{
			name:      "Specific Parse test",
			s:         "100",
			precision: 0,
			scale:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedRes, expectedErr := oldParse(tt.s, tt.precision, tt.scale)
			res, err := Parse(tt.s, tt.precision, tt.scale)
			if expectedErr == nil {
				require.Equal(t, expectedRes, res)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string, precision, scale uint32) {
		expectedRes, expectedErr := oldParse(s, precision, scale)
		res, err := Parse(s, precision, scale)
		if expectedErr == nil {
			require.Equal(t, expectedRes, res)
		} else {
			require.Error(t, err)
		}
	})
}

func oldParse(s string, precision, scale uint32) (*big.Int, error) {
	if scale > precision {
		return nil, precisionError(s, precision, scale)
	}

	v := big.NewInt(0)
	if s == "" {
		return v, nil
	}

	if neg := s[0] == '-'; neg || s[0] == '+' {
		s = s[1:]
	}
	if isInf(s) {
		if neg := s[0] == '-'; neg {
			return v.Set(neginf), nil
		}

		return v.Set(inf), nil
	}
	if isNaN(s) {
		if neg := s[0] == '-'; neg {
			return v.Set(negnan), nil
		}

		return v.Set(nan), nil
	}

	integral := precision - scale

	var dot bool
	for ; len(s) > 0; s = s[1:] {
		c := s[0]
		if c == '.' {
			if dot {
				return nil, syntaxError(s)
			}
			dot = true

			continue
		}
		if dot {
			if scale > 0 {
				scale--
			} else {
				break
			}
		}

		if !isDigit(c) {
			return nil, syntaxError(s)
		}

		v.Mul(v, ten)
		v.Add(v, big.NewInt(int64(c-'0')))

		if !dot && v.Cmp(zero) > 0 && integral == 0 {
			if neg := s[0] == '-'; neg {
				return neginf, nil
			}

			return inf, nil
		}
		integral--
	}
	//nolint:nestif
	if len(s) > 0 { // Characters remaining.
		c := s[0]
		if !isDigit(c) {
			return nil, syntaxError(s)
		}
		plus := c > '5'
		if !plus && c == '5' {
			var x big.Int
			plus = x.And(v, one).Cmp(zero) != 0 // Last digit is not a zero.
			for !plus && len(s) > 1 {
				s = s[1:]
				c := s[0]
				if !isDigit(c) {
					return nil, syntaxError(s)
				}
				plus = c != '0'
			}
		}
		if plus {
			v.Add(v, one)
			if v.Cmp(pow(ten, precision)) >= 0 {
				v.Set(inf)
			}
		}
	}
	v.Mul(v, pow(ten, scale))
	if neg := s[0] == '-'; neg {
		v.Neg(v)
	}

	return v, nil
}
