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

func TestShouldRoundUp(t *testing.T) {
	tests := []struct {
		name             string
		number           *big.Int
		additionalDigits string
		expected         bool
	}{
		{
			name:             "Last digit not zero, no string",
			number:           big.NewInt(123),
			additionalDigits: "",
			expected:         true,
		},
		{
			name:             "Last digit zero, string starts not with zero",
			number:           big.NewInt(120),
			additionalDigits: "1",
			expected:         true,
		},
		{
			name:             "Last digit zero, string all zeros",
			number:           big.NewInt(120),
			additionalDigits: "000",
			expected:         false,
		},
		{
			name:             "Last digit not zero, string irrelevant",
			number:           big.NewInt(123),
			additionalDigits: "004",
			expected:         true,
		},
		{
			name:             "Last digit zero, string has non-zero after zeros",
			number:           big.NewInt(100),
			additionalDigits: "001",
			expected:         true,
		},
		{
			name:             "Last digit zero, string has non-digit characters",
			number:           big.NewInt(100),
			additionalDigits: "00abc",
			expected:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldRoundUp(tt.number, tt.additionalDigits)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestParseSign(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedNeg bool
		expectedRem string
	}{
		{
			name:        "Negative sign",
			input:       "-123",
			expectedNeg: true,
			expectedRem: "123",
		},
		{
			name:        "Positive sign",
			input:       "+456",
			expectedNeg: false,
			expectedRem: "456",
		},
		{
			name:        "No sign",
			input:       "789",
			expectedNeg: false,
			expectedRem: "789",
		},
		{
			name:        "Empty string",
			input:       "",
			expectedNeg: false,
			expectedRem: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			neg, rem := parseSign(tt.input)
			require.Equal(t, tt.expectedNeg, neg, "Neg flag does not match expected value")
			require.Equal(t, tt.expectedRem, rem, "Remaining string does not match expected value")
		})
	}
}

func TestHandleSpecialValues(t *testing.T) {
	tests := []struct {
		name           string
		neg            bool
		initialValue   *big.Int
		posValue       *big.Int
		negValue       *big.Int
		expectedResult *big.Int
	}{
		{
			name:           "Handle positive special value",
			neg:            false,
			initialValue:   big.NewInt(0),
			posValue:       big.NewInt(123),
			negValue:       big.NewInt(-123),
			expectedResult: big.NewInt(123),
		},
		{
			name:           "Handle negative special value",
			neg:            true,
			initialValue:   big.NewInt(0),
			posValue:       big.NewInt(123),
			negValue:       big.NewInt(-123),
			expectedResult: big.NewInt(-123),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handleSpecialValues(tt.initialValue, tt.neg, tt.posValue, tt.negValue)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.expectedResult, result, "The result should match the expected value")
		})
	}
}

func TestParseNumber(t *testing.T) {
	tests := []struct {
		name            string
		s               string
		initialValue    *big.Int
		initialIntegral uint32
		initialScale    uint32
		expectedValue   *big.Int
		expectedRemain  string
		expectError     bool
	}{
		{
			name:            "Parse integer",
			s:               "123",
			initialValue:    big.NewInt(0),
			initialIntegral: 3,
			initialScale:    0,
			expectedValue:   big.NewInt(123),
			expectedRemain:  "",
			expectError:     false,
		},
		{
			name:            "Parse floating point",
			s:               "123.45",
			initialValue:    big.NewInt(0),
			initialIntegral: 3,
			initialScale:    2,
			expectedValue:   big.NewInt(12345),
			expectedRemain:  "",
			expectError:     false,
		},
		{
			name:            "Non-digit character",
			s:               "123x45",
			initialValue:    big.NewInt(0),
			initialIntegral: 3,
			initialScale:    2,
			expectedValue:   nil,
			expectedRemain:  "",
			expectError:     true,
		},
		{
			name:            "Multiple dots",
			s:               "123.45.67",
			initialValue:    big.NewInt(0),
			initialIntegral: 3,
			initialScale:    2,
			expectedValue:   nil,
			expectedRemain:  "",
			expectError:     true,
		},
		{
			name:            "Early termination by integral",
			s:               "12345",
			initialValue:    big.NewInt(0),
			initialIntegral: 3,
			initialScale:    0,
			expectedValue:   big.NewInt(123),
			expectedRemain:  "45",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			integral := tt.initialIntegral
			scale := tt.initialScale
			remain, err := parseNumber(tt.s, tt.initialValue, &integral, &scale)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedValue, tt.initialValue)
				require.Equal(t, tt.expectedRemain, remain)
			}
		})
	}
}

func TestHandleRemainingDigits(t *testing.T) {
	tests := []struct {
		value       *big.Int
		expected    *big.Int
		name        string
		inputString string
		precision   uint32
		expectErr   bool
	}{
		{
			name:        "No rounding needed",
			inputString: "4",
			value:       big.NewInt(1),
			precision:   3,
			expected:    big.NewInt(1),
			expectErr:   false,
		},
		{
			name:        "Rounding up needed",
			inputString: "6",
			value:       big.NewInt(1),
			precision:   3,
			expected:    big.NewInt(2),
			expectErr:   false,
		},
		{
			name:        "Exactly halfway - assume round up for test",
			inputString: "50",
			value:       big.NewInt(1),
			precision:   3,
			expected:    big.NewInt(2),
			expectErr:   false,
		},
		{
			name:        "Invalid character",
			inputString: "a",
			value:       big.NewInt(1),
			precision:   3,
			expected:    nil,
			expectErr:   true,
		},
		{
			name:        "Exceeds precision limit - set to inf",
			inputString: "9",         // Triggers rounding that should exceed precision
			value:       pow(ten, 2), // Set v close to precision limit
			precision:   2,           // Precision limit
			expected:    inf,         // Expected to be set to inf
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleRemainingDigits(tt.inputString, tt.value, tt.precision)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expected.Cmp(inf) == 0 {
					require.Equal(t, 0, tt.expected.Cmp(tt.value), "v should be set to inf")
				} else {
					require.Equal(t, tt.expected.String(), tt.value.String(), "Expected and actual values should match")
				}
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
			value, neg := prepareValue(tt.input)
			require.Equal(t, tt.expectedValue, value)
			require.Equal(t, tt.expectedNeg, neg)
		})
	}
}

func TestInitializeBuffer(t *testing.T) {
	bts, pos := initializeBuffer()
	require.Len(t, bts, 40)
	require.Equal(t, 40, pos)
}

func TestAppendDigit(t *testing.T) {
	bts, _ := initializeBuffer()
	pos := len(bts)
	pos = appendDigit(bts, pos, 5)

	expectedByte := byte('5') // '5' as a byte, equivalent to 0x35 in hexadecimal
	actualByte := bts[pos]    // This should now correctly point to the appended '5'

	require.Equal(t, expectedByte, actualByte, "The appended byte should match '5'")
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
