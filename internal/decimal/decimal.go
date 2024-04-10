package decimal

import (
	"errors"
	"math/big"
	"math/bits"
	"strings"
)

const wordSize = bits.UintSize / 8

var (
	ten  = big.NewInt(10)
	zero = big.NewInt(0)
	one  = big.NewInt(1)
	inf  = big.NewInt(0).Mul(
		big.NewInt(100000000000000000),
		big.NewInt(1000000000000000000),
	)
	nan    = big.NewInt(0).Add(inf, one)
	err    = big.NewInt(0).Add(nan, one)
	neginf = big.NewInt(0).Neg(inf)
	negnan = big.NewInt(0).Neg(nan)
)

const (
	errorTag = "<error>"
)

// IsInf reports whether x is an infinity.
func IsInf(x *big.Int) bool { return x.CmpAbs(inf) == 0 }

// IsNaN reports whether x is a "not-a-number" value.
func IsNaN(x *big.Int) bool { return x.CmpAbs(nan) == 0 }

// IsErr reports whether x is an "error" value.
func IsErr(x *big.Int) bool { return x.Cmp(err) == 0 }

// Inf returns infinity value.
func Inf() *big.Int { return big.NewInt(0).Set(inf) }

// NaN returns "not-a-number" value.
func NaN() *big.Int { return big.NewInt(0).Set(nan) }

// Err returns "error" value.
func Err() *big.Int { return big.NewInt(0).Set(err) }

// FromBytes converts bytes representation of decimal to big integer.
// Most callers should use FromInt128().
//
// If given bytes contains value that is greater than given precision it
// returns infinity or negative infinity value accordingly the bytes sign.
func FromBytes(bts []byte, precision, scale uint32) *big.Int {
	v := big.NewInt(0)
	if len(bts) == 0 {
		return v
	}

	v.SetBytes(bts)

	neg := bts[0]&0x80 != 0
	if neg {
		// Given bytes contains negative value.
		// Interpret is as two's complement.
		not(v)
		v.Add(v, one)
		v.Neg(v)
	}
	if v.CmpAbs(pow(ten, precision)) >= 0 {
		if neg {
			v.Set(neginf)
		} else {
			v.Set(inf)
		}
	}

	return v
}

// FromInt128 returns big integer from given array. That is, it interprets
// 16-byte array as 128-bit integer.
func FromInt128(p [16]byte, precision, scale uint32) *big.Int {
	return FromBytes(p[:], precision, scale)
}

// Parse interprets a string s with the given precision and scale and returns
// the corresponding big integer.
func Parse(s string, precision, scale uint32) (*big.Int, error) {
	if scale > precision {
		return nil, precisionError(s, precision, scale)
	}

	v := big.NewInt(0)
	if s == "" {
		return v, nil
	}

	if SetSpecialValue(v, s) {
		return v, nil
	}

	neg, s := parseSign(s)

	integral := precision - scale
	s, err := parseNumber(s, v, integral, scale)
	if err != nil {
		return nil, err
	}

	if len(s) > 0 {
		if err := handleRemainingDigits(s, v, precision); err != nil {
			return nil, err
		}
	}
	v.Mul(v, pow(ten, scale))
	if neg {
		v.Neg(v)
	}

	return v, nil
}

func SetSpecialValue(v *big.Int, s string) bool {
	neg, s := parseSign(s)

	if isInf(s) {
		if neg {
			v.Set(neginf)
		} else {
			v.Set(inf)
		}

		return true
	}
	if isNaN(s) {
		if neg {
			v.Set(negnan)
		} else {
			v.Set(nan)
		}

		return true
	}

	return false
}

func handleRemainingDigits(s string, v *big.Int, precision uint32) error {
	c := s[0]
	if !isDigit(c) {
		return syntaxError(s)
	}

	if c >= '5' {
		if c > '5' || shouldRoundUp(v, s) {
			v.Add(v, one)
			if v.Cmp(pow(ten, precision)) >= 0 {
				v.Set(inf)
			}
		}
	}

	return nil
}

func shouldRoundUp(v *big.Int, s string) bool {
	var x big.Int
	plus := x.And(v, one).Cmp(zero) != 0
	for !plus && len(s) > 0 {
		c := s[0]
		s = s[1:]
		if c < '0' || c > '9' {
			break
		}
		plus = c != '0'
	}

	return plus
}

func parseSign(s string) (neg bool, remaining string) {
	if s == "" {
		return false, s
	}

	neg = s[0] == '-'
	if neg || s[0] == '+' {
		s = s[1:]
	}

	return neg, s
}

func parseNumber(s string, v *big.Int, integral, scale uint32) (remaining string, err error) {
	var dot bool
	var processed bool
	var remainingBuilder strings.Builder

	for _, c := range s {
		if c == '.' {
			if dot {
				return "", errors.New("syntax error: unexpected '.'")
			}
			dot = true

			continue
		}

		if !isDigit(byte(c)) {
			return "", errors.New("syntax error: non-digit characters")
		}

		if dot && scale > 0 {
			scale--
		} else if !dot {
			if integral == 0 {
				remainingBuilder.WriteRune(c)
				processed = true

				continue
			}
			integral--
		}

		if !processed {
			digitVal := big.NewInt(int64(c - '0'))
			v.Mul(v, big.NewInt(10))
			v.Add(v, digitVal)
		}
	}

	if !dot && scale > 0 {
		for scale > 0 {
			v.Mul(v, big.NewInt(10))
			scale--
		}
	}

	// Convert the strings.Builder content to a string
	return remainingBuilder.String(), nil
}

// Format returns the string representation of x with the given precision and
// scale.
func Format(x *big.Int, precision, scale uint32) string {
	// Check for special values and nil pointer upfront.
	if x == nil {
		return "0"
	}
	if x.CmpAbs(inf) == 0 {
		if x.Sign() < 0 {
			return "-inf"
		}

		return "inf"
	}
	if x.CmpAbs(nan) == 0 {
		if x.Sign() < 0 {
			return "-nan"
		}

		return "nan"
	}

	v, neg := abs(x)
	bts, pos := newStringBuffer()

	var digit big.Int
	for ; v.Cmp(zero) > 0; v.Div(v, ten) {
		if precision == 0 {
			return errorTag
		}
		precision--

		digit.Mod(v, ten)
		d := int(digit.Int64())

		pos--
		if d != 0 || scale == 0 || pos >= 0 {
			setDigitAtPosition(bts, pos, d)
		}

		if scale > 0 {
			scale--
			if scale == 0 && pos > 0 {
				bts[pos-1] = '.'
				pos--
			}
		}
	}

	for ; scale > 0; scale-- {
		if precision == 0 {
			pos = 0

			break
		}
		precision--
		pos--
		bts[pos] = '0'
	}

	if bts[pos] == '.' {
		pos--
		bts[pos] = '0'
	}

	if neg {
		pos--
		bts[pos] = '-'
	}

	return string(bts[pos:])
}

func abs(x *big.Int) (*big.Int, bool) {
	v := big.NewInt(0).Set(x)
	neg := x.Sign() < 0
	if neg {
		// Convert negative to positive.
		v.Neg(x)
	}

	return v, neg
}

func newStringBuffer() ([]byte, int) {
	// log_{10}(2^120) ~= 36.12, 37 decimal places
	// plus dot, zero before dot, sign.
	bts := make([]byte, 40)
	pos := len(bts)

	return bts, pos
}

func setDigitAtPosition(bts []byte, pos, digit int) {
	const numbers = "0123456789"
	bts[pos] = numbers[digit]
}

// BigIntToByte returns the 16-byte array representation of x.
//
// If x value does not fit in 16 bytes with given precision, it returns 16-byte
// representation of infinity or negative infinity value accordingly to x's sign.
func BigIntToByte(x *big.Int, precision, scale uint32) (p [16]byte) {
	if !IsInf(x) && !IsNaN(x) && !IsErr(x) && x.CmpAbs(pow(ten, precision)) >= 0 {
		if x.Sign() < 0 {
			x = neginf
		} else {
			x = inf
		}
	}
	put(x, p[:])

	return p
}

func put(x *big.Int, p []byte) {
	neg := x.Sign() < 0
	if neg {
		x = complement(x)
	}
	i := len(p)
	for _, d := range x.Bits() {
		for j := 0; j < wordSize; j++ {
			i--
			p[i] = byte(d)
			d >>= 8
		}
	}
	var pad byte
	if neg {
		pad = 0xff
	}
	for 0 < i && i < len(p) {
		i--
		p[i] = pad
	}
}

func Append(p []byte, x *big.Int) []byte {
	n := len(p)
	p = ensure(p, size(x))
	put(x, p[n:])

	return p
}

func size(x *big.Int) int {
	if x.Sign() < 0 {
		x = complement(x)
	}

	return len(x.Bits()) * wordSize
}

func ensure(p []byte, n int) []byte {
	var (
		l = len(p)
		c = cap(p)
	)
	if c-l < n {
		cp := make([]byte, l+n)
		copy(cp, p)
		p = cp
	}

	return p[:l+n]
}

// not is almost the same as x.Not() but without handling the sign of x.
// That is, it more similar to x.Xor(ones) where ones is x bits all set to 1.
func not(x *big.Int) {
	abs := x.Bits()
	for i, d := range abs {
		abs[i] = ^d
	}
}

// pow returns new instance of big.Int equal to x^n.
func pow(x *big.Int, n uint32) *big.Int {
	var (
		v = big.NewInt(1)
		m = big.NewInt(0).Set(x)
	)
	for n > 0 {
		if n&1 != 0 {
			v.Mul(v, m)
		}
		n >>= 1
		m.Mul(m, m)
	}

	return v
}

// complement returns two's complement of x.
// x must be negative.
func complement(x *big.Int) *big.Int {
	x = big.NewInt(0).Set(x)
	not(x)
	x.Neg(x)
	x.Add(x, one)

	return x
}

func isInf(s string) bool {
	return len(s) >= 3 && (s[0] == 'i' || s[0] == 'I') && (s[1] == 'n' || s[1] == 'N') && (s[2] == 'f' || s[2] == 'F')
}

func isNaN(s string) bool {
	return len(s) >= 3 && (s[0] == 'n' || s[0] == 'N') && (s[1] == 'a' || s[1] == 'A') && (s[2] == 'n' || s[2] == 'N')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}
