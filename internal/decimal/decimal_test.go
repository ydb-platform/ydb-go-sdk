package decimal

import (
	"encoding/binary"
	"testing"
)

func TestFromBytes(t *testing.T) {
	for _, test := range []struct {
		name      string
		bts       []byte
		precision uint32
		scale     uint32
		format    string
	}{
		{
			bts:       uint128(0xffffffffffffffff, 0xffffffffffffffff),
			precision: 22,
			scale:     9,
			format:    "-0.000000001",
		},
		{
			bts:       uint128(0xffffffffffffffff, 0),
			precision: 22,
			scale:     9,
			format:    "-18446744073.709551616",
		},
		{
			bts:       uint128(0x4000000000000000, 0),
			precision: 22,
			scale:     9,
			format:    "inf",
		},
		{
			bts:       uint128(0x8000000000000000, 0),
			precision: 22,
			scale:     9,
			format:    "-inf",
		},
		{
			bts:       uint128s(1000000000),
			precision: 22,
			scale:     9,
			format:    "1.000000000",
		},
		{
			bts:       []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 250, 240, 128},
			precision: 22,
			scale:     9,
			format:    "0.050000000",
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
			formatted := Format(x, test.precision, test.scale)
			if test.format != formatted {
				t.Errorf("unexpected decimal format. Expected: %s, actual %s", test.format, formatted)
			}
			t.Logf(
				"%s %s",
				Format(x, test.precision, test.scale),
				Format(y, test.precision, test.scale),
			)
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
