package tracefuzz

import (
	"encoding/binary"
)

// Fuzzer consumes fuzz input bytes to produce pseudo-random values.
type Fuzzer struct {
	data []byte
	pos  int
}

// New returns a Fuzzer over data. Empty data is padded internally.
func New(data []byte) *Fuzzer {
	if len(data) == 0 {
		data = []byte{0}
	}

	return &Fuzzer{data: data}
}

func (f *Fuzzer) byte() byte {
	if len(f.data) == 0 {
		return 0
	}
	b := f.data[f.pos%len(f.data)]
	f.pos++

	return b
}

func (f *Fuzzer) uint64() uint64 {
	var buf [8]byte
	for i := range buf {
		buf[i] = f.byte()
	}

	return binary.LittleEndian.Uint64(buf[:])
}

// Choice returns a value in [0, n).
func (f *Fuzzer) Choice(n int) int {
	if n <= 0 {
		return 0
	}

	return int(f.uint64() % uint64(n))
}

func (f *Fuzzer) Bool() bool {
	return f.byte()%2 == 0
}

func (f *Fuzzer) Int() int {
	// Keep values non-negative; width is still reduced in Fill() per destination type.
	return int(f.uint64() >> 1)
}

func (f *Fuzzer) Intn(n int) int {
	if n <= 0 {
		return 0
	}

	return int(f.uint64() % uint64(n))
}

func (f *Fuzzer) String() string {
	n := f.Intn(64) + 1
	b := make([]byte, n)
	for i := range b {
		b[i] = f.byte()
	}

	return string(b)
}

func (f *Fuzzer) Bytes() []byte {
	n := f.Intn(32)
	b := make([]byte, n)
	for i := range b {
		b[i] = f.byte()
	}

	return b
}
