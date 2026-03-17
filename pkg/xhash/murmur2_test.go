package xhash

import (
	"testing"
)

// TestMurmur2Hash64A_StringAndBytes_Consistency verifies that hashing a string
// and the same data as a byte slice produce identical results for a set of inputs and seeds.
func TestMurmur2Hash64A_StringAndBytes_Consistency(t *testing.T) {
	tests := []struct {
		name string
		s    string
	}{
		{name: "empty", s: ""},
		{name: "one_byte", s: "a"},
		{name: "short_ascii", s: "hello"},
		{name: "longer_ascii", s: "hello world, murmur2 hash"},
		{name: "non_ascii", s: "мурмур2-хэш"},
	}

	seeds := []uint64{
		0,
		1,
		0x12345678,
		0xdeadbeefcafebabe,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, seed := range seeds {
				hFromBytes := Murmur2Hash64A([]byte(tt.s), seed)
				hFromString := Murmur2HashString64(tt.s, seed)
				if hFromBytes != hFromString {
					t.Fatalf("mismatch for %q, seed=%#x: bytes=%#x string=%#x", tt.s, seed, hFromBytes, hFromString)
				}
			}
		})
	}
}

// TestMurmur2Hash32_StringAndBytes_Consistency verifies that hashing a string
// and the same data as a byte slice produce identical results for a set of inputs and seeds.
func TestMurmur2Hash32_StringAndBytes_Consistency(t *testing.T) {
	tests := []struct {
		name string
		s    string
	}{
		{name: "empty", s: ""},
		{name: "one_byte", s: "a"},
		{name: "short_ascii", s: "hello"},
		{name: "longer_ascii", s: "hello world, murmur2 hash"},
		{name: "non_ascii", s: "мурмур2-хэш"},
	}

	seeds := []uint32{
		0,
		1,
		0x12345678,
		0xdeadbeef,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, seed := range seeds {
				hFromBytes := Murmur2Hash32([]byte(tt.s), seed)
				hFromString := Murmur2HashString32(tt.s, seed)
				if hFromBytes != hFromString {
					t.Fatalf("mismatch for %q, seed=%#x: bytes=%#x string=%#x", tt.s, seed, hFromBytes, hFromString)
				}
			}
		})
	}
}

// TestMurmur2Hash_Deterministic ensures the hash functions are deterministic:
// the same input and seed always produce the same result.
func TestMurmur2Hash_Deterministic(t *testing.T) {
	inputs := [][]byte{
		{},
		[]byte("a"),
		[]byte("hello"),
		[]byte("hello world, murmur2 hash"),
	}

	seeds64 := []uint64{0, 1, 0x12345678abcdef01}
	seeds32 := []uint32{0, 1, 0x12345678}

	for _, data := range inputs {
		d := append([]byte(nil), data...)

		for _, seed := range seeds64 {
			h1 := Murmur2Hash64A(d, seed)
			h2 := Murmur2Hash64A(d, seed)
			if h1 != h2 {
				t.Fatalf("Murmur2Hash64A not deterministic for %q, seed=%#x: %v vs %v", string(d), seed, h1, h2)
			}
		}

		for _, seed := range seeds32 {
			h1 := Murmur2Hash32(d, seed)
			h2 := Murmur2Hash32(d, seed)
			if h1 != h2 {
				t.Fatalf("Murmur2Hash32 not deterministic for %q, seed=%#x: %v vs %v", string(d), seed, h1, h2)
			}
		}
	}
}

// TestMurmur2Hash_ChangesWithInput ensures that small changes in the input
// produce different hash values for the tested cases.
func TestMurmur2Hash_ChangesWithInput(t *testing.T) {
	const seed64 = uint64(0x12345678abcdef01)
	const seed32 = uint32(0x12345678)

	input1 := []byte("hello world")
	input2 := []byte("hello worle") // last character differs

	h64_1 := Murmur2Hash64A(input1, seed64)
	h64_2 := Murmur2Hash64A(input2, seed64)
	if h64_1 == h64_2 {
		t.Fatalf("Murmur2Hash64A produced equal hashes for different inputs: %v and %v", h64_1, h64_2)
	}

	h32_1 := Murmur2Hash32(input1, seed32)
	h32_2 := Murmur2Hash32(input2, seed32)
	if h32_1 == h32_2 {
		t.Fatalf("Murmur2Hash32 produced equal hashes for different inputs: %v and %v", h32_1, h32_2)
	}
}
