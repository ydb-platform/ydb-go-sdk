package xhash

import "encoding/binary"

// Murmur2Hash64A implements Murmur2 hash 64-bit for 64-bit platforms.
// This is the "MurmurHash64A" variant (little-endian).
func Murmur2Hash64A(data []byte, seed uint64) uint64 {
	const (
		m uint64 = 0xc6a4a7935bd1e995
		r uint   = 47
	)

	n := len(data)
	h := seed ^ (uint64(n) * m)

	// body: process 8-byte blocks
	i := 0
	for ; i+8 <= n; i += 8 {
		k := binary.LittleEndian.Uint64(data[i:])

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	// tail
	switch n - i {
	case 7:
		h ^= uint64(data[i+6]) << 48

		fallthrough
	case 6:
		h ^= uint64(data[i+5]) << 40

		fallthrough
	case 5:
		h ^= uint64(data[i+4]) << 32

		fallthrough
	case 4:
		h ^= uint64(data[i+3]) << 24

		fallthrough
	case 3:
		h ^= uint64(data[i+2]) << 16

		fallthrough
	case 2:
		h ^= uint64(data[i+1]) << 8

		fallthrough
	case 1:
		h ^= uint64(data[i])
		h *= m
	}

	// finalization
	h ^= h >> r
	h *= m
	h ^= h >> r

	return h
}

// Murmur2HashString64 hashes a string to uint64 using MurmurHash64A with a chosen seed.
func Murmur2HashString64(s string, seed uint64) uint64 {
	return Murmur2Hash64A([]byte(s), seed)
}

// Murmur2Hash32 returns Murmur2 hash 32-bit (little-endian), compatible with the
// original MurmurHash2 reference implementation.
func Murmur2Hash32(data []byte, seed uint32) uint32 {
	const m uint32 = 0x5bd1e995
	const r uint32 = 24

	n := len(data)
	h := seed ^ uint32(n)

	// body: 4-byte blocks
	i := 0
	for ; i+4 <= n; i += 4 {
		k := binary.LittleEndian.Uint32(data[i:])

		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}

	// tail
	switch n - i {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}

	// finalization
	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}

// Murmur2HashString32 hashes a string to uint32 using MurmurHash2 with a chosen seed.
func Murmur2HashString32(s string, seed uint32) uint32 {
	return Murmur2Hash32([]byte(s), seed)
}
