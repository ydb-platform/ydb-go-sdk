package rand

import (
	"crypto/rand"
	"math/big"
)

func int64n(max int64) int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(err) // err on negative max
	}
	return n.Int64()
}

func Int64(max int64) int64 {
	return int64n(max)
}

func Int(max int) int {
	return int(int64n(int64(max)))
}
