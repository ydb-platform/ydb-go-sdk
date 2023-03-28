package generator

import (
	"math/rand"

	"github.com/google/uuid"
)

var (
	allowedCharacters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*(){}")
)

type Generator struct {
	MinLength int
	MaxLength int
}

func NewGenerator(min, max int) Generator {
	return Generator{
		MinLength: min,
		MaxLength: max,
	}
}

func (gen Generator) Generate() (Entry, error) {
	return Entry{
		ID:      uuid.New(),
		Payload: gen.getRandomString(),
	}, nil
}

func (gen Generator) getRandomString() string {
	l := gen.MinLength + rand.Intn(gen.MaxLength-gen.MinLength+1)

	sl := make([]rune, 0, l)

	var n int
	for i := 0; i < l; i++ {
		n = rand.Intn(len(allowedCharacters))
		sl = append(sl, allowedCharacters[n])
	}

	return string(sl)
}
