package generator

import (
	"crypto/rand"
	"encoding/base64"
	"math/big"

	"github.com/google/uuid"
)

const (
	MinLength = 20
	MaxLength = 40
)

type Generator struct{}

func New() Generator {
	return Generator{}
}

func (g Generator) Generate() (Entry, error) {
	e := Entry{
		ID: uuid.New(),
	}

	var err error
	e.Payload, err = g.genPayload()
	if err != nil {
		return Entry{}, err
	}

	return e, nil
}

func (g Generator) genPayload() (string, error) {
	l, err := rand.Int(rand.Reader, big.NewInt(MaxLength-MinLength+1))
	if err != nil {
		return "", err
	}

	sl := make([]byte, MinLength+l.Int64())

	_, err = rand.Read(sl)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(sl), nil
}
