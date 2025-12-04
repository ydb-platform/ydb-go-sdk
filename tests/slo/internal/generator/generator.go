package generator

import (
	crypto "crypto/rand"
	"encoding/base64"
	"math/rand"
	"sync"
	"time"
)

const (
	MinLength = 20
	MaxLength = 40
)

type Generator interface {
	Generate() (Row, error)
}

type GeneratorImpl struct {
	currentID RowID
	mu        sync.Mutex
}

func New(id RowID) *GeneratorImpl {
	return &GeneratorImpl{
		currentID: id,
	}
}

func (g *GeneratorImpl) Generate() (Row, error) {
	g.mu.Lock()
	id := g.currentID
	g.currentID++
	g.mu.Unlock()
	e := Row{
		ID:               id,
		PayloadDouble:    func(a float64) *float64 { return &a }(rand.Float64()), //nolint:gosec // speed more important
		PayloadTimestamp: func(a time.Time) *time.Time { return &a }(time.Now()),
		PayloadHash:      func(a uint64) *uint64 { return &a }(rand.Uint64()), //nolint:gosec
	}

	var err error
	e.PayloadStr, err = genPayloadString()
	if err != nil {
		return Row{}, err
	}

	return e, nil
}

func genPayloadString() (*string, error) {
	l := MinLength + rand.Intn(MaxLength-MinLength+1) //nolint:gosec // speed more important

	sl := make([]byte, l)

	if _, err := crypto.Read(sl); err != nil {
		return nil, err
	}

	s := base64.StdEncoding.EncodeToString(sl)

	return &s, nil
}
