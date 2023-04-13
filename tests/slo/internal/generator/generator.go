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

type Generator struct {
	currentID RowID
	mu        sync.Mutex
}

func New(id RowID) *Generator {
	return &Generator{
		currentID: id,
	}
}

func (g *Generator) Generate() (Row, error) {
	g.mu.Lock()
	id := g.currentID
	g.currentID++
	g.mu.Unlock()
	e := Row{
		ID:               id,
		PayloadDouble:    func(a float64) *float64 { return &a }(rand.Float64()), //nolint:gosec // speed more important
		PayloadTimestamp: func(a uint64) *uint64 { return &a }(uint64(time.Now().UnixMicro())),
	}

	var err error
	e.PayloadStr, err = g.genPayloadString()
	if err != nil {
		return Row{}, err
	}

	return e, nil
}

func (g *Generator) genPayloadString() (*string, error) {
	l := MinLength + rand.Intn(MaxLength-MinLength+1) //nolint:gosec // speed more important

	sl := make([]byte, l)

	_, err := crypto.Read(sl)
	if err != nil {
		return nil, err
	}

	s := base64.StdEncoding.EncodeToString(sl)

	return &s, nil
}
