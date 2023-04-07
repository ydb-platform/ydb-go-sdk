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
	currentID EntryID
	mu        sync.Mutex
}

func New(id EntryID) Generator {
	return Generator{
		currentID: id,
	}
}

func (g *Generator) Generate() (Entry, error) {
	g.mu.Lock()
	g.currentID++
	id := g.currentID
	g.mu.Unlock()

	e := Entry{
		ID:               id,
		PayloadDouble:    rand.Float64(),
		PayloadTimestamp: uint64(time.Now().UnixMicro()),
	}

	var err error
	e.PayloadStr, err = g.genPayloadString()
	if err != nil {
		return Entry{}, err
	}

	return e, nil
}

func (g *Generator) genPayloadString() (string, error) {
	l := MinLength + rand.Intn(MaxLength-MinLength+1)

	sl := make([]byte, l)

	_, err := crypto.Read(sl)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(sl), nil
}
