package generator

import (
	"math/rand"
	"time"
)

type Range struct {
	Left  uint64
	Right uint64
}
type SeededGenerator struct {
	rng      *rand.Rand
	setRange *Range
}

func NewSeeded(seed int64) *SeededGenerator {
	return &SeededGenerator{
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (g *SeededGenerator) ConstructRow() (Row, error) {
	e := Row{
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

func (g *SeededGenerator) Generate() (Row, error) {
	row, err := g.ConstructRow()
	if err != nil {
		return Row{}, err
	}
	if g.setRange == nil {
		row.ID = g.rng.Uint64()
	} else {
		row.ID = g.setRange.Left + g.rng.Uint64()%(g.setRange.Right-g.setRange.Left)
	}

	return row, nil
}

func (g *SeededGenerator) SetRange(l uint64, r uint64) {
	g.setRange = &Range{
		Left:  l,
		Right: r,
	}
}
