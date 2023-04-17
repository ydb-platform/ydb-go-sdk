package generator

import "time"

type RowID = uint64

type Row struct {
	ID               RowID
	PayloadStr       *string
	PayloadDouble    *float64
	PayloadTimestamp *time.Time
}
