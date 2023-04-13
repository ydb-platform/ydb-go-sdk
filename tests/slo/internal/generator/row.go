package generator

type RowID = uint64

type Row struct {
	ID               RowID
	PayloadStr       *string
	PayloadDouble    *float64
	PayloadTimestamp *uint64
}
