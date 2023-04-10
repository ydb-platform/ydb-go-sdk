package generator

type EntryID = uint64

type Entry struct {
	ID               EntryID
	PayloadStr       *string
	PayloadDouble    *float64
	PayloadTimestamp *uint64
}

type Entries = map[EntryID]Entry
