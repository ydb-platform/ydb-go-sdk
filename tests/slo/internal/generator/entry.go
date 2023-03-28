package generator

import "github.com/google/uuid"

type EntryID = uuid.UUID

type Entry struct {
	ID      EntryID
	Payload string
}

type Entries = map[EntryID]Entry
