package session

import (
	"fmt"
)

type Status uint32

const (
	statusUnknown = Status(iota)
	StatusIdle
	StatusInUse
	StatusClosing
	StatusClosed
)

func (s Status) String() string {
	switch s {
	case statusUnknown:
		return "Unknown"
	case StatusIdle:
		return "Idle"
	case StatusInUse:
		return "InUse"
	case StatusClosing:
		return "Closing"
	case StatusClosed:
		return "Closed"
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}
