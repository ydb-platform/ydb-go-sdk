package session

type Status = string

const (
	StatusUnknown = Status("Unknown")
	StatusIdle    = Status("Idle")
	StatusInUse   = Status("InUse")
	StatusClosing = Status("Closing")
	StatusClosed  = Status("Closed")
	StatusError   = Status("Error")
)
