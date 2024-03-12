package session

type Status = string

const (
	StatusUnknown = Status("unknown")
	StatusReady   = Status("ready")
	StatusBusy    = Status("busy")
	StatusClosing = Status("closing")
	StatusClosed  = Status("closed")
)
