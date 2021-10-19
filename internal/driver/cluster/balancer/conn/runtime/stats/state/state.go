package state

type State int8

const (
	Offline = State(iota)
	Banned
	Unknown
	Online
)

func (s State) Code() int {
	return int(s)
}

func (s State) String() string {
	switch s {
	case Online:
		return "online"
	case Offline:
		return "offline"
	case Banned:
		return "banned"
	default:
		return "unknown"
	}
}

func (s State) IsValid() bool {
	return s != Unknown
}
