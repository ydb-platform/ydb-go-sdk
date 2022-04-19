package operation

import "fmt"

// Status reports which status of operation: completed or none
type Status uint8

// Binary flags that used as Status
const (
	Finished  = Status(1 << iota >> 1)
	Undefined // may be true or may be false
	NotFinished
)

func (t Status) String() string {
	switch t {
	case Finished:
		return "operation was completed, no need retries"
	case NotFinished:
		return "operation was not completed, need retries"
	case Undefined:
		return "operation completed status undefined, no need retries"
	default:
		return fmt.Sprintf("unknown operation completed code: %d", t)
	}
}
