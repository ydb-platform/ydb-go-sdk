package backoff

import "fmt"

// Type reports how to Backoff operation
type Type uint8

// Binary flags that used as Type
const (
	TypeNoBackoff Type = 1 << iota >> 1

	TypeFastBackoff
	TypeSlowBackoff

	TypeAny = TypeFastBackoff | TypeSlowBackoff
)

func (b Type) String() string {
	switch b {
	case TypeNoBackoff:
		return "immediately"
	case TypeFastBackoff:
		return "fast backoff"
	case TypeSlowBackoff:
		return "slow backoff"
	case TypeAny:
		return "any backoff"
	default:
		return fmt.Sprintf("unknown backoff type %d", b)
	}
}
