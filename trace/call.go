package trace

import "fmt"

// Call contains Call metadata for trace hooks.
type Call interface {
	fmt.Stringer
}
