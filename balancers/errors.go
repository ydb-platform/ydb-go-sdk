package balancers

import "fmt"

type balancerTypeError struct {
	message string
	bType   balancerType
}

func (e *balancerTypeError) Error() string {
	return fmt.Sprintf(e.message, e.bType)
}
