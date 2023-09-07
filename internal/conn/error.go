package conn

import "fmt"

type nodeError struct {
	id       uint32
	endpoint string
	err      error
}

func newNodeError(id uint32, endpoint string, err error) nodeError {
	return nodeError{
		id:       id,
		endpoint: endpoint,
		err:      err,
	}
}

func (n nodeError) Error() string {
	return fmt.Sprintf("on node %v (%v): %v", n.id, n.endpoint, n.err)
}

func (n nodeError) Unwrap() error {
	return n.err
}
