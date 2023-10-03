package conn

import "fmt"

type connError struct {
	nodeID   uint32
	endpoint string
	err      error
}

func newConnError(id uint32, endpoint string, err error) connError {
	return connError{
		nodeID:   id,
		endpoint: endpoint,
		err:      err,
	}
}

func (n connError) Error() string {
	return fmt.Sprintf("connError{node_id:%d,address:'%s'}: %v", n.nodeID, n.endpoint, n.err)
}

func (n connError) Unwrap() error {
	return n.err
}
