package conn

import "google.golang.org/grpc/connectivity"

type State = connectivity.State

func Ready(s State) bool {
	switch s {
	case connectivity.Idle, connectivity.Ready:
		return true
	default:
		return false
	}
}
