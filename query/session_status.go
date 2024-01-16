package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/session"
)

type SessionStatus uint32

const (
	SessionStatusUnknown = SessionStatus(iota)
	SessionStatusReady
	SessionStatusInUse
	SessionStatusClosed
)

func (s SessionStatus) String() string {
	switch s {
	case 0:
		return session.StatusUnknown
	case 1:
		return session.StatusReady
	case 2:
		return session.StatusBusy
	case 3:
		return session.StatusClosing
	case 4:
		return session.StatusClosed
	default:
		return fmt.Sprintf("unknown_%d", s)
	}
}
