package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/session"
)

type sessionStatus uint32

const (
	SessionStatusUnknown = sessionStatus(iota)
	SessionStatusReady
	SessionStatusInUse
	SessionStatusClosed
)

func (s sessionStatus) String() string {
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
