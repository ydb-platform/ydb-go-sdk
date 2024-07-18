package query

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/session"
)

type statusCode uint32

const (
	statusUnknown = statusCode(iota)
	statusIdle
	statusInUse
	statusClosing
	statusClosed
)

func (s statusCode) String() string {
	switch s {
	case statusUnknown:
		return session.StatusUnknown
	case statusIdle:
		return session.StatusIdle
	case statusInUse:
		return session.StatusInUse
	case statusClosing:
		return session.StatusClosing
	case statusClosed:
		return session.StatusClosed
	default:
		return fmt.Sprintf("Unknown%d", s)
	}
}
