package safe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type sessionInfoItem struct {
	id string
}

func (s *sessionInfoItem) ID() string                  { return s.id }
func (s *sessionInfoItem) NodeID() uint32              { return 1 }
func (s *sessionInfoItem) Status() string              { return "ready" }
func (s *sessionInfoItem) Close(context.Context) error { return nil }
func (s *sessionInfoItem) IsAlive() bool               { return true }

func TestSessionInfo(t *testing.T) {
	t.Parallel()

	t.Run("NilPointer", func(t *testing.T) {
		t.Parallel()

		var item *sessionInfoItem

		session := SessionInfo(item)
		require.Nil(t, session)
	})
	t.Run("TypedNilIsUntypedNil", func(t *testing.T) {
		t.Parallel()

		session := SessionInfo((*sessionInfoItem)(nil))

		var untyped trace.SessionInfo
		require.Equal(t, untyped, session)
	})
	t.Run("NonNil", func(t *testing.T) {
		t.Parallel()

		item := &sessionInfoItem{id: "test"}
		session := SessionInfo(item)

		require.NotNil(t, session)
		require.Equal(t, "test", session.ID())
	})
}
