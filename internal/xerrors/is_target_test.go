package xerrors

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorWithCastToTargetError(t *testing.T) {
	err := IsTarget(errors.New("test"), io.EOF)
	require.Equal(t, "test", err.Error())
	require.ErrorIs(t, err, io.EOF)
}
