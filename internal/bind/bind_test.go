package bind

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	for _, tt := range []struct {
		bindings []Bind
		sorted   []Bind
	}{
		{
			bindings: []Bind{
				NumericArgs{},
				AutoDeclare{},
				TablePathPrefix(""),
			},
			sorted: []Bind{
				TablePathPrefix(""),
				AutoDeclare{},
				NumericArgs{},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.sorted, Sort(tt.bindings))
		})
	}
}
