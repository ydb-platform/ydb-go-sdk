package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type poolMock struct {
	items []any
}

func (p *poolMock) Get() (v any) {
	if len(p.items) == 0 {
		return nil
	}

	v, p.items = p.items[0], p.items[1:]

	return v
}

func (p *poolMock) Put(v any) {
	p.items = append(p.items, v)
}

func TestPool(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		p := Pool[int]{
			p: &poolMock{},
		}
		v := p.GetOrNil()
		require.Nil(t, v)
		v = p.GetOrNew()
		require.NotNil(t, v)
		require.Equal(t, 0, *v)
		*v = 123
		p.Put(v)
		v = p.GetOrNew()
		require.NotNil(t, v)
		require.Equal(t, 123, *v)
		v = p.GetOrNil()
		require.Nil(t, v)
	})
	t.Run("New", func(t *testing.T) {
		p := Pool[int]{
			p: &poolMock{},
			New: func() *int {
				v := 123

				return &v
			},
		}
		v := p.GetOrNil()
		require.Nil(t, v)
		v = p.GetOrNew()
		require.NotNil(t, v)
		require.Equal(t, 123, *v)
		*v = 456
		p.Put(v)
		v = p.GetOrNew()
		require.NotNil(t, v)
		require.Equal(t, 456, *v)
		v = p.GetOrNil()
		require.Nil(t, v)
		v = p.GetOrNew()
		require.NotNil(t, v)
		require.Equal(t, 123, *v)
	})
}
