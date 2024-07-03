package xsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	var m Map[int, string]
	v, ok := m.Load(1)
	require.False(t, ok)
	m.Store(1, "one")
	v, ok = m.Load(1)
	require.True(t, ok)
	require.Equal(t, "one", v)
	m.Store(2, "two")
	v, ok = m.Load(2)
	require.True(t, ok)
	require.Equal(t, "two", v)
	m.Delete(1)
	v, ok = m.Load(1)
	require.False(t, ok)
	m.Store(3, "three")
	v, ok = m.Load(3)
	require.True(t, ok)
	require.Equal(t, "three", v)
	exp := map[int]string{
		2: "two",
		3: "three",
	}
	var unexp map[int]string
	m.Range(func(key int, value string) bool {
		if v, ok := exp[key]; ok && v == value {
			delete(exp, key)
		} else {
			unexp[key] = value
		}
		return true
	})
	require.Empty(t, exp)
	require.Empty(t, unexp)
}
