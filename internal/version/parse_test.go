package version

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		s   string
		v   version
		err bool
	}{
		{
			s:   "1",
			v:   version{Major: 1},
			err: false,
		},
		{
			s:   "1.2",
			v:   version{Major: 1, Minor: 2},
			err: false,
		},
		{
			s:   "1.2.3",
			v:   version{Major: 1, Minor: 2, Patch: 3},
			err: false,
		},
		{
			s:   "1.2.3-alpha",
			v:   version{Major: 1, Minor: 2, Patch: 3, Suffix: "alpha"},
			err: false,
		},
		{
			s:   "22.5",
			v:   version{Major: 22, Minor: 5},
			err: false,
		},
		{
			s:   "23.1",
			v:   version{Major: 23, Minor: 1},
			err: false,
		},
		{
			s:   "23.2",
			v:   version{Major: 23, Minor: 2},
			err: false,
		},
		{
			s:   "trunk",
			v:   version{},
			err: true,
		},
	} {
		t.Run(tt.s, func(t *testing.T) {
			v, err := parse(tt.s)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.v, v)
			}
		})
	}
}

func TestLt(t *testing.T) {
	for _, tt := range []struct {
		lhs  string
		rhs  string
		less bool
	}{
		{
			lhs:  "1",
			rhs:  "2",
			less: true,
		},
		{
			lhs:  "2",
			rhs:  "1",
			less: false,
		},
		{
			lhs:  "1",
			rhs:  "1",
			less: false,
		},
		{
			lhs:  "22.5",
			rhs:  "23.1",
			less: true,
		},
		{
			lhs:  "23.1",
			rhs:  "22.5",
			less: false,
		},
		{
			lhs:  "trunk",
			rhs:  "22.5",
			less: false,
		},
		{
			lhs:  "trunk",
			rhs:  "23.1",
			less: false,
		},
		{
			lhs:  "22.5",
			rhs:  "trunk",
			less: false,
		},
		{
			lhs:  "23.1",
			rhs:  "trunk",
			less: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.less, Lt(tt.lhs, tt.rhs))
		})
	}
}

func TestGte(t *testing.T) {
	for _, tt := range []struct {
		lhs  string
		rhs  string
		less bool
	}{
		{
			lhs:  "1",
			rhs:  "2",
			less: false,
		},
		{
			lhs:  "2",
			rhs:  "1",
			less: true,
		},
		{
			lhs:  "1",
			rhs:  "1",
			less: true,
		},
		{
			lhs:  "22.5",
			rhs:  "23.1",
			less: false,
		},
		{
			lhs:  "23.1",
			rhs:  "22.5",
			less: true,
		},
		{
			lhs:  "trunk",
			rhs:  "22.5",
			less: false,
		},
		{
			lhs:  "trunk",
			rhs:  "23.1",
			less: false,
		},
		{
			lhs:  "22.5",
			rhs:  "trunk",
			less: false,
		},
		{
			lhs:  "23.1",
			rhs:  "trunk",
			less: false,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.less, Gte(tt.lhs, tt.rhs))
		})
	}
}
