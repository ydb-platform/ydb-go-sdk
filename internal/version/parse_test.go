package version

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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

var cases = []struct {
	name string
	lhs  string
	rhs  string
	lt   bool
	gte  bool
}{
	{
		name: xtest.CurrentFileLine(),
		lhs:  "1",
		rhs:  "2",
		lt:   true,
		gte:  false,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "2",
		rhs:  "1",
		lt:   false,
		gte:  true,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "1",
		rhs:  "1",
		lt:   false,
		gte:  true,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "22.5",
		rhs:  "23.1",
		lt:   true,
		gte:  false,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "23.1",
		rhs:  "22.5",
		lt:   false,
		gte:  true,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "trunk",
		rhs:  "22.5",
		lt:   false,
		gte:  false,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "trunk",
		rhs:  "23.1",
		lt:   false,
		gte:  false,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "22.5",
		rhs:  "trunk",
		lt:   false,
		gte:  false,
	},
	{
		name: xtest.CurrentFileLine(),
		lhs:  "23.1",
		rhs:  "trunk",
		lt:   false,
		gte:  false,
	},
}

func TestLt(t *testing.T) {
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.lt, Lt(tt.lhs, tt.rhs))
		})
	}
}

func TestGte(t *testing.T) {
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.gte, Gte(tt.lhs, tt.rhs))
		})
	}
}
