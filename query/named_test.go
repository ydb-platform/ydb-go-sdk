package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamed(t *testing.T) {
	for _, tt := range []struct {
		name  string
		ref   interface{}
		dst   namedDestination
		panic bool
	}{
		{
			name:  "",
			ref:   nil,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name:  "nil_ref",
			ref:   nil,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name:  "not_ref",
			ref:   123,
			dst:   namedDestination{},
			panic: true,
		},
		{
			name: "int_ptr",
			ref:  func(v int) *int { return &v }(123),
			dst: namedDestination{
				name: "int_ptr",
				ref:  func(v int) *int { return &v }(123),
			},
			panic: false,
		},
		{
			name: "int_dbl_ptr",
			ref: func(v int) **int {
				vv := &v
				return &vv
			}(123),
			dst: namedDestination{
				name: "int_dbl_ptr",
				ref: func(v int) **int {
					vv := &v
					return &vv
				}(123),
			},
			panic: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panic {
				defer func() {
					require.NotNil(t, recover())
				}()
			} else {
				defer func() {
					require.Nil(t, recover())
				}()
			}
			require.Equal(t, tt.dst, Named(tt.name, tt.ref))
		})
	}
}
