package bind

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_removeComments(t *testing.T) {
	for _, tt := range []struct {
		src string
		dst string
	}{
		{
			src: `
-- some comment
SELECT 1;`,
			dst: `

SELECT 1;`,
		},
		{
			src: `SELECT 1; -- some comment`,
			dst: `SELECT 1; `,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.dst, removeComments(tt.src))
		})
	}
}

func Test_removeEmptyLines(t *testing.T) {
	for _, tt := range []struct {
		src string
		dst string
	}{
		{
			src: `

  
test
 

`,
			dst: `test`,
		},
		{
			src: `

  
   test
 

`,
			dst: `   test`,
		},
		{
			src: `

  
   test
 

end`,
			dst: `   test
end`,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.dst, removeEmptyLines(tt.src))
		})
	}
}
