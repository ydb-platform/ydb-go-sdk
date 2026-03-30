package meta

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

func TestVersionHeader(t *testing.T) {
	for i, tt := range []struct {
		buildInfo     *xsync.Map[string, string]
		versionHeader string
	}{
		{
			buildInfo: func() *xsync.Map[string, string] {
				return &xsync.Map[string, string]{}
			}(),
			versionHeader: version.FullVersion,
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("testFramework", "0.0.0")

				return m
			}(),
			versionHeader: version.FullVersion + ";testFramework/0.0.0",
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("testFramework1", "0.0.1")
				m.Set("testFramework2", "0.0.2")

				return m
			}(),
			versionHeader: version.FullVersion + ";testFramework1/0.0.1;testFramework2/0.0.2",
		},
		{
			buildInfo: func() *xsync.Map[string, string] {
				m := &xsync.Map[string, string]{}
				m.Set("1testFramework", "0.0.1")
				m.Set("2testFramework", "0.0.2")

				return m
			}(),
			versionHeader: version.FullVersion + ";1testFramework/0.0.1;2testFramework/0.0.2",
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			require.Equal(t, tt.versionHeader, versionHeader(tt.buildInfo))
		})
	}
}
