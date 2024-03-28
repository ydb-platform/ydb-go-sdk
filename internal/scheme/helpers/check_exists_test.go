package helpers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type isDirectoryExistsSchemeClient struct {
	dbName       string
	existingPath string
}

func (c isDirectoryExistsSchemeClient) Database() string {
	return c.dbName
}

func (c isDirectoryExistsSchemeClient) ListDirectory(ctx context.Context, path string) (
	d scheme.Directory, err error,
) {
	if c.existingPath == path {
		return scheme.Directory{
			Entry: scheme.Entry{
				Name: path,
				Type: scheme.EntryDirectory,
			},
		}, nil
	}
	if strings.HasPrefix(c.existingPath, path) {
		children := strings.Split(strings.TrimLeft(c.existingPath, path), "/")

		return scheme.Directory{
			Entry: scheme.Entry{
				Name: path,
				Type: scheme.EntryDirectory,
			},
			Children: []scheme.Entry{
				{
					Name: children[0],
					Type: scheme.EntryDirectory,
				},
			},
		}, nil
	}

	return d, fmt.Errorf("path '%s' not found in '%s'", path, c)
}

func TestIsDirectoryExists(t *testing.T) {
	for _, tt := range []struct {
		checkPath string
		client    isDirectoryExistsSchemeClient
		exists    bool
		err       bool
	}{
		{
			checkPath: "/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/"},
			exists:    false,
			err:       true,
		},
		{
			checkPath: "/a",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/"},
			exists:    true,
			err:       false,
		},
		{
			checkPath: "/a/b/c",
			client:    isDirectoryExistsSchemeClient{"/a/b/c", "/a/b/c/d/"},
			exists:    true,
			err:       false,
		},
		{
			checkPath: "/a/b",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/"},
			exists:    true,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/"},
			exists:    false,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/"},
			exists:    false,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/c/"},
			exists:    false,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/c/d/"},
			exists:    true,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isDirectoryExistsSchemeClient{"/a", "/a/b/c/d/e/"},
			exists:    true,
			err:       false,
		},
	} {
		t.Run("", func(t *testing.T) {
			exists, err := IsDirectoryExists(context.Background(), tt.client, tt.checkPath)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.exists, exists)
		})
	}
}

type isTableExistsSchemeClient struct {
	dbName    string
	tablePath string
}

func (c isTableExistsSchemeClient) Database() string {
	return c.dbName
}

func (c isTableExistsSchemeClient) ListDirectory(ctx context.Context, path string) (
	d scheme.Directory, err error,
) {
	if strings.HasPrefix(c.tablePath, path) {
		children := strings.Split(strings.TrimLeft(c.tablePath, path), "/")
		switch {
		case len(children) == 1:
			return scheme.Directory{
				Entry: scheme.Entry{
					Name: path,
					Type: scheme.EntryDirectory,
				},
				Children: []scheme.Entry{
					{
						Name: children[0],
						Type: scheme.EntryTable,
					},
				},
			}, nil
		case len(children) > 1:
			return scheme.Directory{
				Entry: scheme.Entry{
					Name: path,
					Type: scheme.EntryDirectory,
				},
				Children: []scheme.Entry{
					{
						Name: children[0],
						Type: scheme.EntryDirectory,
					},
				},
			}, nil
		default:
			return scheme.Directory{
				Entry: scheme.Entry{
					Name: "",
					Type: scheme.EntryDirectory,
				},
			}, nil
		}
	}

	return d, fmt.Errorf("path '%s' not found in '%s'", path, c)
}

func TestIsTableExists(t *testing.T) {
	for _, tt := range []struct {
		checkPath string
		client    isTableExistsSchemeClient
		exists    bool
		err       bool
	}{
		{
			checkPath: "/a/b/c/d",
			client:    isTableExistsSchemeClient{"/b", "/a/b"},
			exists:    false,
			err:       true,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isTableExistsSchemeClient{"/a", "/a/b"},
			exists:    false,
			err:       true,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isTableExistsSchemeClient{"/a", "/a/b/c"},
			exists:    false,
			err:       true,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isTableExistsSchemeClient{"/a", "/a/b/c/d"},
			exists:    true,
			err:       false,
		},
		{
			checkPath: "/a/b/c/d",
			client:    isTableExistsSchemeClient{"/a", "/a/b/c/d/e"},
			exists:    false,
			err:       true,
		},
	} {
		t.Run("", func(t *testing.T) {
			exists, err := IsEntryExists(context.Background(),
				tt.client, tt.checkPath,
				scheme.EntryTable, scheme.EntryColumnTable,
			)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.exists, exists)
		})
	}
}

func TestEntryTypePrintf(t *testing.T) {
	require.Equal(t,
		"[Table ColumnTable]",
		fmt.Sprintf("%v", []scheme.EntryType{scheme.EntryTable, scheme.EntryColumnTable}),
	)
}
