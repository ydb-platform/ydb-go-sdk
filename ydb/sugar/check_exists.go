package sugar

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme/helpers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

func IsTableExists(ctx context.Context, c scheme.Client, absTablePath string) (exists bool, _ error) {
	exists, err := helpers.IsTableExists(ctx, c, absTablePath)
	if err != nil {
		return exists, xerrors.WithStackTrace(err)
	}
	return exists, nil
}

func IsDirectoryExists(ctx context.Context, c scheme.Client, absTablePath string) (exists bool, _ error) {
	exists, err := helpers.IsDirectoryExists(ctx, c, absTablePath)
	if err != nil {
		return exists, xerrors.WithStackTrace(err)
	}
	return exists, nil
}
