package spans

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func scheme(_ Adapter) (t trace.Scheme) {
	t.OnListDirectory = func(info trace.SchemeListDirectoryStartInfo) func(trace.SchemeListDirectoryDoneInfo) {
		withContextPtr(info.Context, func(c context.Context) context.Context { return withFunctionID(c, safeCall(info.Call)) })

		return nil
	}
	t.OnDescribePath = func(info trace.SchemeDescribePathStartInfo) func(trace.SchemeDescribePathDoneInfo) {
		withContextPtr(info.Context, func(c context.Context) context.Context { return withFunctionID(c, safeCall(info.Call)) })

		return nil
	}
	t.OnModifyPermissions = func(info trace.SchemeModifyPermissionsStartInfo) func(trace.SchemeModifyPermissionsDoneInfo) {
		withContextPtr(info.Context, func(c context.Context) context.Context { return withFunctionID(c, safeCall(info.Call)) })

		return nil
	}
	t.OnMakeDirectory = func(info trace.SchemeMakeDirectoryStartInfo) func(trace.SchemeMakeDirectoryDoneInfo) {
		withContextPtr(info.Context, func(c context.Context) context.Context { return withFunctionID(c, safeCall(info.Call)) })

		return nil
	}
	t.OnRemoveDirectory = func(info trace.SchemeRemoveDirectoryStartInfo) func(trace.SchemeRemoveDirectoryDoneInfo) {
		withContextPtr(info.Context, func(c context.Context) context.Context { return withFunctionID(c, safeCall(info.Call)) })

		return nil
	}

	return t
}
