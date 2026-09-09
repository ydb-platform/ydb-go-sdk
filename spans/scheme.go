package spans

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func scheme(_ Adapter) (t trace.Scheme) {
	t.OnListDirectory = func(info trace.SchemeListDirectoryStartInfo) func(trace.SchemeListDirectoryDoneInfo) {
		withCallContext(info.Context, info.Call)

		return nil
	}
	t.OnDescribePath = func(info trace.SchemeDescribePathStartInfo) func(trace.SchemeDescribePathDoneInfo) {
		withCallContext(info.Context, info.Call)

		return nil
	}
	t.OnModifyPermissions = func(info trace.SchemeModifyPermissionsStartInfo) func(trace.SchemeModifyPermissionsDoneInfo) {
		withCallContext(info.Context, info.Call)

		return nil
	}
	t.OnMakeDirectory = func(info trace.SchemeMakeDirectoryStartInfo) func(trace.SchemeMakeDirectoryDoneInfo) {
		withCallContext(info.Context, info.Call)

		return nil
	}
	t.OnRemoveDirectory = func(info trace.SchemeRemoveDirectoryStartInfo) func(trace.SchemeRemoveDirectoryDoneInfo) {
		withCallContext(info.Context, info.Call)

		return nil
	}

	return t
}
