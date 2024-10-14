package otel

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func scheme(_ Config) (t trace.Scheme) {
	t.OnListDirectory = func(info trace.SchemeListDirectoryStartInfo) func(trace.SchemeListDirectoryDoneInfo) {
		*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

		return nil
	}
	t.OnDescribePath = func(info trace.SchemeDescribePathStartInfo) func(trace.SchemeDescribePathDoneInfo) {
		*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

		return nil
	}
	t.OnModifyPermissions = func(info trace.SchemeModifyPermissionsStartInfo) func(trace.SchemeModifyPermissionsDoneInfo) {
		*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

		return nil
	}
	t.OnMakeDirectory = func(info trace.SchemeMakeDirectoryStartInfo) func(trace.SchemeMakeDirectoryDoneInfo) {
		*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

		return nil
	}
	t.OnRemoveDirectory = func(info trace.SchemeRemoveDirectoryStartInfo) func(trace.SchemeRemoveDirectoryDoneInfo) {
		*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

		return nil
	}

	return t
}
