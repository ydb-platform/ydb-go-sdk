package trace

import (
	"context"
)

type (
	// Scheme specified trace of scheme client activity.
	// gtrace:gen
	// gtrace:out internal/scheme/gtrace
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	Scheme struct {
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnListDirectory func(SchemeListDirectoryStartInfo) func(SchemeListDirectoryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDescribePath func(SchemeDescribePathStartInfo) func(SchemeDescribePathDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnMakeDirectory func(SchemeMakeDirectoryStartInfo) func(SchemeMakeDirectoryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnRemoveDirectory func(SchemeRemoveDirectoryStartInfo) func(SchemeRemoveDirectoryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnModifyPermissions func(SchemeModifyPermissionsStartInfo) func(SchemeModifyPermissionsDoneInfo)
	}

	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeListDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeListDirectoryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeDescribePathStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Path    string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeDescribePathDoneInfo struct {
		EntryType string
		Error     error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeMakeDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Path    string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeMakeDirectoryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeRemoveDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Path    string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeRemoveDirectoryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeModifyPermissionsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Path    string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	SchemeModifyPermissionsDoneInfo struct {
		Error error
	}
)
