package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Scheme specified trace of scheme client activity.
	// gtrace:gen
	Scheme struct {
		OnListDirectory     func(SchemeListDirectoryStartInfo) func(SchemeListDirectoryDoneInfo)
		OnDescribePath      func(SchemeDescribePathStartInfo) func(SchemeDescribePathDoneInfo)
		OnMakeDirectory     func(SchemeMakeDirectoryStartInfo) func(SchemeMakeDirectoryDoneInfo)
		OnRemoveDirectory   func(SchemeRemoveDirectoryStartInfo) func(SchemeRemoveDirectoryDoneInfo)
		OnModifyPermissions func(SchemeModifyPermissionsStartInfo) func(SchemeModifyPermissionsDoneInfo)
	}

	SchemeListDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	SchemeListDirectoryDoneInfo struct {
		Error error
	}
	SchemeDescribePathStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Path    string
	}
	SchemeDescribePathDoneInfo struct {
		EntryType string
		Error     error
	}
	SchemeMakeDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Path    string
	}
	SchemeMakeDirectoryDoneInfo struct {
		Error error
	}
	SchemeRemoveDirectoryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Path    string
	}
	SchemeRemoveDirectoryDoneInfo struct {
		Error error
	}
	SchemeModifyPermissionsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Path    string
	}
	SchemeModifyPermissionsDoneInfo struct {
		Error error
	}
)
