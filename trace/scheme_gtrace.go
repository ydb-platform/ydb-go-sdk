// Code generated by gtrace. DO NOT EDIT.

package trace

import (
	"context"
)

// schemeComposeOptions is a holder of options
type schemeComposeOptions struct {
	panicCallback func(e interface{})
}

// SchemeOption specified Scheme compose option
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
type SchemeComposeOption func(o *schemeComposeOptions)

// WithSchemePanicCallback specified behavior on panic
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func WithSchemePanicCallback(cb func(e interface{})) SchemeComposeOption {
	return func(o *schemeComposeOptions) {
		o.panicCallback = cb
	}
}

// Compose returns a new Scheme which has functional fields composed both from t and x.
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func (t *Scheme) Compose(x *Scheme, opts ...SchemeComposeOption) *Scheme {
	var ret Scheme
	options := schemeComposeOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	{
		h1 := t.OnListDirectory
		h2 := x.OnListDirectory
		ret.OnListDirectory = func(s SchemeListDirectoryStartInfo) func(SchemeListDirectoryDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(SchemeListDirectoryDoneInfo)
			if h1 != nil {
				r = h1(s)
			}
			if h2 != nil {
				r1 = h2(s)
			}
			return func(s SchemeListDirectoryDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(s)
				}
				if r1 != nil {
					r1(s)
				}
			}
		}
	}
	{
		h1 := t.OnDescribePath
		h2 := x.OnDescribePath
		ret.OnDescribePath = func(s SchemeDescribePathStartInfo) func(SchemeDescribePathDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(SchemeDescribePathDoneInfo)
			if h1 != nil {
				r = h1(s)
			}
			if h2 != nil {
				r1 = h2(s)
			}
			return func(s SchemeDescribePathDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(s)
				}
				if r1 != nil {
					r1(s)
				}
			}
		}
	}
	{
		h1 := t.OnMakeDirectory
		h2 := x.OnMakeDirectory
		ret.OnMakeDirectory = func(s SchemeMakeDirectoryStartInfo) func(SchemeMakeDirectoryDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(SchemeMakeDirectoryDoneInfo)
			if h1 != nil {
				r = h1(s)
			}
			if h2 != nil {
				r1 = h2(s)
			}
			return func(s SchemeMakeDirectoryDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(s)
				}
				if r1 != nil {
					r1(s)
				}
			}
		}
	}
	{
		h1 := t.OnRemoveDirectory
		h2 := x.OnRemoveDirectory
		ret.OnRemoveDirectory = func(s SchemeRemoveDirectoryStartInfo) func(SchemeRemoveDirectoryDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(SchemeRemoveDirectoryDoneInfo)
			if h1 != nil {
				r = h1(s)
			}
			if h2 != nil {
				r1 = h2(s)
			}
			return func(s SchemeRemoveDirectoryDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(s)
				}
				if r1 != nil {
					r1(s)
				}
			}
		}
	}
	{
		h1 := t.OnModifyPermissions
		h2 := x.OnModifyPermissions
		ret.OnModifyPermissions = func(s SchemeModifyPermissionsStartInfo) func(SchemeModifyPermissionsDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(SchemeModifyPermissionsDoneInfo)
			if h1 != nil {
				r = h1(s)
			}
			if h2 != nil {
				r1 = h2(s)
			}
			return func(s SchemeModifyPermissionsDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(s)
				}
				if r1 != nil {
					r1(s)
				}
			}
		}
	}
	return &ret
}
func (t *Scheme) onListDirectory(s SchemeListDirectoryStartInfo) func(SchemeListDirectoryDoneInfo) {
	fn := t.OnListDirectory
	if fn == nil {
		return func(SchemeListDirectoryDoneInfo) {
			return
		}
	}
	res := fn(s)
	if res == nil {
		return func(SchemeListDirectoryDoneInfo) {
			return
		}
	}
	return res
}
func (t *Scheme) onDescribePath(s SchemeDescribePathStartInfo) func(SchemeDescribePathDoneInfo) {
	fn := t.OnDescribePath
	if fn == nil {
		return func(SchemeDescribePathDoneInfo) {
			return
		}
	}
	res := fn(s)
	if res == nil {
		return func(SchemeDescribePathDoneInfo) {
			return
		}
	}
	return res
}
func (t *Scheme) onMakeDirectory(s SchemeMakeDirectoryStartInfo) func(SchemeMakeDirectoryDoneInfo) {
	fn := t.OnMakeDirectory
	if fn == nil {
		return func(SchemeMakeDirectoryDoneInfo) {
			return
		}
	}
	res := fn(s)
	if res == nil {
		return func(SchemeMakeDirectoryDoneInfo) {
			return
		}
	}
	return res
}
func (t *Scheme) onRemoveDirectory(s SchemeRemoveDirectoryStartInfo) func(SchemeRemoveDirectoryDoneInfo) {
	fn := t.OnRemoveDirectory
	if fn == nil {
		return func(SchemeRemoveDirectoryDoneInfo) {
			return
		}
	}
	res := fn(s)
	if res == nil {
		return func(SchemeRemoveDirectoryDoneInfo) {
			return
		}
	}
	return res
}
func (t *Scheme) onModifyPermissions(s SchemeModifyPermissionsStartInfo) func(SchemeModifyPermissionsDoneInfo) {
	fn := t.OnModifyPermissions
	if fn == nil {
		return func(SchemeModifyPermissionsDoneInfo) {
			return
		}
	}
	res := fn(s)
	if res == nil {
		return func(SchemeModifyPermissionsDoneInfo) {
			return
		}
	}
	return res
}
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func SchemeOnListDirectory(t *Scheme, c *context.Context, call call) func(error) {
	var p SchemeListDirectoryStartInfo
	p.Context = c
	p.Call = call
	res := t.onListDirectory(p)
	return func(e error) {
		var p SchemeListDirectoryDoneInfo
		p.Error = e
		res(p)
	}
}
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func SchemeOnDescribePath(t *Scheme, c *context.Context, call call, path string) func(entryType string, _ error) {
	var p SchemeDescribePathStartInfo
	p.Context = c
	p.Call = call
	p.Path = path
	res := t.onDescribePath(p)
	return func(entryType string, e error) {
		var p SchemeDescribePathDoneInfo
		p.EntryType = entryType
		p.Error = e
		res(p)
	}
}
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func SchemeOnMakeDirectory(t *Scheme, c *context.Context, call call, path string) func(error) {
	var p SchemeMakeDirectoryStartInfo
	p.Context = c
	p.Call = call
	p.Path = path
	res := t.onMakeDirectory(p)
	return func(e error) {
		var p SchemeMakeDirectoryDoneInfo
		p.Error = e
		res(p)
	}
}
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func SchemeOnRemoveDirectory(t *Scheme, c *context.Context, call call, path string) func(error) {
	var p SchemeRemoveDirectoryStartInfo
	p.Context = c
	p.Call = call
	p.Path = path
	res := t.onRemoveDirectory(p)
	return func(e error) {
		var p SchemeRemoveDirectoryDoneInfo
		p.Error = e
		res(p)
	}
}
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func SchemeOnModifyPermissions(t *Scheme, c *context.Context, call call, path string) func(error) {
	var p SchemeModifyPermissionsStartInfo
	p.Context = c
	p.Call = call
	p.Path = path
	res := t.onModifyPermissions(p)
	return func(e error) {
		var p SchemeModifyPermissionsDoneInfo
		p.Error = e
		res(p)
	}
}
