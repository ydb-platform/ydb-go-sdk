// package controlutil contains methods and interfaces for testing topic/table alter/creation
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
package controlutil

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

// CreateTableOptionsDesc contains request which will be used in CreateTable operation after applying create options.
//
// It is used to compare alter requests in testing purposes.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CreateTableOptionsDesc struct {
	opts *options.CreateTableDesc
}

// ToCreateTableOptionsDesc creates new table options description used for creating YDB table.
//
// This function should be used only for testing purpose.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func ToCreateTableOptionsDesc(opts ...options.CreateTableOption) *CreateTableOptionsDesc {
	req := &options.CreateTableDesc{}
	all := allocator.New()
	defer all.Free()

	for _, v := range opts {
		v(req, all)
	}

	return &CreateTableOptionsDesc{
		opts: req,
	}
}

// AlterTableOptionsDesc contains request which will be used in AlterTable operation after applying alter options.
//
// It is used to compare alter requests in testing purposes.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type AlterTableOptionsDesc struct {
	opts *options.AlterTableDesc
}

// ToAlterTableOptionsDesc create new table options description used for altering YDB table
//
// This function should be used only for testing purpose.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func ToAlterTableOptionsDesc(opts ...options.AlterTableOption) *AlterTableOptionsDesc {
	req := &options.AlterTableDesc{}
	all := allocator.New()
	defer all.Free()

	for _, v := range opts {
		v(req, all)
	}

	return &AlterTableOptionsDesc{
		opts: req,
	}
}

// CreateTopicOptionsDesc contains request which will be used in CreateTopic operation after applying create options.
//
// It is used to compare alter requests in testing purposes.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CreateTopicOptionsDesc struct {
	opts *rawtopic.CreateTopicRequest
}

// ToCreateTopicOptionsDesc compares table options used for creating YDB topic.
//
// This function should be used only for testing purpose.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func ToCreateTopicOptionsDesc(opts ...topicoptions.CreateOption) *CreateTopicOptionsDesc {
	req := &rawtopic.CreateTopicRequest{}
	for _, v := range opts {
		v(req)
	}

	return &CreateTopicOptionsDesc{
		opts: req,
	}
}

// AlterTopicOptionsDesc contains request which will be used in AlterTopic operation after applying alter options.
//
// It is used to compare alter requests in testing purposes.
type AlterTopicOptionsDesc struct {
	opts *rawtopic.AlterTopicRequest
}

// ToAlterTopicOptionsDesc compares table options used for altering YDB topic.
//
// This function should be used only for testing purpose.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func ToAlterTopicOptionsDesc(opts ...topicoptions.AlterOption) *AlterTopicOptionsDesc {
	req := &rawtopic.AlterTopicRequest{}
	for _, v := range opts {
		v(req)
	}

	return &AlterTopicOptionsDesc{
		opts: req,
	}
}
