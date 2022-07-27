package topicoptions

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

// DropOption
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type DropOption func(request *rawtopic.DropTopicRequest)
