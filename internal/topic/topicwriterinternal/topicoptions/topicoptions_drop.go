package topicoptions

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

type DropOption func(request *rawtopic.DropTopicRequest)
