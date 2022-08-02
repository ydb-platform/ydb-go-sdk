package topicoptions

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

type DescribeOption func(req *rawtopic.DescribeTopicRequest)
