package topicoptions

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

// DescribeOption type for options of describe method. Not used now.
type DescribeOption func(req *rawtopic.DescribeTopicRequest)

// DescribeConsumerOption type for options of describe consumer method.
type DescribeConsumerOption func(req *rawtopic.DescribeConsumerRequest)

func IncludeConsumerStats() DescribeConsumerOption {
	return func(req *rawtopic.DescribeConsumerRequest) {
		req.IncludeStats = true
	}
}
