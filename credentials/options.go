package credentials

import (
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/credentials"
)

// WithSourceInfo option append to credentials object the source info for reporting source info details on error case
func WithSourceInfo(sourceInfo string) credentials.SourceInfoOption {
	return credentials.WithSourceInfo(sourceInfo)
}

// WithGrpcDialOptions option append to static credentials object GRPC dial options
func WithGrpcDialOptions(opts ...grpc.DialOption) credentials.StaticCredentialsOption {
	return credentials.WithGrpcDialOptions(opts...)
}
