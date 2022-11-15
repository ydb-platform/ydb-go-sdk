package incoming

import (
	"context"

	"google.golang.org/grpc/metadata"
)

type (
	MetadataCallback    func(header string, values []string)
	metadataCallbackKey struct{}
)

func WithMetadataCallback(ctx context.Context, callback MetadataCallback) context.Context {
	if existingCallback, has := ctx.Value(metadataCallbackKey{}).(MetadataCallback); has {
		return context.WithValue(ctx, metadataCallbackKey{}, MetadataCallback(
			func(header string, values []string) {
				existingCallback(header, values)
				callback(header, values)
			},
		))
	}
	return context.WithValue(ctx, metadataCallbackKey{}, callback)
}

func Notify(ctx context.Context, md metadata.MD) {
	if len(md) == 0 {
		return
	}
	callback, has := ctx.Value(metadataCallbackKey{}).(MetadataCallback)
	if !has {
		return
	}
	for k, v := range md {
		callback(k, v)
	}
}
