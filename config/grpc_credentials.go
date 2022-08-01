package config

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func grpcCredentials(secure bool, tlsConfig *tls.Config) grpc.DialOption {
	if secure {
		return grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig),
		)
	}
	return grpc.WithTransportCredentials(
		insecure.NewCredentials(),
	)
}
