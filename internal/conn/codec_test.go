package conn

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/test/bufconn"
)

func TestFallbackCodecPreservesConfiguredCodec(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name, want := "fallback", "fallback"
		if configured {
			name, want = "connection_default", "configured"
		}
		t.Run(name, func(t *testing.T) {
			opts := []grpc.DialOption{grpc.WithChainStreamInterceptor(FallbackCodecStreamInterceptor)}
			protobuf := encoding.GetCodecV2("proto")
			if configured {
				opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodecV2(
					responseBodyCodec{CodecV2: protobuf, body: "configured"},
				)))
			}
			cc := newCodecTestConn(t, opts...)
			ctx := WithFallbackCodec(t.Context(), responseBodyCodec{CodecV2: protobuf, body: "fallback"})
			require.Equal(t, want, receiveCodecBody(t, ctx, cc), "the selected codec must decode the response")
		})
	}
}

func TestFallbackCodecPreservesReusableCallOptions(t *testing.T) {
	shared := make([]grpc.CallOption, 1, 2)
	shared[0] = grpc.MaxCallRecvMsgSize(1 << 20)
	backing := shared[:cap(shared)]
	sentinel := grpc.WaitForReady(true)
	backing[1] = sentinel
	cc := newCodecTestConn(t, grpc.WithChainStreamInterceptor(
		func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
			streamer grpc.Streamer, _ ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			return streamer(ctx, desc, cc, method, shared...)
		},
		FallbackCodecStreamInterceptor,
	))
	ctx := WithFallbackCodec(t.Context(), responseBodyCodec{CodecV2: encoding.GetCodecV2("proto"), body: "fallback"})
	require.Equal(t, "fallback", receiveCodecBody(t, ctx, cc))
	require.Equal(t, sentinel, backing[1], "selecting a fallback must not mutate the caller's options backing array")
}

func newCodecTestConn(t *testing.T, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	service := &inFlightTestService{halfClosed: make(chan struct{}), release: make(chan struct{})}
	close(service.release)
	server := grpc.NewServer()
	grpc_testing.RegisterTestServiceServer(server, service)
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = server.Serve(listener) }()
	cc, err := grpc.NewClient("passthrough:///codec", append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })

	return cc
}

func receiveCodecBody(t *testing.T, ctx context.Context, cc *grpc.ClientConn) string {
	t.Helper()
	stream, err := grpc_testing.NewTestServiceClient(cc).FullDuplexCall(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	return string(response.GetPayload().GetBody())
}

type responseBodyCodec struct {
	encoding.CodecV2
	body string
}

func (c responseBodyCodec) Unmarshal(data mem.BufferSlice, dst any) error {
	if err := c.CodecV2.Unmarshal(data, dst); err != nil {
		return err
	}
	if response, ok := dst.(*grpc_testing.StreamingOutputCallResponse); ok {
		response.Payload = &grpc_testing.Payload{Body: []byte(c.body)}
	}
	return nil
}
