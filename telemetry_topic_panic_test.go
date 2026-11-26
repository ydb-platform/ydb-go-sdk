package ydb_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestTopicMeterUnregisterPanicUsesDriverCallback(t *testing.T) {
	const childEnv = "YDB_TEST_TOPIC_METER_PANIC_CHILD"
	const witness = "topic meter unregister panic"

	for _, kind := range []string{"reader", "listener"} {
		t.Run(kind, func(t *testing.T) {
			if os.Getenv(childEnv) != "1" {
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				// A missing callback would crash the process, not just fail this test.
				child := exec.CommandContext(ctx, os.Args[0],
					"-test.v", "-test.run=^TestTopicMeterUnregisterPanicUsesDriverCallback$/^"+kind+"$")
				child.Env = append(os.Environ(), childEnv+"=1")
				output, err := child.CombinedOutput()
				require.NoError(t, err, "%s", output)

				return
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			address, server := newMetricTopicServer(ctx, t)
			panics := make(chan any, 1)
			db, err := ydb.Open(ctx, "grpc://"+address+"/local",
				ydb.WithPanicCallback(func(value any) { panics <- value }),
				ydb.WithAnonymousCredentials(), ydb.WithBalancer(balancers.SingleConn()),
				ydb.WithMeter(func(telemetry.Descriptor, telemetry.Int64GaugeCallback) (func() error, error) {
					return func() error { panic(witness) }, nil
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer stop()
				require.NoError(t, db.Close(cleanup))
			})

			switch kind {
			case "reader":
				reader, startErr := db.Topic().StartReader("consumer", topicoptions.ReadTopic("topic"))
				require.NoError(t, startErr)
				readCtx, stopRead := context.WithCancel(ctx)
				defer stopRead()
				readDone := make(chan struct{})
				go func() {
					defer close(readDone)
					_, _ = reader.ReadMessage(readCtx) // Only admit the stream; no data is needed.
				}()
				require.NoError(t, reader.WaitInit(ctx))
				// Let the fake enter Recv so CloseSend can finish the RPC.
				select {
				case session := <-server.sessions:
					close(session.start)
					select {
					case <-session.confirmed:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				stopRead()
				select {
				case <-readDone:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.NoError(t, reader.Close(ctx))
			case "listener":
				listener, startErr := db.Topic().StartListener(
					"consumer", topiclistener.BaseHandler{}, topicoptions.ReadTopic("topic"),
				)
				require.NoError(t, startErr)
				require.NoError(t, listener.WaitInit(ctx))
				require.NoError(t, listener.Close(ctx))
			}

			select {
			case value := <-panics:
				require.Equal(t, witness, value)
			case <-ctx.Done():
				t.Fatal("driver panic callback was not called")
			}
		})
	}
}
