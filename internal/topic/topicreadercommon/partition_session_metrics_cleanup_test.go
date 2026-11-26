package topicreadercommon

import (
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestPartitionSessionGaugeReleasesCollectorWhileUnregisterIsBlocked(t *testing.T) {
	var retained telemetry.Int64GaugeCallback
	unregisterStarted := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	meter := func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
		retained = cb

		return func() error {
			close(unregisterStarted)
			<-release

			return nil
		}, nil
	}
	pointer := func() weak.Pointer[[1 << 20]byte] {
		payload := new([1 << 20]byte)
		payload[0] = 1
		stop, err := RegisterPartitionSessionCount(
			ReaderMetricsConfig{Meter: meter}, "consumer", []*PublicReadSelector{{Path: "topic"}},
			func() (map[string]int64, error) {
				return map[string]int64{"topic": int64(payload[0])}, nil
			},
		)
		require.NoError(t, err)
		var observations []int64
		require.NoError(t, retained(t.Context(), func(n int64, _ ...telemetry.Attribute) {
			observations = append(observations, n)
		}))
		require.Equal(t, []int64{1}, observations)
		stop()
		stop()

		return weak.Make(payload)
	}()

	select {
	case <-unregisterStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("asynchronous unregister did not start")
	}
	require.NoError(t, retained(t.Context(), func(int64, ...telemetry.Attribute) {
		t.Error("closed callback produced an observation")
	}))
	runtime.GC()
	runtime.GC()
	require.Nil(t, pointer.Value(), "closed gauge callback still pins the collector payload")
	runtime.KeepAlive(retained)
}
