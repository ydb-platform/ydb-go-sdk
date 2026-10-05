package telemetry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestCollectorReduction(t *testing.T) {
	for _, reduction := range []telemetry.GaugeReduction{telemetry.GaugeSum, telemetry.GaugeMax} {
		t.Run(map[telemetry.GaugeReduction]string{telemetry.GaugeSum: "sum", telemetry.GaugeMax: "max"}[reduction],
			func(t *testing.T) {
				c := telemetry.NewCollector()
				desc := gaugeDescriptor("sessions", reduction)
				first, err := c.RegisterInt64Gauge(desc, constantSource(-5,
					telemetry.Attribute{Key: "a", Value: "b"}, telemetry.Attribute{Key: "c", Value: "d"}))
				require.NoError(t, err)
				_, err = c.RegisterInt64Gauge(desc, constantSource(-3,
					telemetry.Attribute{Key: "c", Value: "d"}, telemetry.Attribute{Key: "a", Value: "b"}))
				require.NoError(t, err)
				data, err := c.Collect(context.Background())
				require.NoError(t, err)
				require.Len(t, data, 1)
				require.Len(t, data[0].Points, 1)
				want := int64(-8)
				if reduction == telemetry.GaugeMax {
					want = -3
				}
				require.Equal(t, want, data[0].Points[0].Value)
				require.NoError(t, first.Close(context.Background()))
				require.NoError(t, first.Close(context.Background()))
				data, err = c.Collect(context.Background())
				require.NoError(t, err)
				require.Equal(t, int64(-3), data[0].Points[0].Value)
			})
	}
}

func TestCollectorErrors(t *testing.T) {
	c := telemetry.NewCollector()
	desc := gaugeDescriptor("sessions", telemetry.GaugeSum)
	_, err := c.RegisterInt64Gauge(desc, constantSource(1))
	require.NoError(t, err)
	conflict := desc
	conflict.Unit = "{other}"
	_, err = c.RegisterInt64Gauge(conflict, constantSource(1))
	require.Error(t, err)
	sourceErr := errors.New("source failed")
	_, err = c.RegisterInt64Gauge(desc, testSource(func(context.Context) ([]telemetry.Int64Point, error) {
		return nil, sourceErr
	}))
	require.NoError(t, err)
	_, err = c.RegisterInt64Gauge(gaugeDescriptor("other", telemetry.GaugeMax), constantSource(2))
	require.NoError(t, err)
	data, err := c.Collect(context.Background())
	require.ErrorIs(t, err, sourceErr)
	require.Len(t, data, 1)
	require.Equal(t, "other", data[0].Descriptor.Name)
}

func TestCollectorAttributeEncoding(t *testing.T) {
	c := telemetry.NewCollector()
	desc := gaugeDescriptor("sessions", telemetry.GaugeSum)
	for _, attribute := range []telemetry.Attribute{
		{Key: "a", Value: "b:c\x00d"}, {Key: "a:b", Value: "c\x00d"},
	} {
		_, err := c.RegisterInt64Gauge(desc, constantSource(1, attribute))
		require.NoError(t, err)
	}
	data, err := c.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, data[0].Points, 2)
	_, err = c.RegisterInt64Gauge(desc, constantSource(2,
		telemetry.Attribute{Key: "a", Value: "b"}, telemetry.Attribute{Key: "a", Value: "c"}))
	require.NoError(t, err)
	data, err = c.Collect(context.Background())
	require.Error(t, err)
	require.Empty(t, data)
}

func TestCollectorCloseDuringCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := telemetry.NewCollector()
	started, release := make(chan struct{}), make(chan struct{})
	reg, err := c.RegisterInt64Gauge(gaugeDescriptor("sessions", telemetry.GaugeSum),
		testSource(func(ctx context.Context) ([]telemetry.Int64Point, error) {
			close(started)
			select {
			case <-release:
				return []telemetry.Int64Point{{Value: 1}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}))
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, collectErr := c.Collect(ctx)
		result <- collectErr
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, reg.Close(canceled), context.Canceled)
	// Another source can register while Snapshot is blocked: no registry lock
	// is held across source calls.
	other, err := c.RegisterInt64Gauge(gaugeDescriptor("other", telemetry.GaugeSum), constantSource(2))
	require.NoError(t, err)
	data, err := c.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, data, 1)
	require.Equal(t, "other", data[0].Descriptor.Name)
	close(release)
	select {
	case err = <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, reg.Close(ctx))
	require.NoError(t, other.Close(ctx))
	data, err = c.Collect(ctx)
	require.NoError(t, err)
	require.Empty(t, data)
}

func gaugeDescriptor(name string, reduction telemetry.GaugeReduction) telemetry.Int64GaugeDescriptor {
	return telemetry.Int64GaugeDescriptor{
		Descriptor: telemetry.Descriptor{Name: name, Unit: "{session}"}, Reduction: reduction,
	}
}

type testSource func(context.Context) ([]telemetry.Int64Point, error)

func (s testSource) Snapshot(ctx context.Context) ([]telemetry.Int64Point, error) {
	return s(ctx)
}

func constantSource(value int64, attributes ...telemetry.Attribute) testSource {
	return func(context.Context) ([]telemetry.Int64Point, error) {
		return []telemetry.Int64Point{{Value: value, Attributes: append([]telemetry.Attribute(nil), attributes...)}}, nil
	}
}
