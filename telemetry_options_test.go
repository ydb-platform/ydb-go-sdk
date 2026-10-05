package ydb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestMeterScopesAreIndependent(t *testing.T) {
	ctx := context.Background()
	meter := telemetry.NewCollector()
	first := &meterScope{meter: meter, drained: make(chan struct{})}
	second := &meterScope{meter: meter, drained: make(chan struct{})}
	desc := telemetry.Int64GaugeDescriptor{
		Descriptor: telemetry.Descriptor{Name: "sessions"}, Reduction: telemetry.GaugeSum,
	}
	reg, err := first.RegisterInt64Gauge(desc, scopeTestSource{})
	require.NoError(t, err)
	_, err = second.RegisterInt64Gauge(desc, scopeTestSource{})
	require.NoError(t, err)
	data, err := meter.Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), data[0].Points[0].Value)
	require.NoError(t, first.Close(ctx))
	require.NoError(t, reg.Close(ctx))
	data, err = meter.Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), data[0].Points[0].Value)
	_, err = first.RegisterInt64Gauge(desc, scopeTestSource{})
	require.ErrorIs(t, err, errMeterScopeClosed)
	require.NoError(t, second.Close(ctx))
	data, err = meter.Collect(ctx)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestMeterScopeCloseDuringRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meter := &blockingScopeMeter{
		Collector: telemetry.NewCollector(), started: make(chan struct{}), release: make(chan struct{}),
		ctx: ctx,
	}
	scope := &meterScope{meter: meter, drained: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := scope.RegisterInt64Gauge(telemetry.Int64GaugeDescriptor{
			Descriptor: telemetry.Descriptor{Name: "sessions"}, Reduction: telemetry.GaugeSum,
		}, scopeTestSource{})
		result <- err
	}()
	select {
	case <-meter.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, scope.Close(canceled), context.Canceled)
	close(meter.release)
	select {
	case err := <-result:
		require.ErrorIs(t, err, errMeterScopeClosed)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, scope.Close(ctx))
	data, err := meter.Collect(ctx)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestMeterScopeRegistrationFailure(t *testing.T) {
	failure := errors.New("registration failed")
	scope := &meterScope{meter: failingScopeMeter{failure}, drained: make(chan struct{})}
	reg, err := scope.RegisterInt64Gauge(telemetry.Int64GaugeDescriptor{}, scopeTestSource{})
	require.ErrorIs(t, err, failure)
	require.Nil(t, reg)
	require.NoError(t, scope.Close(context.Background()))
}

type scopeTestSource struct{}

func (scopeTestSource) Snapshot(context.Context) ([]telemetry.Int64Point, error) {
	return []telemetry.Int64Point{{Value: 1}}, nil
}

type blockingScopeMeter struct {
	*telemetry.Collector

	started chan struct{}
	release chan struct{}
	ctx     context.Context //nolint:containedctx
}

func (m *blockingScopeMeter) RegisterInt64Gauge(
	desc telemetry.Int64GaugeDescriptor, source telemetry.Int64GaugeSource,
) (telemetry.Registration, error) {
	close(m.started)
	select {
	case <-m.release:
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}

	return m.Collector.RegisterInt64Gauge(desc, source)
}

type failingScopeMeter struct {
	err error
}

func (m failingScopeMeter) RegisterInt64Gauge(
	telemetry.Int64GaugeDescriptor, telemetry.Int64GaugeSource,
) (telemetry.Registration, error) {
	return nil, m.err
}
