package ydb

import (
	"context"
	"errors"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

// WithMeter enables independent observable gauges. Nil disables them.
// The driver unregisters its sources on close but does not close the meter or
// an application exporter. Child drivers inherit the meter with separate scopes.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithMeter(meter telemetry.Meter) Option {
	return func(_ context.Context, d *Driver) error {
		d.meter = meter

		return nil
	}
}

var errMeterScopeClosed = errors.New("ydb: telemetry scope closed")

type meterScope struct {
	meter         telemetry.Meter
	mu            sync.Mutex
	registrations map[*scopedRegistration]struct{}
	pending       int
	closed        bool
	drained       chan struct{}
	closeContext  context.Context //nolint:containedctx
	closeErrors   []error
}

func (s *meterScope) RegisterInt64Gauge(
	descriptor telemetry.Int64GaugeDescriptor,
	source telemetry.Int64GaugeSource,
) (telemetry.Registration, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return nil, errMeterScopeClosed
	}
	s.pending++
	s.mu.Unlock()

	raw, err := s.meter.RegisterInt64Gauge(descriptor, source)
	s.mu.Lock()
	if !s.closed {
		s.pending--
		if err != nil {
			s.mu.Unlock()

			return nil, err
		}
		reg := &scopedRegistration{scope: s, registration: raw}
		if s.registrations == nil {
			s.registrations = make(map[*scopedRegistration]struct{})
		}
		s.registrations[reg] = struct{}{}
		s.mu.Unlock()

		return reg, nil
	}
	ctx := s.closeContext
	s.mu.Unlock()
	var closeErr error
	if err == nil {
		closeErr = raw.Close(ctx)
	}
	s.mu.Lock()
	if closeErr != nil {
		s.closeErrors = append(s.closeErrors, closeErr)
	}
	s.pending--
	if s.pending == 0 {
		close(s.drained)
	}
	s.mu.Unlock()

	return nil, errors.Join(errMeterScopeClosed, err, closeErr)
}

func (s *meterScope) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.closeContext = ctx
		if s.pending == 0 {
			close(s.drained)
		}
	}
	registrations := make([]*scopedRegistration, 0, len(s.registrations))
	for reg := range s.registrations {
		registrations = append(registrations, reg)
	}
	s.mu.Unlock()
	var issues []error
	for _, reg := range registrations {
		if err := reg.Close(ctx); err != nil {
			issues = append(issues, err)
		}
	}
	select {
	case <-s.drained:
	case <-ctx.Done():
		issues = append(issues, ctx.Err())
	}
	s.mu.Lock()
	issues = append(issues, s.closeErrors...)
	s.mu.Unlock()

	return errors.Join(issues...)
}

type scopedRegistration struct {
	scope        *meterScope
	registration telemetry.Registration
}

func (r *scopedRegistration) Close(ctx context.Context) error {
	err := r.registration.Close(ctx)
	r.scope.mu.Lock()
	delete(r.scope.registrations, r)
	r.scope.mu.Unlock()

	return err
}
