package options

import "time"

type AcquireType uint8

const (
	AcquireTypeAcquire = AcquireType(iota)
	AcquireTypeReportSync
	AcquireTypeReportAsync

	AcquireTypeDefault = AcquireTypeAcquire
)

type Acquire interface {
	Type() AcquireType
	Timeout() *time.Duration
}

type acquireOptionsHolder struct {
	acquireType AcquireType
	timeout     *time.Duration
}

func (h *acquireOptionsHolder) Timeout() *time.Duration {
	return h.timeout
}

func (h *acquireOptionsHolder) Type() AcquireType {
	return h.acquireType
}

type AcquireOption func(h *acquireOptionsHolder)

func WithTimeout(timeout time.Duration) AcquireOption {
	return func(h *acquireOptionsHolder) {
		h.timeout = &timeout
	}
}

func WithAcquire() AcquireOption {
	return func(h *acquireOptionsHolder) {
		h.acquireType = AcquireTypeAcquire
	}
}

func WithReportAsync() AcquireOption {
	return func(h *acquireOptionsHolder) {
		h.acquireType = AcquireTypeReportAsync
	}
}

func WithReportSync() AcquireOption {
	return func(h *acquireOptionsHolder) {
		h.acquireType = AcquireTypeReportSync
	}
}

func NewAcquire(opts ...AcquireOption) Acquire {
	h := &acquireOptionsHolder{
		acquireType: AcquireTypeDefault,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}
