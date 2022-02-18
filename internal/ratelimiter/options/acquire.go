package options

import "time"

const (
	DefaultDecrease = 100 * time.Millisecond
)

type AcquireType uint8

const (
	AcquireTypeAcquire = AcquireType(iota)
	AcquireTypeReportSync
	AcquireTypeReportAsync

	AcquireTypeDefault = AcquireTypeAcquire
)

type Acquire interface {
	// Type defines type of acquire request
	Type() AcquireType

	// DecreaseTimeout defines value for decrease timeout from context
	DecreaseTimeout() time.Duration
}

type acquireOptionsHolder struct {
	acquireType     AcquireType
	decreaseTimeout time.Duration
}

func (h *acquireOptionsHolder) DecreaseTimeout() time.Duration {
	return h.decreaseTimeout
}

func (h *acquireOptionsHolder) Type() AcquireType {
	return h.acquireType
}

type AcquireOption func(h *acquireOptionsHolder)

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

func WithDecrease(decreaseTimeout time.Duration) AcquireOption {
	return func(h *acquireOptionsHolder) {
		h.decreaseTimeout = decreaseTimeout
	}
}

func NewAcquire(opts ...AcquireOption) Acquire {
	h := &acquireOptionsHolder{
		acquireType:     AcquireTypeDefault,
		decreaseTimeout: DefaultDecrease,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}
