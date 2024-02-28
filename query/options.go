package query

type idempotentOption struct{}

func WithIdempotent() idempotentOption {
	return idempotentOption{}
}
