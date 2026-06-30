package trace

// TxInfo contains transaction metadata for trace hooks.
type TxInfo interface {
	ID() string
}
