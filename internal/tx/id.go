package tx

var (
	_ Identifier = (*ID)(nil)
	_ Identifier = (*LazyID)(nil)
)

const (
	LazyTxID = "LAZY_TX"
)

type (
	Identifier interface {
		ID() string
		isYdbTx()
	}
	ID     string
	LazyID struct {
		v *string
	}
)

func (id *LazyID) ID() string {
	if id.v == nil {
		return LazyTxID
	}

	return *id.v
}

func (id *LazyID) SetTxID(txID string) {
	id.v = &txID
}

func (id *LazyID) isYdbTx() {}

func NewID(id string) ID {
	return ID(id)
}

func (id ID) ID() string {
	return string(id)
}

func (id ID) isYdbTx() {}
