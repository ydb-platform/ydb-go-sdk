package tx

var _ Identifier = LazyID{}

const (
	LazyTxID = "LAZY_TX"
	FakeTxID = "FAKE_TX"
)

type (
	Identifier interface {
		ID() string
		isYdbTx()
	}
	LazyID struct {
		v *string
	}
)

func (id LazyID) ID() string {
	if id.v == nil {
		return LazyTxID
	}

	return *id.v
}

// SetTxID initializes a lazy ID. Once materialized, the ID stays unchanged so
// background topic workers can read it while query responses repeat the metadata.
// Initialization must finish before the ID is shared with concurrent users.
func (id *LazyID) SetTxID(txID string) {
	if id.v == nil {
		id.v = &txID
	}
}

func (id LazyID) isYdbTx() {}

func ID(id string) LazyID {
	return LazyID{v: &id}
}
