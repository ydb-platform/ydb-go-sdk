package tx

var _ Identifier = (*ID)(nil)

type (
	Identifier interface {
		ID() string
		isYdbTx()
	}
	ID string
)

func (id ID) ID() string {
	return string(id)
}

func (id ID) isYdbTx() {}
