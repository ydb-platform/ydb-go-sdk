package tx

var _ Identifier = (*ID)(nil)

type (
	Identifier interface {
		ID() string
		isYdbTx()
	}
	ID string
)

var Lazy = ID("")

func NewID(id string) ID {
	return ID(id)
}

func (id ID) ID() string {
	return string(id)
}

func (id ID) isYdbTx() {}
