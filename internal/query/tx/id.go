package tx

var _ Identifier = (*Parent)(nil)

type Parent string

func (id Parent) ID() string {
	return string(id)
}

func (id Parent) isYdbTx() {}
