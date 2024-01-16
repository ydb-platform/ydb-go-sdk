package query

type Column interface {
	Name() string
	Type() Type
}
