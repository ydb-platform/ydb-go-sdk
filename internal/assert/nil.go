package assert

type nillable interface {
	IsNil() bool
}

func IsNil(v interface{}) bool {
	if n, ok := v.(nillable); ok {
		return n.IsNil()
	}
	return v == nil
}
