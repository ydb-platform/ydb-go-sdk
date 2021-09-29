package assert

type nillable interface {
	isNil() bool
}

func IsNil(v interface{}) bool {
	if n, ok := v.(nillable); ok {
		return n.isNil()
	}
	return v == nil
}
