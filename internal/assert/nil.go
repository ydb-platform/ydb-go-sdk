package assert

type nillable interface {
	isNil() bool
}

func IsNil(v interface{}) bool {
	if nillable, ok := v.(nillable); ok {
		return nillable.isNil()
	}
	return v == nil
}
