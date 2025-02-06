package value

func CastTo(v Value, dst interface{}) error {
	if dst == nil {
		return errNilDestination
	}
	if ptr, has := dst.(*Value); has {
		*ptr = v

		return nil
	}

	if scanner, has := dst.(Scanner); has {
		return scanner.UnmarshalYDBValue(v)
	}

	return v.castTo(dst)
}

type Scanner interface {
	UnmarshalYDBValue(value Value) error
}
