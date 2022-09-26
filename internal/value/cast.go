package value

func CastTo(v Value, dst interface{}) error {
	return v.castTo(dst)
}
