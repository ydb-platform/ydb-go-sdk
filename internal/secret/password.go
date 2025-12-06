package secret

func Password(password string) string {
	return Mask(password)
}
