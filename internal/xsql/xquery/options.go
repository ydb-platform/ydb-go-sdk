package xquery

type Option func(c *Conn)

func WithOnClose(onClose func()) Option {
	return func(c *Conn) {
		c.onClose = append(c.onClose, onClose)
	}
}

func WithFakeTx() Option {
	return func(c *Conn) {
		c.fakeTx = true
	}
}

func WithResponsePartPrefetch(parts int) Option {
	if parts <= 0 {
		parts = 0
	}

	return func(c *Conn) {
		c.responsePartPrefetch = parts
	}
}
