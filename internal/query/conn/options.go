package conn

type Option func(c *Conn)

func WithOnClose(onClose func()) Option {
	return func(c *Conn) {
		c.onClose = append(c.onClose, onClose)
	}
}
