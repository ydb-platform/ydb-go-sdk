package conn

import "time"

type Config interface {
	connConfig

	ConnectionTTL() time.Duration
}
