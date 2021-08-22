package ydb

// EndpointInfo is struct contained information about endpoint
type EndpointInfo interface {
	Conn() *conn
	Address() string
}
