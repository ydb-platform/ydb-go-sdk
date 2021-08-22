package ydb

// CallInfo is struct contained information about call
type CallInfo interface {
	EndpointInfo
}

type callInfo struct {
	conn *conn
}

func (info *callInfo) Conn() *conn {
	return info.conn
}

func (info *callInfo) Address() string {
	return info.conn.addr.String()
}
