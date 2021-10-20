package testutil

import (
	"fmt"
	"math/rand"
	"strconv"
)

type (
	sessionIDOption func(*sessionIDHolder)
	sessionIDHolder struct {
		serviceID uint32
		nodeID    uint32
		hash      string
	}
)

func WithServiceID(serviceID uint32) sessionIDOption {
	return func(h *sessionIDHolder) {
		h.serviceID = serviceID
	}
}

func SessionID(opts ...sessionIDOption) string {
	h := &sessionIDHolder{
		serviceID: rand.Uint32(),
		nodeID:    rand.Uint32(),
		hash:      strconv.FormatUint(rand.Uint64(), 16),
	}
	for _, o := range opts {
		o(h)
	}
	return fmt.Sprintf("ydb://session/%d?node_id=%d&id=%s==", h.serviceID, h.nodeID, h.hash)
}
