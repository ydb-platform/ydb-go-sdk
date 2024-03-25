package testutil

import (
	"fmt"
	"math"
	"strconv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
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

func WithNodeID(nodeID uint32) sessionIDOption {
	return func(h *sessionIDHolder) {
		h.nodeID = nodeID
	}
}

func SessionID(opts ...sessionIDOption) string {
	h := &sessionIDHolder{
		serviceID: uint32(xrand.New().Int64(math.MaxUint32)),
		nodeID:    uint32(xrand.New().Int64(math.MaxUint32)),
		hash:      strconv.FormatInt(xrand.New().Int64(math.MaxInt64), 16),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}

	return fmt.Sprintf("ydb://session/%d?node_id=%d&id=%s==", h.serviceID, h.nodeID, h.hash)
}
