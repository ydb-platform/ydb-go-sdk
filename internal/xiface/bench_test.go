package xiface

import (
	"strconv"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type benchSession struct {
	nodeID uint32
}

func (s *benchSession) ID() string     { return "id" }
func (s *benchSession) NodeID() uint32 { return s.nodeID }
func (s *benchSession) Status() string { return "ready" }

func safeNodeIDReflect(s trace.SessionInfo) string {
	if isNilReflect(s) {
		return "0"
	}

	return strconv.FormatUint(uint64(s.NodeID()), 10)
}

func safeNodeIDUnsafe(s trace.SessionInfo) string {
	if IsNil(s) {
		return "0"
	}

	return strconv.FormatUint(uint64(s.NodeID()), 10)
}

func safeNodeIDEqOnly(s trace.SessionInfo) string {
	if s == nil {
		return "0"
	}

	return strconv.FormatUint(uint64(s.NodeID()), 10)
}

// BenchmarkSafeNodeID* models a typical metrics/spans hot fragment:
// read SessionInfo from a trace done-callback and format node_id label.
func BenchmarkSafeNodeIDUnsafeValid(b *testing.B) {
	s := trace.SessionInfo(&benchSession{nodeID: 42})
	for b.Loop() {
		_ = safeNodeIDUnsafe(s)
	}
}

func BenchmarkSafeNodeIDReflectValid(b *testing.B) {
	s := trace.SessionInfo(&benchSession{nodeID: 42})
	for b.Loop() {
		_ = safeNodeIDReflect(s)
	}
}

func BenchmarkSafeNodeIDEqOnlyValid(b *testing.B) {
	s := trace.SessionInfo(&benchSession{nodeID: 42})
	for b.Loop() {
		_ = safeNodeIDEqOnly(s)
	}
}

func BenchmarkSafeNodeIDUnsafeTypedNil(b *testing.B) {
	var typedNil *benchSession
	s := trace.SessionInfo(typedNil)
	for b.Loop() {
		_ = safeNodeIDUnsafe(s)
	}
}

func BenchmarkSafeNodeIDReflectTypedNil(b *testing.B) {
	var typedNil *benchSession
	s := trace.SessionInfo(typedNil)
	for b.Loop() {
		_ = safeNodeIDReflect(s)
	}
}
