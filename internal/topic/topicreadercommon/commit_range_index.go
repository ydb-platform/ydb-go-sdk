package topicreadercommon

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"

// commitRangeNode is part of an end-keyed treap. Each key holds the sum of
// all submissions ending there, and sum includes both subtrees.
type commitRangeNode struct {
	left, right *commitRangeNode
	end         rawtopiccommon.Offset
	count, sum  int64
	priority    uint64
}

func (n *commitRangeNode) subtreeSum() int64 {
	if n == nil {
		return 0
	}

	return n.sum
}

func (n *commitRangeNode) updateSum() {
	n.sum = n.count + n.left.subtreeSum() + n.right.subtreeSum()
}

func (n *commitRangeNode) insert(r commitMetricRange, priority uint64) *commitRangeNode {
	if n == nil {
		return &commitRangeNode{end: r.end, count: r.count, sum: r.count, priority: priority}
	}

	switch {
	case r.end < n.end:
		left := n.left.insert(r, priority)
		if left.priority > n.priority {
			n.left, left.right = left.right, n
			n.updateSum()
			n = left
		} else {
			n.left = left
		}
	case r.end > n.end:
		right := n.right.insert(r, priority)
		if right.priority > n.priority {
			n.right, right.left = right.left, n
			n.updateSum()
			n = right
		} else {
			n.right = right
		}
	default:
		n.count += r.count
	}
	n.updateSum()

	return n
}

// removeThrough returns the remaining tree and the sum of the discarded
// prefix. It touches only one path; covered subtrees are detached as units.
func (n *commitRangeNode) removeThrough(end rawtopiccommon.Offset) (*commitRangeNode, int64) {
	if n == nil {
		return nil, 0
	}
	if n.end <= end {
		right, covered := n.right.removeThrough(end)

		return right, covered + n.count + n.left.subtreeSum()
	}

	var covered int64
	n.left, covered = n.left.removeThrough(end)
	n.updateSum()

	return n, covered
}
