package node_hints

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"slices"
	"slo/internal/generator"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func describeTable(ctx context.Context, driver *ydb.Driver, tableName string) (desc options.Description, err error) {
	err = driver.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			desc, err = session.DescribeTable(ctx, tableName,
				options.WithTableStats(),
				options.WithPartitionStats(),
				options.WithShardKeyBounds(),
				options.WithShardNodesInfo(),
			)
			return err
		},
		table.WithIdempotent(),
	)
	return desc, err
}

type NodeSelector struct {
	LowerBounds []uint64
	UpperBounds []uint64
	NodeIDs     []uint32
}

func extractKey(v types.Value, side int) (uint64, error) {
	if types.IsNull(v) {
		if side == LEFT {
			return 0, nil
		} else {
			return ^uint64(0), nil
		}
	}
	parts, err := types.TupleItems(v)
	if err != nil {
		return 0, fmt.Errorf("extract tuple: %w", err)
	}

	var res uint64
	if err := types.CastTo(parts[0], &res); err != nil {
		return 0, fmt.Errorf("cast to uint64: %w", err)
	}

	return res, nil
}

const (
	LEFT  = iota
	RIGHT = iota
)

func MakeNodeSelector(ctx context.Context, driver *ydb.Driver, tableName string) (*NodeSelector, error) {
	dsc, err := describeTable(ctx, driver, tableName)

	if err != nil {
		return nil, err
	}

	s := NodeSelector{}

	for _, kr := range dsc.KeyRanges {
		l, err := extractKey(kr.From, LEFT)
		if err != nil {
			return nil, err
		}
		s.LowerBounds = append(s.LowerBounds, l)
		r, err := extractKey(kr.To, RIGHT)
		if err != nil {
			return nil, err
		}
		s.UpperBounds = append(s.UpperBounds, r)
	}

	for i := range len(s.UpperBounds) - 1 {
		if s.UpperBounds[i] >= s.UpperBounds[i+1] {
			for _, b := range s.UpperBounds {
				log.Println(b)
			}
			log.Fatalf("boundaries are not sorted")
		}
	}

	for _, ps := range dsc.Stats.PartitionStats {
		s.NodeIDs = append(s.NodeIDs, ps.LeaderNodeID)
	}
	return &s, nil
}

func (s *NodeSelector) findNodeID(key uint64) uint32 {
	idx, found := slices.BinarySearch(s.UpperBounds, key)
	if found {
		idx++
	}
	return s.NodeIDs[idx]
}

func (s *NodeSelector) WithNodeHint(ctx context.Context, key uint64) context.Context {
	if s == nil || len(s.NodeIDs) == 0 {
		return ctx
	}
	return ydb.WithPreferredNodeID(ctx, s.findNodeID(key))
}

func (s *NodeSelector) GeneratePartitionKey(partitionId uint64) uint64 {
	l := s.UpperBounds[partitionId] - s.LowerBounds[partitionId]
	return s.LowerBounds[partitionId] + rand.Uint64()%l
}

func RunUpdates(ctx context.Context, driver *ydb.Driver, tableName string, frequency time.Duration) (*atomic.Pointer[NodeSelector], error) {
	var ns atomic.Pointer[NodeSelector]
	updateSelector := func() error {
		selector, err := MakeNodeSelector(ctx, driver, tableName)
		if err != nil {
			return err
		}
		ns.Store(selector)
		return nil
	}

	err := updateSelector()
	if err != nil {
		return nil, err
	} else {
		ticker := time.NewTicker(frequency)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err = updateSelector()
					if err != nil {
						log.Printf("node hints update error: %v\n", err)
					}
				}
			}
		}()
	}
	return &ns, nil
}

func (ns *NodeSelector) GetRandomNodeID(generator generator.Generator) (int, uint32) {
	r, err := generator.Generate()
	if err != nil {
		log.Panicf("GetRandomNodeID: generator.Generate failed: %v", err)
	}
	shift := r.ID % uint64(len(ns.NodeIDs))
	for id, nodeID := range ns.NodeIDs {
		if id == int(shift) {
			return id, nodeID
		}
	}
	log.Panicf("GetRandomNodeID: no nodeID found for shift: %d", shift)
	return 0, 0
}
