package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"slo/internal/config"
	"slo/internal/log"
)

type Estimator struct {
	cfg            *config.Config
	NodeInstances  map[uint32]string
	NodeRequests   map[uint32]float64
	ClusterCounter float64
}

func getMetric(ctx context.Context, cfg *config.Config, query string) model.Vector {
	client, err := api.NewClient(api.Config{
		Address: cfg.PrometheusEndpoint,
	})
	if err != nil {
		log.Panicf("api.NewClient failed: %v", err)
	}

	v1api := v1.NewAPI(client)

	result, warnings, err := v1api.Query(
		ctx,
		query,
		time.Now(),
	)
	if err != nil {
		log.Panicf("query failed: %v", err)
	}
	if len(warnings) > 0 {
		fmt.Println("Warnings: ", warnings)
	}
	vector, ok := result.(model.Vector)
	if !ok || len(vector) == 0 {
		log.Panicf("no data found for query: %s", query)
	}

	return vector
}

func getMetricValue(ctx context.Context, cfg *config.Config, query string) float64 {
	vector := getMetric(ctx, cfg, query)

	return float64(vector[0].Value)
}

func formatNodeID(v model.LabelValue) uint32 {
	id, err := strconv.Atoi(string(v))
	if err != nil {
		log.Panicf("formatNodeID failed: %v", err)
	}

	return uint32(id)
}

func NewEstimator(ctx context.Context, storage *Storage) *Estimator {
	e := &Estimator{
		cfg: storage.cfg,
	}
	vec := getMetric(ctx, e.cfg, `Traffic{}`)
	allNodeIDs := make(map[uint32]bool)
	instanceID := make(map[string]map[uint32]bool)
	nodeInstance := make(map[uint32]string)
	// get all node ids
	for _, v := range vec {
		allNodeIDs[formatNodeID(v.Metric["peer_node_id"])] = true
	}
	// for target instance, the only absent node id is correct
	for _, v := range vec {
		instance := string(v.Metric["instance"])
		instanceID[instance] = make(map[uint32]bool)
		for nodeID := range allNodeIDs {
			instanceID[instance][nodeID] = true
		}
	}
	for _, v := range vec {
		instance := string(v.Metric["instance"])
		instanceID[instance][formatNodeID(v.Metric["peer_node_id"])] = false
	}
	// backwards mapping
	for instance, nodeIDs := range instanceID {
		if strings.Contains(instance, "storage") {
			continue
		}
		for k, v := range nodeIDs {
			if v {
				nodeInstance[k] = instance
			}
		}
	}
	e.NodeInstances = nodeInstance
	e.NodeRequests = make(map[uint32]float64)
	// collect counters
	for nodeID := range e.NodeInstances {
		e.NodeRequests[nodeID] = e.NodeRWCounter(ctx, nodeID)
	}
	e.ClusterCounter = e.ClusterRWCounter(ctx)

	return e
}

func (e *Estimator) NodeGrpcAPICounter(ctx context.Context, method string, nodeID uint32) float64 {
	instance, ok := e.NodeInstances[nodeID]
	if !ok {
		log.Panicf("no instance found for nodeID: %d", nodeID)
	}

	return getMetricValue(ctx, e.cfg, fmt.Sprintf(`api_grpc_request_count{instance="%s",method="%s"}`, instance, method))
}

func (e *Estimator) ClusterGrpcAPICounter(ctx context.Context, method string) float64 {
	return getMetricValue(ctx, e.cfg, fmt.Sprintf(`sum(api_grpc_request_count{method="%s"})`, method))
}

func (e *Estimator) NodeRWCounter(ctx context.Context, nodeID uint32) float64 {
	return e.NodeGrpcAPICounter(ctx, "ReadRows", nodeID) + e.NodeGrpcAPICounter(ctx, "BulkUpsert", nodeID)
}

func (e *Estimator) ClusterRWCounter(ctx context.Context) float64 {
	return e.ClusterGrpcAPICounter(ctx, "ReadRows") + e.ClusterGrpcAPICounter(ctx, "BulkUpsert")
}

func (e *Estimator) OnlyThisNode(ctx context.Context, nodeID uint32) {
	clusterNow := e.ClusterRWCounter(ctx)
	nodeNow := e.NodeRWCounter(ctx, nodeID)
	if clusterNow-e.ClusterCounter > nodeNow-e.NodeRequests[nodeID] {
		log.Panicf("requests were served by other nodes: cluster %f -> %f, node %d %f -> %f",
			e.ClusterCounter, clusterNow,
			nodeID,
			e.NodeRequests[nodeID], nodeNow,
		)
	}
}
