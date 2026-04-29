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
)

type Estimator struct {
	prometheusEndpoint string
	NodeInstances      map[uint32]string
	NodeRequests       map[uint32]float64
	ClusterCounter     float64
}

func getMetric(ctx context.Context, prometheusEndpoint string, query string) (model.Vector, error) {
	client, err := api.NewClient(api.Config{
		Address: prometheusEndpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("api.NewClient failed: %w", err)
	}

	v1api := v1.NewAPI(client)

	result, warnings, err := v1api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if len(warnings) > 0 {
		fmt.Println("Warnings: ", warnings)
	}
	vector, ok := result.(model.Vector)
	if !ok || len(vector) == 0 {
		return nil, fmt.Errorf("no data found for query: %s", query)
	}

	return vector, nil
}

func getMetricValue(ctx context.Context, prometheusEndpoint string, query string) (float64, error) {
	vector, err := getMetric(ctx, prometheusEndpoint, query)
	if err != nil {
		return 0, err
	}

	return float64(vector[0].Value), nil
}

func formatNodeID(v model.LabelValue) (uint32, error) {
	i64, err := strconv.ParseUint(string(v), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("formatNodeID failed: %w", err)
	}

	return uint32(i64), nil
}

func NewEstimator(ctx context.Context, storage *Storage) *Estimator {
	e := &Estimator{
		prometheusEndpoint: storage.fw.Config.PrometheusEndpoint,
	}
	vec, err := getMetric(ctx, e.prometheusEndpoint, `Traffic{}`)
	if err != nil {
		storage.fw.Logger.Errorf("estimator: get Traffic metric: %v", err)

		return e
	}

	allNodeIDs := make(map[uint32]bool)
	instanceID := make(map[string]map[uint32]bool)
	nodeInstance := make(map[uint32]string)

	for _, v := range vec {
		nid, err := formatNodeID(v.Metric["peer_node_id"])
		if err != nil {
			storage.fw.Logger.Errorf("estimator: formatNodeID: %v", err)

			continue
		}
		allNodeIDs[nid] = true
	}

	for _, v := range vec {
		instance := string(v.Metric["instance"])
		instanceID[instance] = make(map[uint32]bool)
		for nodeID := range allNodeIDs {
			instanceID[instance][nodeID] = true
		}
	}
	for _, v := range vec {
		instance := string(v.Metric["instance"])
		nid, _ := formatNodeID(v.Metric["peer_node_id"])
		instanceID[instance][nid] = false
	}

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

	for nodeID := range e.NodeInstances {
		val, err := e.NodeRWCounter(ctx, nodeID)
		if err != nil {
			storage.fw.Logger.Errorf("estimator: NodeRWCounter: %v", err)

			continue
		}
		e.NodeRequests[nodeID] = val
	}
	clusterVal, err := e.ClusterRWCounter(ctx)
	if err != nil {
		storage.fw.Logger.Errorf("estimator: ClusterRWCounter: %v", err)
	}
	e.ClusterCounter = clusterVal

	return e
}

func (e *Estimator) NodeGrpcAPICounter(ctx context.Context, method string, nodeID uint32) (float64, error) {
	instance, ok := e.NodeInstances[nodeID]
	if !ok {
		return 0, fmt.Errorf("no instance found for nodeID: %d", nodeID)
	}

	return getMetricValue(ctx, e.prometheusEndpoint,
		fmt.Sprintf(`api_grpc_request_count{instance="%s",method="%s"}`, instance, method))
}

func (e *Estimator) ClusterGrpcAPICounter(ctx context.Context, method string) (float64, error) {
	return getMetricValue(ctx, e.prometheusEndpoint,
		fmt.Sprintf(`sum(api_grpc_request_count{method="%s"})`, method))
}

func (e *Estimator) NodeRWCounter(ctx context.Context, nodeID uint32) (float64, error) {
	readRows, err := e.NodeGrpcAPICounter(ctx, "ReadRows", nodeID)
	if err != nil {
		return 0, err
	}
	bulkUpsert, err := e.NodeGrpcAPICounter(ctx, "BulkUpsert", nodeID)
	if err != nil {
		return 0, err
	}

	return readRows + bulkUpsert, nil
}

func (e *Estimator) ClusterRWCounter(ctx context.Context) (float64, error) {
	readRows, err := e.ClusterGrpcAPICounter(ctx, "ReadRows")
	if err != nil {
		return 0, err
	}
	bulkUpsert, err := e.ClusterGrpcAPICounter(ctx, "BulkUpsert")
	if err != nil {
		return 0, err
	}

	return readRows + bulkUpsert, nil
}

func (e *Estimator) OnlyThisNode(ctx context.Context, nodeID uint32) error {
	clusterNow, err := e.ClusterRWCounter(ctx)
	if err != nil {
		return fmt.Errorf("get cluster counter: %w", err)
	}
	nodeNow, err := e.NodeRWCounter(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("get node counter: %w", err)
	}
	if clusterNow-e.ClusterCounter > nodeNow-e.NodeRequests[nodeID] {
		return fmt.Errorf("requests were served by other nodes: cluster %f -> %f, node %d %f -> %f",
			e.ClusterCounter, clusterNow,
			nodeID,
			e.NodeRequests[nodeID], nodeNow,
		)
	}

	return nil
}
