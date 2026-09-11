package research_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

func initializeTopicPartitionSteps(sc *godog.ScenarioContext) {
	sc.Step(`^TopicService\.AlterTopic: AlterTopicRequest\{(.*)\}$`, stepAlterTopic)
}

func stepAlterTopic(ctx context.Context, parameters string) error {
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}
	request, err := parseAlterTopicRequest(parameters, research.world.topicPath)
	if err != nil {
		return err
	}
	method := Ydb_Topic_V1.TopicService_AlterTopic_FullMethodName
	research.observeProto("→", method, request)
	response, err := Ydb_Topic_V1.NewTopicServiceClient(ydb.GRPCConn(research.world.driver)).AlterTopic(ctx, request)
	if err != nil {
		research.observe(fmt.Sprintf("gRPC client ← server %s: %v.", method, err))

		return ctx.Err()
	}
	research.observeProto("←", method, response)

	return nil
}

func parseAlterTopicRequest(parameters, topicPath string) (*Ydb_Topic.AlterTopicRequest, error) {
	request := &Ydb_Topic.AlterTopicRequest{}
	if err := prototext.Unmarshal([]byte(parameters), request); err != nil {
		return nil, fmt.Errorf("parse AlterTopicRequest: %w", err)
	}
	if request.GetPath() != "" || request.GetOperationParams() != nil {
		return nil, errors.New("AlterTopic uses the scenario's topic path and synchronous operation mode")
	}
	request.Path = topicPath
	request.OperationParams = &Ydb_Operations.OperationParams{OperationMode: Ydb_Operations.OperationParams_SYNC}

	return request, nil
}

func (r *streamWriteResearch) observeProto(direction, method string, message proto.Message) {
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		r.observe(fmt.Sprintf("decode %s: %v", method, err))

		return
	}
	r.observe(fmt.Sprintf("gRPC client %s server %s / %s: %s.",
		direction, method, message.ProtoReflect().Descriptor().FullName(), encoded))
}
