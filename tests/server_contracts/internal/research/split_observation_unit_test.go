package research_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/tests/server_contracts/internal/grpcclient"
)

func TestSplitObservationStopsAfterAlterAndFreshDescription(t *testing.T) {
	for _, test := range []struct {
		name         string
		alterStatus  Ydb.StatusIds_StatusCode
		rejectWrites bool
	}{
		{name: "success without split", alterStatus: Ydb.StatusIds_SUCCESS},
		{name: "alter rejected", alterStatus: Ydb.StatusIds_BAD_REQUEST},
		{name: "writer rejected before alter completed", alterStatus: Ydb.StatusIds_SUCCESS, rejectWrites: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			research := &streamWriteResearch{}
			world := &researchWorld{research: research, topicPath: "/local/topic"}
			research.world = world
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx = context.WithValue(ctx, worldContextKey{}, world)
			_, wire := startControlledSession(t, research, "Parent")
			firstDescription := make(chan struct{})
			var described sync.Once
			postAlterSamples := 0
			conn, err := grpcclient.Open("grpc://unused:2136/local", grpc.WithUnaryInterceptor(func(
				ctx context.Context, method string, _, reply any, _ *grpc.ClientConn,
				_ grpc.UnaryInvoker, _ ...grpc.CallOption,
			) error {
				switch method {
				case Ydb_Topic_V1.TopicService_AlterTopic_FullMethodName:
					select {
					case <-firstDescription:
					case <-ctx.Done():
						return ctx.Err()
					}
					proto.Merge(reply.(proto.Message), &Ydb_Topic.AlterTopicResponse{
						Operation: &Ydb_Operations.Operation{Ready: true, Status: test.alterStatus},
					})
				case Ydb_Topic_V1.TopicService_DescribeTopic_FullMethodName:
					// Describe runs on the observation goroutine, so this inspects the
					// completion the loop has actually consumed, without timing assumptions.
					if !research.splitObservation.alterFinished.IsZero() {
						postAlterSamples++
						if postAlterSamples > 1 {
							return errors.New("unexpected repeated probing after AlterTopic completed")
						}
					}
					result, err := anypb.New(&Ydb_Topic.DescribeTopicResult{
						Partitions: []*Ydb_Topic.DescribeTopicResult_PartitionInfo{{PartitionId: 0, Active: true}},
					})
					if err != nil {
						return err
					}
					proto.Merge(reply.(proto.Message), &Ydb_Topic.DescribeTopicResponse{
						Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: result},
					})
					described.Do(func() { close(firstDescription) })
				}

				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			world.conn = conn
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				for {
					select {
					case request := <-wire.sent:
						response := writeAckForTest(request.GetWriteRequest().GetMessages()[0].GetSeqNo())
						if test.rejectWrites {
							response = &Ydb_Topic.StreamWriteMessage_FromServer{Status: Ydb.StatusIds_OVERLOADED}
						}
						select {
						case wire.received <- streamWriteReceive{message: response}:
						case <-ctx.Done():
							return
						}
					case <-ctx.Done():
						return
					}
				}
			}()
			err = stepAlterWhileDescribing(ctx,
				"alter_partitioning_settings: {set_min_active_partitions: 2, set_max_active_partitions: 2}", "Parent", 1)
			cancel()
			<-writerDone
			if err != nil {
				t.Fatalf("research must finish after AlterTopic and a fresh description without requiring rejection: %v", err)
			}
			observation := research.splitObservation
			if postAlterSamples != 1 || observation.alterResponse.GetOperation().GetStatus() != test.alterStatus ||
				observation.writeFailed.IsZero() == test.rejectWrites {
				t.Fatalf("observation lost the actual outcome: %+v; final descriptions=%d", observation, postAlterSamples)
			}
			last := observation.samples[len(observation.samples)-1]
			if !last.started.After(observation.alterFinished) || !last.result.GetPartitions()[0].GetActive() {
				t.Fatalf("final description does not preserve the active parent: %+v", last)
			}
			if !test.rejectWrites && !strings.Contains(research.HumanReadableReport(), "writer_rejected=false") {
				t.Fatal("research must explicitly report that writer rejection was not observed")
			}
		})
	}
}
