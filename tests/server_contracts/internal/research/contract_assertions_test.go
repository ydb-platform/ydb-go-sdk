package research_test

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/proto"
)

func TestContractACKsMatchSequenceNumbersAndResults(t *testing.T) {
	type writeResponses = []*Ydb_Topic.StreamWriteMessage_FromServer
	want := [][]string{
		{"1", "written_in_tx"},
		{"2", "written_in_tx"},
		{"2", "skipped"},
		{"2", "written_in_tx"},
		{"3", "written_in_tx"},
	}
	baseline := []*Ydb_Topic.StreamWriteMessage_FromServer{
		{Status: Ydb.StatusIds_SUCCESS, ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{PartitionId: 73, LastSeqNo: 900},
		}},
		contractACKResponse([]int64{1, 2}, false),
		contractACKResponse([]int64{2}, true),
		contractACKResponse([]int64{2, 3}, false),
	}
	if err := checkContractACKs(baseline, want); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(writeResponses) writeResponses
		valid  bool
	}{
		{name: "no observations", mutate: func(_ writeResponses) writeResponses {
			return nil
		}},
		{name: "missing ACKs", mutate: func(r writeResponses) writeResponses {
			return r[:3]
		}},
		{name: "duplicate ACKs", mutate: func(r writeResponses) writeResponses {
			return append(r, r[3])
		}},
		{name: "reordered ACKs across responses", valid: true, mutate: func(r writeResponses) writeResponses {
			r[1], r[2] = r[2], r[1]

			return r
		}},
		{name: "merged responses", valid: true, mutate: func(r writeResponses) writeResponses {
			r[1].GetWriteResponse().Acks = append(r[1].GetWriteResponse().Acks, r[2].GetWriteResponse().GetAcks()...)

			return append(r[:2], r[3])
		}},
		{name: "split responses", valid: true, mutate: func(r writeResponses) writeResponses {
			return append(writeResponses{
				r[0],
				contractACKResponse([]int64{1}, false),
				contractACKResponse([]int64{2}, false),
			}, r[2:]...)
		}},
		{name: "reordered ACKs within a response", valid: true, mutate: func(r writeResponses) writeResponses {
			acks := r[1].GetWriteResponse().GetAcks()
			acks[0], acks[1] = acks[1], acks[0]

			return r
		}},
		{name: "wrong sequence number", mutate: func(r writeResponses) writeResponses {
			r[1].GetWriteResponse().Acks[0].SeqNo = 99

			return r
		}},
		{name: "wrong repeated ACK count", mutate: func(r writeResponses) writeResponses {
			r[1].GetWriteResponse().Acks[1].SeqNo = 1

			return r
		}},
		{name: "wrong result", mutate: func(r writeResponses) writeResponses {
			r[2] = contractACKResponse([]int64{2}, false)

			return r
		}},
		{name: "server rejection", mutate: func(r writeResponses) writeResponses {
			r[1].Status = Ydb.StatusIds_BAD_REQUEST

			return r
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var responses []*Ydb_Topic.StreamWriteMessage_FromServer
			for _, response := range baseline {
				responses = append(responses, proto.Clone(response).(*Ydb_Topic.StreamWriteMessage_FromServer))
			}
			if err := checkContractACKs(test.mutate(responses), want); (err == nil) != test.valid {
				t.Fatalf("expected valid=%t, got %v", test.valid, err)
			}
		})
	}
}

func TestContractACKWaitUsesMessageCount(t *testing.T) {
	for _, test := range []struct {
		name    string
		batches [][]int64
	}{
		{name: "merged and reordered", batches: [][]int64{{3, 1, 2}}},
		{name: "split and reordered", batches: [][]int64{{2}, {3}, {1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			research := &streamWriteResearch{
				namedTransactions: map[string]*queryTransaction{
					"A": {id: "tx", sessionID: "query-session"},
				},
			}
			session, wire := startControlledSession(t, research, "S")
			ctx := context.WithValue(t.Context(), worldContextKey{}, &researchWorld{research: research})
			wire.received <- streamWriteReceive{message: &Ydb_Topic.StreamWriteMessage_FromServer{
				Status: Ydb.StatusIds_SUCCESS,
				ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
					InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{},
				},
			}}
			if _, err := session.receive(ctx); err != nil {
				t.Fatal(err)
			}
			if err := stepPipelineWrites(ctx); err != nil {
				t.Fatal(err)
			}
			if err := stepSendWriteRequest(ctx, "S", "txId: A", messageTableForTest(
				[]string{"data", "seq_no"}, []string{"first", "1"}, []string{"second", "2"},
			)); err != nil {
				t.Fatal(err)
			}
			if err := stepSendWriteRequest(ctx, "S", "txId: A", messageTableForTest(
				[]string{"data", "seq_no"}, []string{"third", "3"},
			)); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for _, numbers := range test.batches {
					select {
					case wire.received <- streamWriteReceive{message: contractACKResponse(numbers, false)}:
					case <-ctx.Done():
						return
					}
				}
			}()
			if err := contractACKs(ctx, "S", messageTableForTest(
				[]string{"seq_no", "result"}, []string{"1", "written_in_tx"},
				[]string{"2", "written_in_tx"}, []string{"3", "written_in_tx"},
			)); err != nil {
				t.Fatal(err)
			}
			<-done
			if pending := session.takePendingResponses(); pending != 0 {
				t.Fatalf("ACK wait left %d pending request observations", pending)
			}
			select {
			case <-session.receiveDone:
				t.Fatal("ACK wait required the stream to close")
			default:
			}
		})
	}
}

func contractACKResponse(numbers []int64, skipped bool) *Ydb_Topic.StreamWriteMessage_FromServer {
	write := &Ydb_Topic.StreamWriteMessage_WriteResponse{}
	for _, number := range numbers {
		ack := &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck{SeqNo: number}
		if skipped {
			ack.MessageWriteStatus = &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped_{
				Skipped: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Skipped{},
			}
		} else {
			ack.MessageWriteStatus = &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_WrittenInTx_{
				WrittenInTx: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_WrittenInTx{},
			}
		}
		write.Acks = append(write.Acks, ack)
	}

	return &Ydb_Topic.StreamWriteMessage_FromServer{
		Status:        Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_WriteResponse{WriteResponse: write},
	}
}
