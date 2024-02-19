package rawtopicwriter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestSendWriteRequest(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		expected := &Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest{
			WriteRequest: &Ydb_Topic.StreamWriteMessage_WriteRequest{
				Codec: 123,
				Messages: []*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
					{
						SeqNo: 1,
					},
				},
			},
		}

		sendCounter := 0
		var send sendFunc = func(req *Ydb_Topic.StreamWriteMessage_FromClient) error {
			sendCounter++
			require.Equal(t, expected, req.GetClientMessage())

			return nil
		}
		err := sendWriteRequest(send, expected)
		require.NoError(t, err)
		require.Equal(t, 1, sendCounter)
	})

	t.Run("Split", func(t *testing.T) {
		originalMessage := &Ydb_Topic.StreamWriteMessage_WriteRequest{
			Codec: 123,
			Messages: []*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
				{
					SeqNo: 1,
				},
				{
					SeqNo: 2,
				},
				{
					SeqNo: 3,
				},
			},
		}

		split1 := &Ydb_Topic.StreamWriteMessage_WriteRequest{
			Codec: 123,
			Messages: []*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
				{
					SeqNo: 1,
				},
			},
		}

		split2 := &Ydb_Topic.StreamWriteMessage_WriteRequest{
			Codec: 123,
			Messages: []*Ydb_Topic.StreamWriteMessage_WriteRequest_MessageData{
				{
					SeqNo: 2,
				},
				{
					SeqNo: 3,
				},
			},
		}

		getWriteRequest := func(req *Ydb_Topic.StreamWriteMessage_FromClient) *Ydb_Topic.StreamWriteMessage_WriteRequest {
			return req.GetClientMessage().(*Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest).WriteRequest
		}

		sendCounter := 0
		var send sendFunc = func(commonReq *Ydb_Topic.StreamWriteMessage_FromClient) error {
			sendCounter++
			req := getWriteRequest(commonReq)
			switch sendCounter {
			case 1:
				require.Equal(t, originalMessage, req)

				return grpcStatus.Error(codes.ResourceExhausted, "test resource exhausted")
			case 2:
				require.Equal(t, split1, req)
			case 3:
				require.Equal(t, split2, req)
			default:
				t.Fatal()
			}

			return nil
		}

		err := sendWriteRequest(send, &Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest{WriteRequest: originalMessage})
		require.NoError(t, err)
		require.Equal(t, 3, sendCounter)
	})
}
