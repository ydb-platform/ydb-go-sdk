package topicwriterinternal_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestRegressionOperationUnavailableIssue1007(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		e := fixenv.New(t)

		mock := newTopicWriterOperationUnavailable()
		connString := xtest.GrpcMockTopicConnString(e, mock)

		db, err := ydb.Open(sf.Context(e), connString)
		require.NoError(t, err)

		writer, err := db.Topic().StartWriter("test", topicoptions.WithWriterWaitServerAck(true))
		require.NoError(t, err)

		err = writer.Write(sf.Context(e), topicwriter.Message{
			Data: strings.NewReader("asd"),
		})
		require.NoError(t, err)
		require.True(t, mock.UnavailableResponsed)
	})
}

type topicWriterOperationUnavailable struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	UnavailableResponsed bool
}

func newTopicWriterOperationUnavailable() *topicWriterOperationUnavailable {
	return &topicWriterOperationUnavailable{}
}

func (t *topicWriterOperationUnavailable) StreamWrite(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
	initMsg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("failed read init message: %w", err)
	}

	if initMsg.GetInitRequest() == nil {
		return errors.New("first message must be init message")
	}

	err = server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{
				LastSeqNo:       0,
				SessionId:       "test",
				PartitionId:     0,
				SupportedCodecs: nil,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send init response: %w", err)
	}

	if !t.UnavailableResponsed {
		t.UnavailableResponsed = true

		err = server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
			Status: Ydb.StatusIds_UNAVAILABLE,
			Issues: []*Ydb_Issue.IssueMessage{
				{
					Message: "Test status unavailable",
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}

		return nil
	}

	// wait message block
	messagesMsg, err := server.Recv()
	if err != nil {
		return errors.New("failed to read messages block")
	}

	if len(messagesMsg.GetClientMessage().(*Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest).
		WriteRequest.GetMessages()) == 0 {
		return errors.New("received zero messages block")
	}

	err = server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_WriteResponse{
			WriteResponse: &Ydb_Topic.StreamWriteMessage_WriteResponse{
				Acks: []*Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck{
					{
						SeqNo: 1,
						MessageWriteStatus: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_{
							Written: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written{
								Offset: 1,
							},
						},
					},
				},
				PartitionId:     0,
				WriteStatistics: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteStatistics{},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to sent write ack: %w", err)
	}

	return nil
}
