package conversation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
)

func TestNewController(t *testing.T) {
	controller := NewController()
	require.NotNil(t, controller)
	require.NotNil(t, controller.notifyChan)
	require.NotNil(t, controller.conflicts)
}

func TestNewConversation(t *testing.T) {
	t.Run("SimpleConversation", func(t *testing.T) {
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_SessionStop_{
					SessionStop: &Ydb_Coordination.SessionRequest_SessionStop{},
				},
			}
		})
		require.NotNil(t, conv)
		require.NotNil(t, conv.message)
	})
	t.Run("WithResponseFilter", func(t *testing.T) {
		responseFilter := func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return true
		}
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithResponseFilter(responseFilter),
		)
		require.NotNil(t, conv)
		require.NotNil(t, conv.responseFilter)
	})
	t.Run("WithAcknowledgeFilter", func(t *testing.T) {
		acknowledgeFilter := func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return true
		}
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithAcknowledgeFilter(acknowledgeFilter),
		)
		require.NotNil(t, conv)
		require.NotNil(t, conv.acknowledgeFilter)
	})
	t.Run("WithCancelMessage", func(t *testing.T) {
		cancelMessage := func(req *Ydb_Coordination.SessionRequest) *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{}
		}
		cancelFilter := func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return true
		}
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithCancelMessage(cancelMessage, cancelFilter),
		)
		require.NotNil(t, conv)
		require.NotNil(t, conv.cancelMessage)
		require.NotNil(t, conv.cancelFilter)
	})
	t.Run("WithConflictKey", func(t *testing.T) {
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithConflictKey("test-key"),
		)
		require.NotNil(t, conv)
		require.Equal(t, "test-key", conv.conflictKey)
	})
	t.Run("WithIdempotence", func(t *testing.T) {
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithIdempotence(true),
		)
		require.NotNil(t, conv)
		require.True(t, conv.idempotent)
	})
	t.Run("WithAllOptions", func(t *testing.T) {
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
			WithAcknowledgeFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
			WithCancelMessage(
				func(req *Ydb_Coordination.SessionRequest) *Ydb_Coordination.SessionRequest {
					return &Ydb_Coordination.SessionRequest{}
				},
				func(
					request *Ydb_Coordination.SessionRequest,
					response *Ydb_Coordination.SessionResponse,
				) bool {
					return true
				},
			),
			WithConflictKey("test-key"),
			WithIdempotence(true),
		)
		require.NotNil(t, conv)
		require.NotNil(t, conv.responseFilter)
		require.NotNil(t, conv.acknowledgeFilter)
		require.NotNil(t, conv.cancelMessage)
		require.NotNil(t, conv.cancelFilter)
		require.Equal(t, "test-key", conv.conflictKey)
		require.True(t, conv.idempotent)
	})
	t.Run("WithNilOption", func(t *testing.T) {
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			nil,
		)
		require.NotNil(t, conv)
		require.NotNil(t, conv.message)
	})
}

func TestPushBack(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{}
		})
		err := controller.PushBack(conv)
		require.NoError(t, err)
	})
	t.Run("ClosedController", func(t *testing.T) {
		controller := NewController()
		controller.Close(nil)
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{}
		})
		err := controller.PushBack(conv)
		require.ErrorIs(t, err, coordination.ErrSessionClosed)
	})
}

func TestPushFront(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{}
		})
		err := controller.PushFront(conv)
		require.NoError(t, err)
	})
	t.Run("ClosedController", func(t *testing.T) {
		controller := NewController()
		controller.Close(nil)
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{}
		})
		err := controller.PushFront(conv)
		require.ErrorIs(t, err, coordination.ErrSessionClosed)
	})
}

func TestOnSend(t *testing.T) {
	t.Run("SendMessage", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_SessionStop_{
					SessionStop: &Ydb_Coordination.SessionRequest_SessionStop{},
				},
			}
		})
		err := controller.PushBack(conv)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
	})
	t.Run("CanceledContext", func(t *testing.T) {
		controller := NewController()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		msg, err := controller.OnSend(ctx)
		require.Error(t, err)
		require.Nil(t, msg)
	})
}

func TestOnRecv(t *testing.T) {
	t.Run("MatchingResponse", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Send the request
		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Receive the response
		response := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult_{
				CreateSemaphoreResult: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult{
					ReqId: 123,
				},
			},
		}
		handled := controller.OnRecv(response)
		require.True(t, handled)
	})
	t.Run("NonMatchingResponse", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Send the request
		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Receive a non-matching response
		response := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult_{
				CreateSemaphoreResult: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult{
					ReqId: 456, // Different ID
				},
			},
		}
		handled := controller.OnRecv(response)
		require.False(t, handled)
	})
	t.Run("AcknowledgeResponse", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_AcquireSemaphore_{
						AcquireSemaphore: &Ydb_Coordination.SessionRequest_AcquireSemaphore{
							ReqId: 123,
							Name:  "test",
							Count: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetAcquireSemaphoreResult().GetReqId() == request.GetAcquireSemaphore().GetReqId()
			}),
			WithAcknowledgeFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetAcquireSemaphorePending().GetReqId() == request.GetAcquireSemaphore().GetReqId()
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Send the request
		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Receive acknowledgement
		ackResponse := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_AcquireSemaphorePending_{
				AcquireSemaphorePending: &Ydb_Coordination.SessionResponse_AcquireSemaphorePending{
					ReqId: 123,
				},
			},
		}
		handled := controller.OnRecv(ackResponse)
		require.True(t, handled)
	})
}

func TestClose(t *testing.T) {
	t.Run("CloseEmptyController", func(t *testing.T) {
		controller := NewController()
		controller.Close(nil)
		require.True(t, controller.closed)
	})
	t.Run("CloseWithPendingConversations", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		controller.Close(nil)
		require.True(t, controller.closed)

		// Verify conversation is failed
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = controller.Await(ctx, conv)
		require.ErrorIs(t, err, coordination.ErrSessionClosed)
	})
	t.Run("CloseWithByeConversation", func(t *testing.T) {
		controller := NewController()
		byeConv := NewConversation(func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_SessionStop_{
					SessionStop: &Ydb_Coordination.SessionRequest_SessionStop{},
				},
			}
		})
		controller.Close(byeConv)
		require.True(t, controller.closed)
	})
}

func TestOnDetach(t *testing.T) {
	t.Run("FailNonIdempotentConversations", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
			WithIdempotence(false),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Send the request
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = controller.OnSend(ctx)
		require.NoError(t, err)

		controller.OnDetach()

		// Verify conversation is failed
		_, err = controller.Await(ctx, conv)
		require.ErrorIs(t, err, coordination.ErrOperationStatusUnknown)
	})
	t.Run("KeepIdempotentConversations", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
			WithIdempotence(true),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		controller.OnDetach()

		// Conversation should still be in queue (not failed)
		controller.mutex.Lock()
		queueLen := len(controller.queue)
		controller.mutex.Unlock()
		require.Equal(t, 1, queueLen)
	})
}

func TestOnAttach(t *testing.T) {
	t.Run("RetryIdempotentConversations", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return true
			}),
			WithIdempotence(true),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Send the request
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = controller.OnSend(ctx)
		require.NoError(t, err)

		// Simulate reconnect
		controller.OnAttach()

		// Verify conversation is retried
		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)
	})
}

func TestAwait(t *testing.T) {
	t.Run("AwaitSuccess", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Send and receive in goroutine
		go func() {
			ctx := context.Background()
			msg, _ := controller.OnSend(ctx)
			if msg != nil {
				response := &Ydb_Coordination.SessionResponse{
					Response: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult_{
						CreateSemaphoreResult: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult{
							ReqId: 123,
						},
					},
				}
				controller.OnRecv(response)
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := controller.Await(ctx, conv)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
	t.Run("AwaitCanceled", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
			}),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = controller.Await(ctx, conv)
		require.Error(t, err)
	})
	t.Run("AwaitWithCancelMessage", func(t *testing.T) {
		controller := NewController()
		conv := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_AcquireSemaphore_{
						AcquireSemaphore: &Ydb_Coordination.SessionRequest_AcquireSemaphore{
							ReqId: 123,
							Name:  "test",
							Count: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetAcquireSemaphoreResult().GetReqId() == request.GetAcquireSemaphore().GetReqId()
			}),
			WithCancelMessage(
				func(req *Ydb_Coordination.SessionRequest) *Ydb_Coordination.SessionRequest {
					return &Ydb_Coordination.SessionRequest{
						Request: &Ydb_Coordination.SessionRequest_ReleaseSemaphore_{
							ReleaseSemaphore: &Ydb_Coordination.SessionRequest_ReleaseSemaphore{
								ReqId: 456,
								Name:  "test",
							},
						},
					}
				},
				func(
					request *Ydb_Coordination.SessionRequest,
					response *Ydb_Coordination.SessionResponse,
				) bool {
					return response.GetReleaseSemaphoreResult().GetReqId() == request.GetReleaseSemaphore().GetReqId()
				},
			),
		)
		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Send the request
		go func() {
			ctx := context.Background()
			_, _ = controller.OnSend(ctx)
		}()

		// Wait a bit to ensure request is sent
		time.Sleep(10 * time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = controller.Await(ctx, conv)
		require.Error(t, err)
	})
}

func TestConflictKey(t *testing.T) {
	t.Run("ConflictingConversations", func(t *testing.T) {
		controller := NewController()

		// First conversation with conflict key
		conv1 := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
						CreateSemaphore: &Ydb_Coordination.SessionRequest_CreateSemaphore{
							ReqId: 123,
							Name:  "test",
							Limit: 1,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
			}),
			WithConflictKey("test"),
		)

		// Second conversation with same conflict key
		conv2 := NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_UpdateSemaphore_{
						UpdateSemaphore: &Ydb_Coordination.SessionRequest_UpdateSemaphore{
							ReqId: 456,
							Name:  "test",
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				return response.GetUpdateSemaphoreResult().GetReqId() == request.GetUpdateSemaphore().GetReqId()
			}),
			WithConflictKey("test"),
		)

		err := controller.PushBack(conv1)
		require.NoError(t, err)
		err = controller.PushBack(conv2)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// First message should be from conv1
		msg1, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg1)
		require.Equal(t, uint64(123), msg1.GetCreateSemaphore().GetReqId())

		// Second message should not be available yet (conflict key)
		controller.mutex.Lock()
		msg2 := controller.sendFront()
		controller.mutex.Unlock()
		require.Nil(t, msg2)

		// After receiving response for conv1, conv2 should be available
		response1 := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult_{
				CreateSemaphoreResult: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult{
					ReqId: 123,
				},
			},
		}
		controller.OnRecv(response1)

		// Now conv2 should be sendable
		msg2, err = controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg2)
		require.Equal(t, uint64(456), msg2.GetUpdateSemaphore().GetReqId())
	})
}

func TestOnRecvWithFailure(t *testing.T) {
	t.Run("ClosedControllerOnRecv", func(t *testing.T) {
		controller := NewController()
		controller.Close(nil)

		response := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_Failure_{
				Failure: &Ydb_Coordination.SessionResponse_Failure{
					Status: Ydb.StatusIds_BAD_SESSION,
				},
			},
		}
		handled := controller.OnRecv(response)
		require.True(t, handled)
	})
}
