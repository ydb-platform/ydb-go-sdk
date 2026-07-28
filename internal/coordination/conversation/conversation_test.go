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
	t.Run("ConflictKeyIsSet", func(t *testing.T) {
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
			WithConflictKey("test"),
		)

		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Use OnSend to properly send the message
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Verify conflict key is set
		controller.mutex.Lock()
		_, hasConflict := controller.conflicts["test"]
		controller.mutex.Unlock()
		require.True(t, hasConflict)
	})
	t.Run("ConflictKeyReleasedAfterResponse", func(t *testing.T) {
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
			WithConflictKey("test"),
		)

		err := controller.PushBack(conv)
		require.NoError(t, err)

		// Use OnSend to properly send the message
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		// Verify conflict key is set
		controller.mutex.Lock()
		_, hasConflict := controller.conflicts["test"]
		controller.mutex.Unlock()
		require.True(t, hasConflict)

		// Process response
		response := &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult_{
				CreateSemaphoreResult: &Ydb_Coordination.SessionResponse_CreateSemaphoreResult{
					ReqId: 123,
				},
			},
		}
		handled := controller.OnRecv(response)
		require.True(t, handled)

		// Verify conflict key is cleared
		controller.mutex.Lock()
		_, hasConflict = controller.conflicts["test"]
		controller.mutex.Unlock()
		require.False(t, hasConflict)
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

func TestDescribeSemaphoreWatch(t *testing.T) {
	newDescribeConversation := func(
		reqID uint64,
		onChanged *[]bool,
	) *Conversation {
		return NewConversation(
			func() *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_DescribeSemaphore_{
						DescribeSemaphore: &Ydb_Coordination.SessionRequest_DescribeSemaphore{
							ReqId:       reqID,
							Name:        "sem",
							WatchData:   true,
							WatchOwners: true,
						},
					},
				}
			},
			WithResponseFilter(func(
				request *Ydb_Coordination.SessionRequest,
				response *Ydb_Coordination.SessionResponse,
			) bool {
				result := response.GetDescribeSemaphoreResult()

				return result != nil && result.GetReqId() == request.GetDescribeSemaphore().GetReqId()
			}),
			WithKeepAlive(func(response *Ydb_Coordination.SessionResponse) bool {
				return response.GetDescribeSemaphoreResult().GetWatchAdded()
			}),
			WithNotifyFilter(
				func(
					request *Ydb_Coordination.SessionRequest,
					response *Ydb_Coordination.SessionResponse,
				) bool {
					changed := response.GetDescribeSemaphoreChanged()

					return changed != nil && changed.GetReqId() == request.GetDescribeSemaphore().GetReqId()
				},
				func(response *Ydb_Coordination.SessionResponse) {
					changed := response.GetDescribeSemaphoreChanged()
					triggered := changed.GetDataChanged() || changed.GetOwnersChanged()
					*onChanged = append(*onChanged, triggered)
				},
			),
			WithOnAbandoned(func() {
				*onChanged = append(*onChanged, false)
			}),
			WithConflictKey("sem"),
			WithIdempotence(true),
		)
	}

	t.Run("ResultWithoutWatchRemovesConversation", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(1, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg.GetDescribeSemaphore().GetReqId())

		handled := controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      1,
					WatchAdded: false,
				},
			},
		})
		require.True(t, handled)

		resp, err := controller.Await(ctx, conv)
		require.NoError(t, err)
		require.False(t, resp.GetDescribeSemaphoreResult().GetWatchAdded())

		controller.mutex.Lock()
		queueLen := len(controller.queue)
		_, hasConflict := controller.conflicts["sem"]
		controller.mutex.Unlock()
		require.Equal(t, 0, queueLen)
		require.False(t, hasConflict)
		require.Empty(t, onChanged)
	})

	t.Run("WatchAddedThenChanged", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(2, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := controller.OnSend(ctx)
		require.NoError(t, err)

		handled := controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      2,
					WatchAdded: true,
				},
			},
		})
		require.True(t, handled)

		resp, err := controller.Await(ctx, conv)
		require.NoError(t, err)
		require.True(t, resp.GetDescribeSemaphoreResult().GetWatchAdded())

		controller.mutex.Lock()
		queueLen := len(controller.queue)
		_, hasConflict := controller.conflicts["sem"]
		controller.mutex.Unlock()
		require.Equal(t, 1, queueLen)
		require.False(t, hasConflict)

		// Conflict cleared: a superseding describe can be sent.
		var secondChanged []bool
		second := newDescribeConversation(3, &secondChanged)
		require.NoError(t, controller.PushBack(second))
		msg, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(3), msg.GetDescribeSemaphore().GetReqId())

		handled = controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
				DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
					ReqId:         2,
					DataChanged:   true,
					OwnersChanged: false,
				},
			},
		})
		require.True(t, handled)
		require.Equal(t, []bool{true}, onChanged)

		controller.mutex.Lock()
		queueLen = len(controller.queue)
		controller.mutex.Unlock()
		require.Equal(t, 1, queueLen) // only second conversation remains
	})

	t.Run("FalseWakeOnChanged", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(4, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      4,
					WatchAdded: true,
				},
			},
		}))
		_, err = controller.Await(ctx, conv)
		require.NoError(t, err)

		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
				DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
					ReqId:         4,
					DataChanged:   false,
					OwnersChanged: false,
				},
			},
		}))
		require.Equal(t, []bool{false}, onChanged)
	})

	t.Run("AbandonedOnDetach", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(5, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      5,
					WatchAdded: true,
				},
			},
		}))
		_, err = controller.Await(ctx, conv)
		require.NoError(t, err)

		controller.OnDetach()

		controller.mutex.Lock()
		queueLen := len(controller.queue)
		controller.mutex.Unlock()
		require.Equal(t, 0, queueLen)
		require.Equal(t, []bool{false}, onChanged)
	})

	t.Run("AbandonedOnClose", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(6, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      6,
					WatchAdded: true,
				},
			},
		}))
		_, err = controller.Await(ctx, conv)
		require.NoError(t, err)

		controller.Close(nil)
		require.Equal(t, []bool{false}, onChanged)
	})

	t.Run("UnexpectedChangedNotHandledWithoutWatch", func(t *testing.T) {
		controller := NewController()
		handled := controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
				DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
					ReqId:       7,
					DataChanged: true,
				},
			},
		})
		require.False(t, handled)
	})

	t.Run("SucceedFailCancelAfterResultDelivered", func(t *testing.T) {
		controller := NewController()
		var onChanged []bool
		conv := newDescribeConversation(8, &onChanged)
		require.NoError(t, controller.PushBack(conv))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, err := controller.OnSend(ctx)
		require.NoError(t, err)
		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      8,
					WatchAdded: true,
				},
			},
		}))
		_, err = controller.Await(ctx, conv)
		require.NoError(t, err)

		// Duplicate result must be ignored after Await completed.
		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
				DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
					ReqId:      8,
					WatchAdded: true,
				},
			},
		}))

		conv.fail(coordination.ErrSessionClosed) // no-op after resultDelivered
		conv.cancel()                            // marks canceled without closing done twice
		require.True(t, conv.canceled)

		require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
				DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
					ReqId:       8,
					DataChanged: true,
				},
			},
		}))
		require.Empty(t, onChanged) // canceled watch does not invoke onNotify
	})
}
