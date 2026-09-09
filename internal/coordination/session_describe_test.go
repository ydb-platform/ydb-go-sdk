package coordination

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/conversation"
)

func TestDescribeSemaphore(t *testing.T) {
	t.Run("WithoutWatch", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()
			require.NotNil(t, req)
			require.Equal(t, "sem", req.GetName())
			require.True(t, req.GetIncludeOwners())
			require.False(t, req.GetWatchData())
			require.False(t, req.GetWatchOwners())

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status: Ydb.StatusIds_SUCCESS,
						ReqId:  req.GetReqId(),
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name:  "sem",
							Count: 2,
							Limit: 5,
							Data:  []byte("payload"),
							Owners: []*Ydb_Coordination.SemaphoreSession{
								{
									OrderId:       1,
									SessionId:     42,
									Count:         2,
									Data:          []byte("owner"),
									TimeoutMillis: math.MaxUint64,
								},
								nil,
							},
							Waiters: []*Ydb_Coordination.SemaphoreSession{{
								OrderId:       2,
								SessionId:     43,
								Count:         1,
								TimeoutMillis: 1500,
							}},
						},
					},
				},
			}))
		}()

		desc, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithDescribeOwners(true),
			options.WithDescribeWaiters(true),
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "sem", desc.Name)
		require.EqualValues(t, 2, desc.Count)
		require.EqualValues(t, 5, desc.Limit)
		require.Equal(t, []byte("payload"), desc.Data)
		require.Len(t, desc.Owners, 2)
		require.EqualValues(t, 42, desc.Owners[0].SessionID)
		require.Equal(t, time.Duration(math.MaxInt64), desc.Owners[0].Timeout)
		require.NotNil(t, desc.Owners[1])
		require.Len(t, desc.Waiters, 1)
		require.Equal(t, 1500*time.Millisecond, desc.Waiters[0].Timeout)
		wg.Wait()
	})

	t.Run("AwaitCanceled", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := controller.OnSend(context.Background())
			require.NoError(t, err)
			cancel()
		}()

		_, err := s.DescribeSemaphore(ctx, "sem")
		require.ErrorIs(t, err, context.Canceled)
		wg.Wait()
	})

	t.Run("WatchDataChanged", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		changed := make(chan options.SemaphoreWatchEvent, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()
			require.True(t, req.GetWatchData())
			require.False(t, req.GetWatchOwners())

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status:     Ydb.StatusIds_SUCCESS,
						ReqId:      req.GetReqId(),
						WatchAdded: true,
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name: "sem",
							Data: []byte("before"),
						},
					},
				},
			}))

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
					DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
						ReqId:         req.GetReqId(),
						DataChanged:   true,
						OwnersChanged: true, // ignored: not watching owners
					},
				},
			}))
		}()

		desc, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithSemaphoreWatch(options.WatchData, func(event options.SemaphoreWatchEvent) {
				changed <- event
			}),
		)
		require.NoError(t, err)
		require.Equal(t, []byte("before"), desc.Data)

		select {
		case event := <-changed:
			require.True(t, event.DataChanged)
			require.False(t, event.OwnersChanged)
			require.False(t, event.Lost)
		case <-ctx.Done():
			t.Fatal("timed out waiting for watch event")
		}
		wg.Wait()
	})

	t.Run("WatchOwnersChanged", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		changed := make(chan options.SemaphoreWatchEvent, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()
			require.False(t, req.GetWatchData())
			require.True(t, req.GetWatchOwners())

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status:     Ydb.StatusIds_SUCCESS,
						ReqId:      req.GetReqId(),
						WatchAdded: true,
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name: "sem",
						},
					},
				},
			}))

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
					DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
						ReqId:         req.GetReqId(),
						OwnersChanged: true,
					},
				},
			}))
		}()

		_, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithSemaphoreWatch(options.WatchOwners, func(event options.SemaphoreWatchEvent) {
				changed <- event
			}),
		)
		require.NoError(t, err)

		select {
		case event := <-changed:
			require.True(t, event.OwnersChanged)
			require.False(t, event.DataChanged)
			require.False(t, event.Lost)
		case <-ctx.Done():
			t.Fatal("timed out waiting for watch event")
		}
		wg.Wait()
	})

	t.Run("WatchLostOnFalseWake", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		changed := make(chan options.SemaphoreWatchEvent, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status:     Ydb.StatusIds_SUCCESS,
						ReqId:      req.GetReqId(),
						WatchAdded: true,
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name: "sem",
						},
					},
				},
			}))

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged_{
					DescribeSemaphoreChanged: &Ydb_Coordination.SessionResponse_DescribeSemaphoreChanged{
						ReqId: req.GetReqId(),
					},
				},
			}))
		}()

		_, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithSemaphoreWatch(options.WatchData|options.WatchOwners, func(event options.SemaphoreWatchEvent) {
				changed <- event
			}),
		)
		require.NoError(t, err)

		select {
		case event := <-changed:
			require.True(t, event.Lost)
			require.False(t, event.DataChanged)
			require.False(t, event.OwnersChanged)
		case <-ctx.Done():
			t.Fatal("timed out waiting for lost event")
		}
		wg.Wait()
	})

	t.Run("WatchLostOnControllerClose", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		changed := make(chan options.SemaphoreWatchEvent, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status:     Ydb.StatusIds_SUCCESS,
						ReqId:      req.GetReqId(),
						WatchAdded: true,
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name: "sem",
						},
					},
				},
			}))
			controller.Close(nil)
		}()

		_, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithSemaphoreWatch(options.WatchData, func(event options.SemaphoreWatchEvent) {
				changed <- event
			}),
		)
		require.NoError(t, err)

		select {
		case event := <-changed:
			require.True(t, event.Lost)
		case <-ctx.Done():
			t.Fatal("timed out waiting for abandoned watch")
		}
		wg.Wait()
	})

	t.Run("WatchNotAddedRemovesConversation", func(t *testing.T) {
		controller := conversation.NewController()
		s := &session{controller: controller}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		changed := make(chan options.SemaphoreWatchEvent, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg, err := controller.OnSend(ctx)
			require.NoError(t, err)
			req := msg.GetDescribeSemaphore()

			require.True(t, controller.OnRecv(&Ydb_Coordination.SessionResponse{
				Response: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult_{
					DescribeSemaphoreResult: &Ydb_Coordination.SessionResponse_DescribeSemaphoreResult{
						Status:     Ydb.StatusIds_SUCCESS,
						ReqId:      req.GetReqId(),
						WatchAdded: false,
						SemaphoreDescription: &Ydb_Coordination.SemaphoreDescription{
							Name: "sem",
						},
					},
				},
			}))
		}()

		desc, err := s.DescribeSemaphore(
			ctx,
			"sem",
			options.WithSemaphoreWatch(options.WatchData, func(event options.SemaphoreWatchEvent) {
				changed <- event
			}),
		)
		require.NoError(t, err)
		require.Equal(t, "sem", desc.Name)

		select {
		case event := <-changed:
			t.Fatalf("unexpected watch event: %+v", event)
		case <-time.After(50 * time.Millisecond):
		}
		wg.Wait()
	})

	t.Run("ClosedController", func(t *testing.T) {
		controller := conversation.NewController()
		controller.Close(nil)
		s := &session{controller: controller}

		_, err := s.DescribeSemaphore(context.Background(), "sem")
		require.ErrorIs(t, err, coordination.ErrSessionClosed)
	})
}
