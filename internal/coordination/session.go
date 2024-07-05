package coordination

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/conversation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type session struct {
	options *options.CreateSessionOptions
	client  *Client

	ctx               context.Context //nolint:containedctx
	cancel            context.CancelFunc
	sessionClosedChan chan struct{}
	controller        *conversation.Controller
	sessionID         uint64

	mutex                sync.Mutex // guards the field below
	lastGoodResponseTime time.Time
	cancelStream         context.CancelFunc
}

type lease struct {
	session *session
	name    string
	ctx     context.Context //nolint:containedctx
	cancel  context.CancelFunc
}

func createSession(
	ctx context.Context,
	client *Client,
	path string,
	opts *options.CreateSessionOptions,
) (*session, error) {
	sessionCtx, cancel := xcontext.WithCancel(xcontext.ValueOnly(ctx))
	s := session{
		options:           opts,
		client:            client,
		ctx:               sessionCtx,
		cancel:            cancel,
		sessionClosedChan: make(chan struct{}),
		controller:        conversation.NewController(),
	}
	client.sessionCreated(&s)

	sessionStartedChan := make(chan struct{})
	go s.mainLoop(path, sessionStartedChan)

	select {
	case <-ctx.Done():
		cancel()

		return nil, ctx.Err()
	case <-sessionStartedChan:
	}

	return &s, nil
}

func newProtectionKey() []byte {
	key := make([]byte, 8)                            //nolint:gomnd
	binary.LittleEndian.PutUint64(key, rand.Uint64()) //nolint:gosec

	return key
}

func newReqID() uint64 {
	return rand.Uint64() //nolint:gosec
}

func (s *session) updateLastGoodResponseTime() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()

	if now.After(s.lastGoodResponseTime) {
		s.lastGoodResponseTime = now
	}
}

func (s *session) getLastGoodResponseTime() time.Time {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.lastGoodResponseTime
}

func (s *session) updateCancelStream(cancel context.CancelFunc) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.cancelStream = cancel
}

// Create a new gRPC stream using an independent context.
//
//nolint:funlen
func (s *session) newStream(
	streamCtx context.Context,
	cancelStream context.CancelFunc,
) (Ydb_Coordination_V1.CoordinationService_SessionClient, error) {
	// This deadline if final. If we have not got a session before it, the session is either expired or has never been
	// created.
	var deadline time.Time
	if s.sessionID != 0 {
		deadline = s.getLastGoodResponseTime().Add(s.options.SessionTimeout)
	} else {
		// Large enough to make the loop infinite, small enough to allow the maximum duration value (~290 years).
		deadline = time.Now().Add(time.Hour * 24 * 365 * 100) //nolint:gomnd
	}

	lastChance := false
	for {
		result := make(chan Ydb_Coordination_V1.CoordinationService_SessionClient, 1)
		go func() {
			var err error
			onDone := trace.CoordinationOnStreamNew(s.client.config.Trace())
			defer func() {
				onDone(err)
			}()

			client, err := s.client.client.Session(streamCtx)
			result <- client
		}()

		var client Ydb_Coordination_V1.CoordinationService_SessionClient
		if lastChance {
			timer := time.NewTimer(s.options.SessionKeepAliveTimeout)
			select {
			case <-timer.C:
			case client = <-result:
			}
			timer.Stop()

			if client != nil {
				return client, nil
			}

			cancelStream()

			return nil, s.ctx.Err()
		}

		// Since the deadline is probably large enough, avoid the timer leak with time.After.
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-s.ctx.Done():
		case client = <-result:
		case <-timer.C:
			trace.CoordinationOnSessionClientTimeout(
				s.client.config.Trace(),
				s.getLastGoodResponseTime(),
				s.options.SessionTimeout,
			)
			cancelStream()

			return nil, coordination.ErrSessionClosed
		}
		timer.Stop()

		if client != nil {
			return client, nil
		}

		// Waiting for some time before trying to reconnect.
		sessionReconnectDelay := time.NewTimer(s.options.SessionReconnectDelay)
		select {
		case <-sessionReconnectDelay.C:
		case <-s.ctx.Done():
		}
		sessionReconnectDelay.Stop()

		if s.ctx.Err() != nil {
			// Give this session the last chance to stop gracefully if the session is canceled in the reconnect cycle.
			if s.sessionID != 0 {
				lastChance = true
			} else {
				cancelStream()

				return nil, s.ctx.Err()
			}
		}
	}
}

//nolint:funlen
func (s *session) mainLoop(path string, sessionStartedChan chan struct{}) {
	defer s.client.sessionClosed(s)
	defer close(s.sessionClosedChan)
	defer s.cancel()

	var seqNo uint64

	protectionKey := newProtectionKey()
	closing := false

	for {
		// Create a new grpc stream and start the receiver and sender loops.
		//
		// We use the stream context as a way to inform the main loop that the session must be reconnected if an
		// unrecoverable error occurs in the receiver or sender loop. This also helps stop the other loop if an error
		// is caught on only one of them.
		//
		// We intentionally place a stream context outside the scope of any existing contexts to make an attempt to
		// close the session gracefully at the end of the main loop.

		streamCtx, cancelStream := context.WithCancel(context.Background())
		sessionClient, err := s.newStream(streamCtx, cancelStream)
		if err != nil {
			// Giving up, we can do nothing without a stream.
			s.controller.Close(nil)

			return
		}

		s.updateCancelStream(cancelStream)

		// Start the loops.
		wg := sync.WaitGroup{}
		wg.Add(2) //nolint:gomnd
		sessionStarted := make(chan *Ydb_Coordination.SessionResponse_SessionStarted, 1)
		sessionStopped := make(chan *Ydb_Coordination.SessionResponse_SessionStopped, 1)
		startSending := make(chan struct{})
		s.controller.OnAttach()

		go s.receiveLoop(&wg, sessionClient, cancelStream, sessionStarted, sessionStopped)
		go s.sendLoop(
			&wg,
			sessionClient,
			streamCtx,
			cancelStream,
			startSending,
			path,
			protectionKey,
			s.sessionID,
			seqNo,
		)

		// Wait for the session started response unless the stream context is done. We intentionally do not take into
		// account stream context cancellation in order to proceed with the graceful shutdown if it requires reconnect.
		sessionStartTimer := time.NewTimer(s.options.SessionStartTimeout)
		select {
		case start := <-sessionStarted:
			trace.CoordinationOnSessionStarted(s.client.config.Trace(), start.GetSessionId(), s.sessionID)
			if s.sessionID == 0 {
				s.sessionID = start.GetSessionId()
				close(sessionStartedChan)
			} else if start.GetSessionId() != s.sessionID {
				// Reconnect if the server response is invalid.
				cancelStream()
			}
			close(startSending)
		case <-sessionStartTimer.C:
			// Reconnect if no response was received before the timeout occurred.
			trace.CoordinationOnSessionStartTimeout(s.client.config.Trace(), s.options.SessionStartTimeout)
			cancelStream()
		case <-streamCtx.Done():
		case <-s.ctx.Done():
		}
		sessionStartTimer.Stop()

		for {
			// Respect the failure reason priority: if the session context is done, we must stop the session, even
			// though the stream context may also be canceled.
			if s.ctx.Err() != nil {
				closing = true

				break
			}
			if streamCtx.Err() != nil {
				// Reconnect if an error occurred during the start session conversation.
				break
			}

			keepAliveTime := time.Until(s.getLastGoodResponseTime().Add(s.options.SessionKeepAliveTimeout))
			keepAliveTimeTimer := time.NewTimer(keepAliveTime)
			select {
			case <-keepAliveTimeTimer.C:
				last := s.getLastGoodResponseTime()
				if time.Since(last) > s.options.SessionKeepAliveTimeout {
					// Reconnect if the underlying stream is likely to be dead.
					trace.CoordinationOnSessionKeepAliveTimeout(
						s.client.config.Trace(),
						last,
						s.options.SessionKeepAliveTimeout,
					)
					cancelStream()
				}
			case <-streamCtx.Done():
			case <-s.ctx.Done():
			}
			keepAliveTimeTimer.Stop()
		}

		if closing {
			// No need to stop the session if it was not started.
			if s.sessionID == 0 {
				s.controller.Close(nil)
				cancelStream()

				return
			}

			trace.CoordinationOnSessionStop(s.client.config.Trace(), s.sessionID)
			s.controller.Close(conversation.NewConversation(
				func() *Ydb_Coordination.SessionRequest {
					return &Ydb_Coordination.SessionRequest{
						Request: &Ydb_Coordination.SessionRequest_SessionStop_{
							SessionStop: &Ydb_Coordination.SessionRequest_SessionStop{},
						},
					}
				}),
			)

			// Wait for the session stopped response unless the stream context is done.
			sessionStopTimeout := time.NewTimer(s.options.SessionStopTimeout)
			select {
			case stop := <-sessionStopped:
				sessionStopTimeout.Stop()
				trace.CoordinationOnSessionStopped(s.client.config.Trace(), stop.GetSessionId(), s.sessionID)
				if stop.GetSessionId() == s.sessionID {
					cancelStream()

					return
				}

				// Reconnect if the server response is invalid.
				cancelStream()
			case <-sessionStopTimeout.C:
				sessionStopTimeout.Stop() // no really need, call stop for common style only

				// Reconnect if no response was received before the timeout occurred.
				trace.CoordinationOnSessionStopTimeout(s.client.config.Trace(), s.options.SessionStopTimeout)
				cancelStream()
			case <-s.ctx.Done():
				sessionStopTimeout.Stop()
				cancelStream()

				return
			case <-streamCtx.Done():
				sessionStopTimeout.Stop()
			}
		}

		// Make sure no one is processing the stream anymore.
		wg.Wait()

		s.controller.OnDetach()
		seqNo++
	}
}

//nolint:funlen
func (s *session) receiveLoop(
	wg *sync.WaitGroup,
	sessionClient Ydb_Coordination_V1.CoordinationService_SessionClient,
	cancelStream context.CancelFunc,
	sessionStarted chan *Ydb_Coordination.SessionResponse_SessionStarted,
	sessionStopped chan *Ydb_Coordination.SessionResponse_SessionStopped,
) {
	// If the sendLoop is done, make sure the stream is also canceled to make the receiveLoop finish its work and cause
	// reconnect.
	defer wg.Done()
	defer cancelStream()

	for {
		onDone := trace.CoordinationOnSessionReceive(s.client.config.Trace())
		message, err := sessionClient.Recv()
		if err != nil {
			// Any stream error is unrecoverable, try to reconnect.
			onDone(nil, err)

			return
		}
		onDone(message, nil)

		switch message.GetResponse().(type) {
		case *Ydb_Coordination.SessionResponse_Failure_:
			if message.GetFailure().GetStatus() == Ydb.StatusIds_SESSION_EXPIRED ||
				message.GetFailure().GetStatus() == Ydb.StatusIds_UNAUTHORIZED ||
				message.GetFailure().GetStatus() == Ydb.StatusIds_NOT_FOUND {
				// Consider the session expired if we got an unrecoverable status.
				trace.CoordinationOnSessionServerExpire(s.client.config.Trace(), message.GetFailure())

				return
			}

			trace.CoordinationOnSessionServerError(s.client.config.Trace(), message.GetFailure())

			return
		case *Ydb_Coordination.SessionResponse_SessionStarted_:
			sessionStarted <- message.GetSessionStarted()
			s.updateLastGoodResponseTime()
		case *Ydb_Coordination.SessionResponse_SessionStopped_:
			sessionStopped <- message.GetSessionStopped()
			s.cancel()

			return
		case *Ydb_Coordination.SessionResponse_Ping:
			opaque := message.GetPing().GetOpaque()
			err := s.controller.PushFront(conversation.NewConversation(
				func() *Ydb_Coordination.SessionRequest {
					return &Ydb_Coordination.SessionRequest{
						Request: &Ydb_Coordination.SessionRequest_Pong{
							Pong: &Ydb_Coordination.SessionRequest_PingPong{
								Opaque: opaque,
							},
						},
					}
				}),
			)
			if err != nil {
				// The session is closed if we cannot send the pong request back, so just exit the loop.
				return
			}
			s.updateLastGoodResponseTime()
		case *Ydb_Coordination.SessionResponse_Pong:
			// Ignore pongs since we do not ping the server.
		default:
			if !s.controller.OnRecv(message) {
				// Reconnect if the message is not from any known conversation.
				trace.CoordinationOnSessionReceiveUnexpected(s.client.config.Trace(), message)

				return
			}

			s.updateLastGoodResponseTime()
		}
	}
}

//nolint:revive,funlen
func (s *session) sendLoop(
	wg *sync.WaitGroup,
	sessionClient Ydb_Coordination_V1.CoordinationService_SessionClient,
	streamCtx context.Context,
	cancelStream context.CancelFunc,
	startSending chan struct{},
	path string,
	protectionKey []byte,
	sessionID uint64,
	seqNo uint64,
) {
	// If the sendLoop is done, make sure the stream is also canceled to make the receiveLoop finish its work and cause
	// reconnect.
	defer wg.Done()
	defer cancelStream()

	// Start a new session.
	onDone := trace.CoordinationOnSessionStart(s.client.config.Trace())
	startSession := Ydb_Coordination.SessionRequest{
		Request: &Ydb_Coordination.SessionRequest_SessionStart_{
			SessionStart: &Ydb_Coordination.SessionRequest_SessionStart{
				Path:          path,
				SessionId:     sessionID,
				TimeoutMillis: uint64(s.options.SessionTimeout.Milliseconds()),
				ProtectionKey: protectionKey,
				SeqNo:         seqNo,
				Description:   s.options.Description,
			},
		},
	}
	err := sessionClient.Send(&startSession)
	if err != nil {
		// Reconnect if a session cannot be started in this stream.
		onDone(err)

		return
	}
	onDone(nil)

	// Wait for a response to the session start request in order to carry over the accumulated conversations until the
	// server confirms that the session is running. This is not absolutely necessary but helps the client to not fail
	// non-idempotent requests in case of the session handshake errors.
	select {
	case <-streamCtx.Done():
	case <-startSending:
	}

	for {
		message, err := s.controller.OnSend(streamCtx)
		if err != nil {
			return
		}

		onSendDone := trace.CoordinationOnSessionSend(s.client.config.Trace(), message)
		err = sessionClient.Send(message)
		if err != nil {
			// Any stream error is unrecoverable, try to reconnect.
			onSendDone(err)

			return
		}
		onSendDone(nil)
	}
}

func (s *session) Context() context.Context {
	return s.ctx
}

func (s *session) Close(ctx context.Context) error {
	s.cancel()

	select {
	case <-s.sessionClosedChan:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *session) Reconnect() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.cancelStream != nil {
		s.cancelStream()
	}
}

func (s *session) SessionID() uint64 {
	return s.sessionID
}

func (s *session) CreateSemaphore(
	ctx context.Context,
	name string,
	limit uint64,
	opts ...options.CreateSemaphoreOption,
) error {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			createSemaphore := Ydb_Coordination.SessionRequest_CreateSemaphore{
				ReqId: newReqID(),
				Name:  name,
				Limit: limit,
			}
			for _, o := range opts {
				if o != nil {
					o(&createSemaphore)
				}
			}

			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_CreateSemaphore_{
					CreateSemaphore: &createSemaphore,
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetCreateSemaphoreResult().GetReqId() == request.GetCreateSemaphore().GetReqId()
		}),
	)
	if err := s.controller.PushBack(req); err != nil {
		return err
	}

	_, err := s.controller.Await(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *session) UpdateSemaphore(
	ctx context.Context,
	name string,
	opts ...options.UpdateSemaphoreOption,
) error {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			updateSemaphore := Ydb_Coordination.SessionRequest_UpdateSemaphore{
				ReqId: newReqID(),
				Name:  name,
			}
			for _, o := range opts {
				if o != nil {
					o(&updateSemaphore)
				}
			}

			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_UpdateSemaphore_{
					UpdateSemaphore: &updateSemaphore,
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetUpdateSemaphoreResult().GetReqId() == request.GetUpdateSemaphore().GetReqId()
		}),
		conversation.WithConflictKey(name),
		conversation.WithIdempotence(true),
	)
	if err := s.controller.PushBack(req); err != nil {
		return err
	}

	_, err := s.controller.Await(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *session) DeleteSemaphore(
	ctx context.Context,
	name string,
	opts ...options.DeleteSemaphoreOption,
) error {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			deleteSemaphore := Ydb_Coordination.SessionRequest_DeleteSemaphore{
				ReqId: newReqID(),
				Name:  name,
			}
			for _, o := range opts {
				if o != nil {
					o(&deleteSemaphore)
				}
			}

			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_DeleteSemaphore_{
					DeleteSemaphore: &deleteSemaphore,
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetDeleteSemaphoreResult().GetReqId() == request.GetDeleteSemaphore().GetReqId()
		}),
		conversation.WithConflictKey(name),
	)
	if err := s.controller.PushBack(req); err != nil {
		return err
	}

	_, err := s.controller.Await(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *session) DescribeSemaphore(
	ctx context.Context,
	name string,
	opts ...options.DescribeSemaphoreOption,
) (*coordination.SemaphoreDescription, error) {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			describeSemaphore := Ydb_Coordination.SessionRequest_DescribeSemaphore{
				ReqId: newReqID(),
				Name:  name,
			}
			for _, o := range opts {
				if o != nil {
					o(&describeSemaphore)
				}
			}

			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_DescribeSemaphore_{
					DescribeSemaphore: &describeSemaphore,
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetDescribeSemaphoreResult().GetReqId() == request.GetDescribeSemaphore().GetReqId()
		}),
		conversation.WithConflictKey(name),
		conversation.WithIdempotence(true),
	)
	if err := s.controller.PushBack(req); err != nil {
		return nil, err
	}

	resp, err := s.controller.Await(ctx, req)
	if err != nil {
		return nil, err
	}

	return convertSemaphoreDescription(resp.GetDescribeSemaphoreResult().GetSemaphoreDescription()), nil
}

func convertSemaphoreDescription(
	desc *Ydb_Coordination.SemaphoreDescription,
) *coordination.SemaphoreDescription {
	var result coordination.SemaphoreDescription

	if desc != nil {
		result.Name = desc.GetName()
		result.Limit = desc.GetLimit()
		result.Ephemeral = desc.GetEphemeral()
		result.Count = desc.GetCount()
		result.Data = desc.GetData()
		result.Owners = convertSemaphoreSessions(desc.GetOwners())
		result.Waiters = convertSemaphoreSessions(desc.GetWaiters())
	}

	return &result
}

func convertSemaphoreSessions(
	sessions []*Ydb_Coordination.SemaphoreSession,
) []*coordination.SemaphoreSession {
	if sessions == nil {
		return nil
	}

	result := make([]*coordination.SemaphoreSession, len(sessions))
	for i, s := range sessions {
		result[i] = convertSemaphoreSession(s)
	}

	return result
}

func convertSemaphoreSession(
	session *Ydb_Coordination.SemaphoreSession,
) *coordination.SemaphoreSession {
	var result coordination.SemaphoreSession

	if session != nil {
		result.SessionID = session.GetSessionId()
		result.Count = session.GetCount()
		result.OrderID = session.GetOrderId()
		result.Data = session.GetData()
		if session.GetTimeoutMillis() == math.MaxUint64 {
			result.Timeout = time.Duration(math.MaxInt64)
		} else {
			// The service does not allow big timeout values, so the conversion seems to be safe.
			result.Timeout = time.Duration(session.GetTimeoutMillis()) * time.Millisecond
		}
	}

	return &result
}

//nolint:funlen
func (s *session) AcquireSemaphore(
	ctx context.Context,
	name string,
	count uint64,
	opts ...options.AcquireSemaphoreOption,
) (coordination.Lease, error) {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			acquireSemaphore := Ydb_Coordination.SessionRequest_AcquireSemaphore{
				ReqId:         newReqID(),
				Name:          name,
				Count:         count,
				TimeoutMillis: math.MaxUint64,
			}
			for _, o := range opts {
				if o != nil {
					o(&acquireSemaphore)
				}
			}

			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_AcquireSemaphore_{
					AcquireSemaphore: &acquireSemaphore,
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetAcquireSemaphoreResult().GetReqId() == request.GetAcquireSemaphore().GetReqId()
		}),
		conversation.WithAcknowledgeFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetAcquireSemaphorePending().GetReqId() == request.GetAcquireSemaphore().GetReqId()
		}),
		conversation.WithCancelMessage(
			func(request *Ydb_Coordination.SessionRequest) *Ydb_Coordination.SessionRequest {
				return &Ydb_Coordination.SessionRequest{
					Request: &Ydb_Coordination.SessionRequest_ReleaseSemaphore_{
						ReleaseSemaphore: &Ydb_Coordination.SessionRequest_ReleaseSemaphore{
							Name:  name,
							ReqId: newReqID(),
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
		conversation.WithConflictKey(name),
		conversation.WithIdempotence(true),
	)
	if err := s.controller.PushBack(req); err != nil {
		return nil, err
	}

	resp, err := s.controller.Await(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.GetAcquireSemaphoreResult().GetAcquired() {
		return nil, coordination.ErrAcquireTimeout
	}

	ctx, cancel := context.WithCancel(s.ctx)

	return &lease{
		session: s,
		name:    name,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (l *lease) Context() context.Context {
	return l.ctx
}

func (l *lease) Release() error {
	req := conversation.NewConversation(
		func() *Ydb_Coordination.SessionRequest {
			return &Ydb_Coordination.SessionRequest{
				Request: &Ydb_Coordination.SessionRequest_ReleaseSemaphore_{
					ReleaseSemaphore: &Ydb_Coordination.SessionRequest_ReleaseSemaphore{
						ReqId: newReqID(),
						Name:  l.name,
					},
				},
			}
		},
		conversation.WithResponseFilter(func(
			request *Ydb_Coordination.SessionRequest,
			response *Ydb_Coordination.SessionResponse,
		) bool {
			return response.GetReleaseSemaphoreResult().GetReqId() == request.GetReleaseSemaphore().GetReqId()
		}),
		conversation.WithConflictKey(l.name),
		conversation.WithIdempotence(true),
	)
	if err := l.session.controller.PushBack(req); err != nil {
		return err
	}

	_, err := l.session.controller.Await(l.session.ctx, req)
	if err != nil {
		return err
	}

	l.cancel()

	return nil
}

func (l *lease) Session() coordination.Session {
	return l.session
}
