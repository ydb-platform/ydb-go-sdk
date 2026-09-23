package partition

import (
	"context"
	"errors"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

var errReplacementNotPublished = errors.New("partition replacement was not published")

// TopicDescriber loads topic metadata without depending on client or writer options.
type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

// Source caches metadata for one topic and is shared by that topic's writers in one client.
// Obtain a Source through Sources.Get; its zero value is not usable.
// Cached metadata has no time-based expiration or periodic refresh.
// Reloads are triggered by explicit invalidation or session errors that may indicate a topology change.
// A caller may await a reported partition replacement before retrying work against the refreshed topology.
type Source struct {
	topicPath            string
	describe             TopicDescriber
	partitions           *Partitions
	partitionsLoad       *partitionsLoad
	replacementRefreshes map[int64]*replacementRefresh
	subscriptions        map[*subscription]struct{}
	mu                   sync.Mutex
}

type replacementRefresh struct {
	ctx       context.Context //nolint:containedctx // Shared refresh lifetime context.
	cancel    context.CancelFunc
	done      chan struct{}
	reporters int
	err       error
}

type partitionsLoad struct {
	done        chan struct{}
	invalidated bool
	err         error
}

// NewRouter initializes a chooser from this source and subscribes it to subsequent metadata updates.
// The context controls initialization, route change waiting, and the subscription lifetime.
func (s *Source) NewRouter(ctx context.Context, chooser Chooser) (*Router, error) {
	partitions, err := s.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	if chooser != nil {
		if err := chooser.AddNewPartitions(partitions.activeInfos()...); err != nil {
			return nil, err
		}
	}

	router := &Router{
		ctx:        ctx,
		chooser:    chooser,
		partitions: partitions,
	}
	subscription := newSubscription(ctx, router)
	router.subscription = subscription
	if err = s.subscribe(subscription, partitions); err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		s.unsubscribe(subscription)
	}()

	return router, nil
}

// Partitions returns the current read-only topology snapshot.
// A previously returned snapshot is not updated when a newer topology is published.
func (s *Source) Partitions(ctx context.Context) (*Partitions, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		partitions, load, created := s.cachedPartitionsOrLoad()
		if partitions != nil {
			return partitions, nil
		}
		if !created {
			select {
			case <-load.done:
				if load.err != nil && !isContextError(load.err) {
					return nil, load.err
				}

				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		description, err := s.describe(ctx, s.topicPath)
		partitions = partitionsFromDescription(description)
		subscriptions, publish := s.beginPartitionsPublication(load, err)
		if err != nil {
			return nil, err
		}
		if !publish {
			continue
		}
		for _, subscription := range subscriptions {
			previous, updateErr := subscription.router.updatePartitions(partitions)
			if updateErr != nil {
				subscription.push(topologyEvent{err: updateErr})

				continue
			}
			subscription.notifyReplacements(previous, partitions)
		}
		if s.finishPartitionsPublication(load, partitions) {
			return partitions, nil
		}
	}
}

// NotifySessionError reports a session failure without waiting for network I/O or subscriber callbacks.
// It accepts only OVERLOADED errors containing the WRITE_ERROR_PARTITION_INACTIVE issue (500029).
// A non-nil result means topology handling accepted the error; the caller must stop using that failed session.
// The returned function waits for the shared Source topology update and returns its error. An error updating an
// individual Router is delivered only through that Router's WaitForRouteChange method, because other Routers
// sharing this Source may have updated successfully. A nil result leaves the error to the caller's usual error or
// retry policy.
func (s *Source) NotifySessionError(
	ctx context.Context,
	partitionID int64,
	err error,
) (waitForReplacement func(context.Context) error) {
	if ctx.Err() != nil || !xerrors.IsOperationErrorTopicPartitionInactive(err) {
		return nil
	}
	refresh, created := s.addReplacementReporter(partitionID)
	if refresh == nil {
		return func(context.Context) error { return nil }
	}
	go s.watchReplacementReporter(ctx, refresh)
	if created {
		go s.refreshReplacement(partitionID, refresh)
	}

	return func(ctx context.Context) error {
		return waitForReplacementRefresh(ctx, refresh)
	}
}

// Invalidate marks this topic's cached metadata for reload without doing network I/O.
func (s *Source) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partitions = nil
	if s.partitionsLoad != nil {
		s.partitionsLoad.invalidated = true
	}
}

func (s *Source) subscribe(sub *subscription, initialized *Partitions) error {
	sub.router.mu.Lock()
	defer sub.router.mu.Unlock()

	s.mu.Lock()
	s.subscriptions[sub] = struct{}{}
	current := s.partitions
	s.mu.Unlock()
	if current == nil || current == initialized {
		return nil
	}
	previous, err := sub.router.updatePartitionsNeedLock(current)
	if err != nil {
		s.unsubscribe(sub)

		return err
	}
	sub.notifyReplacements(previous, current)

	return nil
}

func (s *Source) unsubscribe(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.subscriptions, sub)
}

func (s *Source) cachedPartitionsOrLoad() (partitions *Partitions, load *partitionsLoad, created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.partitions != nil {
		return s.partitions, nil, false
	}
	if s.partitionsLoad != nil {
		return nil, s.partitionsLoad, false
	}
	load = &partitionsLoad{done: make(chan struct{})}
	s.partitionsLoad = load

	return nil, load, true
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func partitionsFromDescription(description topictypes.TopicDescription) *Partitions {
	partitions := &Partitions{
		all:  make(List, 0, len(description.Partitions)),
		byID: make(map[int64]Partition, len(description.Partitions)),
	}
	for _, partition := range description.Partitions {
		topicPartition := Partition{
			info:       partition,
			partitions: partitions,
		}
		partitions.all = append(partitions.all, topicPartition)
		partitions.byID[topicPartition.ID()] = topicPartition
	}

	return partitions
}

func (s *Source) beginPartitionsPublication(load *partitionsLoad, err error) (
	subscriptions []*subscription,
	publish bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	load.err = err
	if err != nil || load.invalidated {
		s.finishPartitionsLoadNeedLock(load)

		return nil, false
	}

	return s.subscriptionsNeedLock(), true
}

func (s *Source) finishPartitionsPublication(load *partitionsLoad, partitions *Partitions) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !load.invalidated {
		s.partitions = partitions
	}
	s.finishPartitionsLoadNeedLock(load)

	return !load.invalidated
}

func (s *Source) finishPartitionsLoadNeedLock(load *partitionsLoad) {
	s.partitionsLoad = nil
	close(load.done)
}

func (s *Source) addReplacementReporter(partitionID int64) (refresh *replacementRefresh, created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if replacementPublished(s.partitions, partitionID) {
		return nil, false
	}
	if refresh, ok := s.replacementRefreshes[partitionID]; ok {
		select {
		case <-refresh.done:
			delete(s.replacementRefreshes, partitionID)
		default:
			refresh.reporters++

			return refresh, false
		}
	}
	if s.replacementRefreshes == nil {
		s.replacementRefreshes = make(map[int64]*replacementRefresh)
	}
	refreshCtx, cancel := context.WithCancel(context.Background())
	refresh = &replacementRefresh{ctx: refreshCtx, cancel: cancel, done: make(chan struct{}), reporters: 1}
	s.replacementRefreshes[partitionID] = refresh
	s.partitions = nil

	return refresh, true
}

func replacementPublished(partitions *Partitions, partitionID int64) bool {
	if partitions == nil {
		return false
	}
	parent, ok := partitions.find(partitionID)
	if !ok || parent.IsActive() || len(parent.info.ChildPartitionIDs) == 0 {
		return false
	}
	path := map[int64]struct{}{partitionID: {}}
	for _, childID := range parent.info.ChildPartitionIDs {
		if !replacementBranchPublished(partitions, childID, path) {
			return false
		}
	}

	return true
}

func replacementBranchPublished(partitions *Partitions, partitionID int64, path map[int64]struct{}) bool {
	partition, ok := partitions.find(partitionID)
	if !ok {
		return false
	}
	if partition.IsActive() {
		return true
	}
	if len(partition.info.ChildPartitionIDs) == 0 {
		return false
	}
	if _, ok = path[partitionID]; ok {
		return false
	}
	path[partitionID] = struct{}{}
	defer delete(path, partitionID)

	for _, childID := range partition.info.ChildPartitionIDs {
		if !replacementBranchPublished(partitions, childID, path) {
			return false
		}
	}

	return true
}

func (s *Source) watchReplacementReporter(ctx context.Context, refresh *replacementRefresh) {
	select {
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		refresh.reporters--
		if refresh.reporters == 0 {
			refresh.cancel()
		}
	case <-refresh.done:
	}
}

func (s *Source) refreshReplacement(partitionID int64, refresh *replacementRefresh) {
	err := retry.Retry(refresh.ctx, func(ctx context.Context) error {
		partitions, err := s.Partitions(ctx)
		if err != nil {
			return err
		}
		if replacementPublished(partitions, partitionID) {
			return nil
		}
		s.Invalidate()

		return retry.RetryableError(errReplacementNotPublished, retry.WithBackoff(retry.TypeFastBackoff))
	}, retry.WithIdempotent(true))
	if err != nil && refresh.ctx.Err() == nil {
		s.failSubscriptions(err)
	}
	s.finishReplacementRefresh(partitionID, refresh, err)
}

func (s *Source) failSubscriptions(err error) {
	s.mu.Lock()
	subscriptions := s.subscriptionsNeedLock()
	s.mu.Unlock()

	for _, subscription := range subscriptions {
		subscription.router.fail(err)
		subscription.push(topologyEvent{err: err})
	}
}

func (s *Source) subscriptionsNeedLock() []*subscription {
	subscriptions := make([]*subscription, 0, len(s.subscriptions))
	for subscription := range s.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}

	return subscriptions
}

func (s *Source) finishReplacementRefresh(partitionID int64, refresh *replacementRefresh, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if current := s.replacementRefreshes[partitionID]; current == refresh {
		delete(s.replacementRefreshes, partitionID)
	}
	refresh.err = err
	close(refresh.done)
	refresh.cancel()
}

func waitForReplacementRefresh(ctx context.Context, refresh *replacementRefresh) error {
	select {
	case <-refresh.done:
		return refresh.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
