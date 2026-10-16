package research_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cucumber/godog"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

type topicDescriptionSample struct {
	started  time.Time
	finished time.Time
	result   *Ydb_Topic.DescribeTopicResult
}

type splitObservation struct {
	started        time.Time
	alterFinished  time.Time
	alterResponse  *Ydb_Topic.AlterTopicResponse
	samples        []topicDescriptionSample
	writeFailed    time.Time
	writeResponse  *Ydb_Topic.StreamWriteMessage_FromServer
	sampleInterval time.Duration
}

func initializeSplitObservationSteps(sc *godog.ScenarioContext) {
	sc.Step(streamWriteStepPrefix+`observe server responses for (\d+)ms$`, stepObserveWriteResponses)
	sc.Step(`^TopicService\.AlterTopic: AlterTopicRequest\{(.*)\} `+
		`while probing StreamWrite "([^"]+)" and sampling DescribeTopic every (\d+)ms$`,
		stepAlterWhileDescribing)
}

func stepObserveWriteResponses(ctx context.Context, name string, milliseconds int) error {
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	window, cancel := context.WithTimeout(ctx, time.Duration(milliseconds)*time.Millisecond)
	defer cancel()
	for {
		if _, err := session.receive(window); err != nil {
			session.observe(fmt.Sprintf("StreamWrite %s observation finished: %v.", session.label(), err))

			return ctx.Err()
		}
	}
}

func stepAlterWhileDescribing(ctx context.Context, parameters, name string, milliseconds int) error {
	if milliseconds <= 0 {
		return errors.New("DescribeTopic sampling interval must be positive")
	}
	research, err := ensureStreamWriteResearch(ctx)
	if err != nil {
		return err
	}
	request, err := parseAlterTopicRequest(parameters, research.world.topicPath)
	if err != nil {
		return err
	}
	session, err := writeSessionFromContext(ctx, name)
	if err != nil {
		return err
	}
	client := Ydb_Topic_V1.NewTopicServiceClient(research.world.conn)
	observation := &splitObservation{started: time.Now(), sampleInterval: time.Duration(milliseconds) * time.Millisecond}
	research.splitObservation = observation
	observeCtx, cancel := context.WithCancel(ctx)
	type alterResult struct {
		response *Ydb_Topic.AlterTopicResponse
		finished time.Time
		err      error
	}
	alterDone := make(chan alterResult, 1)
	go func() {
		method := Ydb_Topic_V1.TopicService_AlterTopic_FullMethodName
		research.observeProto("→", method, request)
		response, alterErr := client.AlterTopic(observeCtx, request)
		finished := time.Now()
		if alterErr == nil {
			research.observeProto("←", method, response)
		}
		alterDone <- alterResult{response: response, finished: finished, err: alterErr}
	}()
	// Always join the RPC before returning, including on a failed DescribeTopic.
	defer func() {
		cancel()
		if alterDone != nil {
			<-alterDone
		}
	}()
	ticker := time.NewTicker(time.Duration(milliseconds) * time.Millisecond)
	defer ticker.Stop()
	var sequence int64
	for {
		alterPending := observation.alterFinished.IsZero()
		operation := observation.alterResponse.GetOperation()
		if observation.writeFailed.IsZero() &&
			(alterPending || (operation.GetReady() && operation.GetStatus() == Ydb.StatusIds_SUCCESS)) {
			sequence++
			if err := probeSplitWrite(observeCtx, session, observation, sequence); err != nil {
				return err
			}
		}
		sample, err := research.sampleTopicDescription(observeCtx, observation)
		if err != nil {
			return err
		}
		// A successful AlterTopic need not split the partition or reject its writer.
		// Keep the samples taken during Alter, then finish after one final probe and
		// a fresh description. Contracts check rejection and eventual children separately.
		if !alterPending && sample.started.After(observation.alterFinished) {
			research.observe(fmt.Sprintf(
				"Write probing finished after AlterTopic and a fresh DescribeTopic: probes=%d, writer_rejected=%t.",
				sequence, !observation.writeFailed.IsZero()))

			return nil
		}
		select {
		case result := <-alterDone:
			alterDone = nil
			if result.err != nil {
				return result.err
			}
			observation.alterFinished, observation.alterResponse = result.finished, result.response
			research.lastAlterResponse = result.response
			research.observe(fmt.Sprintf("AlterTopic completed at +%s.", result.finished.Sub(observation.started)))
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func probeSplitWrite(
	ctx context.Context, session *streamWriteSession, observation *splitObservation, sequence int64,
) error {
	sendErr := session.send(ctx, nonTransactionalWriteRequest("split-probe", &sequence))
	response, receiveErr := session.receive(ctx)
	if response != nil && response.GetStatus() != Ydb.StatusIds_SUCCESS {
		observation.writeFailed, observation.writeResponse = time.Now(), response
		session.observe(fmt.Sprintf("StreamWrite %s rejected a probe at +%s: %s.",
			session.label(), observation.writeFailed.Sub(observation.started), response.GetStatus()))

		return nil
	}

	return errors.Join(sendErr, receiveErr)
}

func (r *streamWriteResearch) sampleTopicDescription(
	ctx context.Context, observation *splitObservation,
) (topicDescriptionSample, error) {
	sample := topicDescriptionSample{started: time.Now()}
	client := Ydb_Topic_V1.NewTopicServiceClient(r.world.conn)
	response, err := client.DescribeTopic(ctx, &Ydb_Topic.DescribeTopicRequest{
		Path:            r.world.topicPath,
		OperationParams: &Ydb_Operations.OperationParams{OperationMode: Ydb_Operations.OperationParams_SYNC},
	})
	sample.finished = time.Now()
	if err != nil {
		return sample, err
	}
	operation := response.GetOperation()
	if !operation.GetReady() || operation.GetStatus() != Ydb.StatusIds_SUCCESS {
		return sample, fmt.Errorf("DescribeTopic observation failed: %v", operation)
	}
	sample.result = &Ydb_Topic.DescribeTopicResult{}
	if err := operation.GetResult().UnmarshalTo(sample.result); err != nil {
		return sample, err
	}
	observation.samples = append(observation.samples, sample)
	r.observe(fmt.Sprintf("DescribeTopic sample started at +%s, finished at +%s: partitions=%v.",
		sample.started.Sub(observation.started), sample.finished.Sub(observation.started), sample.result.GetPartitions()))

	return sample, nil
}
