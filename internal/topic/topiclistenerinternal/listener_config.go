package topiclistenerinternal

import (
	"errors"
	"fmt"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type StreamListenerConfig struct {
	RetrySettings          topic.RetrySettings
	BufferSize             int
	Decoders               *topicreadercommon.MultiDecoder
	Selectors              []*topicreadercommon.PublicReadSelector
	Consumer               string
	ConnectWithoutConsumer bool
	Tracer                 *trace.Topic

	clock    clockwork.Clock
	readerID int64
}

func NewStreamListenerConfig() StreamListenerConfig {
	return StreamListenerConfig{
		RetrySettings: topic.RetrySettings{StartTimeout: topic.DefaultStartTimeout},
		clock:         clockwork.NewRealClock(),
		BufferSize:    topicreadercommon.DefaultBufferSize,
		Decoders:      topicreadercommon.NewMultiDecoder(),
		Selectors:     nil,
		Consumer:      "",
		readerID:      topicreadercommon.NextReaderID(),
		Tracer:        &trace.Topic{},
	}
}

func (cfg *StreamListenerConfig) Validate() error {
	var errs []error
	if cfg.Consumer == "" && !cfg.ConnectWithoutConsumer {
		errs = append(errs, errors.New(
			"empty consumer without ConnectWithoutConsumer flag. Set the consumer or  the flag",
		))
	}
	if cfg.Consumer != "" && cfg.ConnectWithoutConsumer {
		errs = append(errs, errors.New(
			"non empty consumer, but ConnectWithoutConsumer flag set. Clear the consumer or the flag",
		))
	}
	if len(cfg.Selectors) == 0 {
		errs = append(errs, errors.New("topic selectors are empty"))
	}
	if cfg.BufferSize <= 0 {
		errs = append(errs, fmt.Errorf(
			"buffer size of the topic listener should be greater then 0, now: %v",
			cfg.BufferSize,
		))
	}

	if len(errs) > 0 {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: topic listener config validation failed: %w",
			errors.Join(errs...),
		)))
	}

	return nil
}
