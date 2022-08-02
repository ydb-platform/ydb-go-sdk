// Code generated by gtrace. DO NOT EDIT.

package trace

import (
	"context"
)

// topicComposeOptions is a holder of options
type topicComposeOptions struct {
	panicCallback func(e interface{})
}

// TopicOption specified Topic compose option
type TopicComposeOption func(o *topicComposeOptions)

// WithTopicPanicCallback specified behavior on panic
func WithTopicPanicCallback(cb func(e interface{})) TopicComposeOption {
	return func(o *topicComposeOptions) {
		o.panicCallback = cb
	}
}

// Compose returns a new Topic which has functional fields composed both from t and x.
func (t Topic) Compose(x Topic, opts ...TopicComposeOption) (ret Topic) {
	options := topicComposeOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	{
		h1 := t.OnPartitionReadStart
		h2 := x.OnPartitionReadStart
		ret.OnPartitionReadStart = func(o OnPartitionReadStartInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnPartitionReadStop
		h2 := x.OnPartitionReadStop
		ret.OnPartitionReadStop = func(o OnPartitionReadStopInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnPartitionCommittedNotify
		h2 := x.OnPartitionCommittedNotify
		ret.OnPartitionCommittedNotify = func(o OnPartitionCommittedInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnReaderStreamConnect
		h2 := x.OnReaderStreamConnect
		ret.OnReaderStreamConnect = func(o OnReaderStreamConnectStartInfo) func(OnReaderStreamConnectDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(OnReaderStreamConnectDoneInfo)
			if h1 != nil {
				r = h1(o)
			}
			if h2 != nil {
				r1 = h2(o)
			}
			return func(o OnReaderStreamConnectDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(o)
				}
				if r1 != nil {
					r1(o)
				}
			}
		}
	}
	{
		h1 := t.OnReaderStreamClose
		h2 := x.OnReaderStreamClose
		ret.OnReaderStreamClose = func(o OnReaderStreamCloseStartInfo) func(OnReaderStreamCloseDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(OnReaderStreamCloseDoneInfo)
			if h1 != nil {
				r = h1(o)
			}
			if h2 != nil {
				r1 = h2(o)
			}
			return func(o OnReaderStreamCloseDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(o)
				}
				if r1 != nil {
					r1(o)
				}
			}
		}
	}
	{
		h1 := t.OnReadStreamInit
		h2 := x.OnReadStreamInit
		ret.OnReadStreamInit = func(o OnReadStreamInitStartInfo) func(OnReadStreamInitDoneInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			var r, r1 func(OnReadStreamInitDoneInfo)
			if h1 != nil {
				r = h1(o)
			}
			if h2 != nil {
				r1 = h2(o)
			}
			return func(o OnReadStreamInitDoneInfo) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
						}
					}()
				}
				if r != nil {
					r(o)
				}
				if r1 != nil {
					r1(o)
				}
			}
		}
	}
	{
		h1 := t.OnReadStreamError
		h2 := x.OnReadStreamError
		ret.OnReadStreamError = func(o OnReadStreamErrorInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnReadUnknownGrpcMessage
		h2 := x.OnReadUnknownGrpcMessage
		ret.OnReadUnknownGrpcMessage = func(o OnReadUnknownGrpcMessageInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnReadStreamRawReceived
		h2 := x.OnReadStreamRawReceived
		ret.OnReadStreamRawReceived = func(o OnReadStreamRawReceivedInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnReadStreamRawSent
		h2 := x.OnReadStreamRawSent
		ret.OnReadStreamRawSent = func(o OnReadStreamRawSentInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	{
		h1 := t.OnReadStreamUpdateToken
		h2 := x.OnReadStreamUpdateToken
		ret.OnReadStreamUpdateToken = func(o OnReadStreamUpdateTokenInfo) {
			if options.panicCallback != nil {
				defer func() {
					if e := recover(); e != nil {
						options.panicCallback(e)
					}
				}()
			}
			if h1 != nil {
				h1(o)
			}
			if h2 != nil {
				h2(o)
			}
		}
	}
	return ret
}
func (t Topic) onPartitionReadStart(o OnPartitionReadStartInfo) {
	fn := t.OnPartitionReadStart
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onPartitionReadStop(o OnPartitionReadStopInfo) {
	fn := t.OnPartitionReadStop
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onPartitionCommittedNotify(o OnPartitionCommittedInfo) {
	fn := t.OnPartitionCommittedNotify
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onReaderStreamConnect(o OnReaderStreamConnectStartInfo) func(OnReaderStreamConnectDoneInfo) {
	fn := t.OnReaderStreamConnect
	if fn == nil {
		return func(OnReaderStreamConnectDoneInfo) {
			return
		}
	}
	res := fn(o)
	if res == nil {
		return func(OnReaderStreamConnectDoneInfo) {
			return
		}
	}
	return res
}
func (t Topic) onReaderStreamClose(o OnReaderStreamCloseStartInfo) func(OnReaderStreamCloseDoneInfo) {
	fn := t.OnReaderStreamClose
	if fn == nil {
		return func(OnReaderStreamCloseDoneInfo) {
			return
		}
	}
	res := fn(o)
	if res == nil {
		return func(OnReaderStreamCloseDoneInfo) {
			return
		}
	}
	return res
}
func (t Topic) onReadStreamInit(o OnReadStreamInitStartInfo) func(OnReadStreamInitDoneInfo) {
	fn := t.OnReadStreamInit
	if fn == nil {
		return func(OnReadStreamInitDoneInfo) {
			return
		}
	}
	res := fn(o)
	if res == nil {
		return func(OnReadStreamInitDoneInfo) {
			return
		}
	}
	return res
}
func (t Topic) onReadStreamError(o OnReadStreamErrorInfo) {
	fn := t.OnReadStreamError
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onReadUnknownGrpcMessage(o OnReadUnknownGrpcMessageInfo) {
	fn := t.OnReadUnknownGrpcMessage
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onReadStreamRawReceived(o OnReadStreamRawReceivedInfo) {
	fn := t.OnReadStreamRawReceived
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onReadStreamRawSent(o OnReadStreamRawSentInfo) {
	fn := t.OnReadStreamRawSent
	if fn == nil {
		return
	}
	fn(o)
}
func (t Topic) onReadStreamUpdateToken(o OnReadStreamUpdateTokenInfo) {
	fn := t.OnReadStreamUpdateToken
	if fn == nil {
		return
	}
	fn(o)
}
func TopicOnPartitionReadStart(t Topic, readerConnectionID string, partitionContext context.Context, topic string, partitionID int64, readOffset *int64, commitOffset *int64) {
	var p OnPartitionReadStartInfo
	p.ReaderConnectionID = readerConnectionID
	p.PartitionContext = partitionContext
	p.Topic = topic
	p.PartitionID = partitionID
	p.ReadOffset = readOffset
	p.CommitOffset = commitOffset
	t.onPartitionReadStart(p)
}
func TopicOnPartitionReadStop(t Topic, readerConnectionID string, partitionContext context.Context, topic string, partitionID int64, partitionSessionID int64, committedOffset int64, graceful bool) {
	var p OnPartitionReadStopInfo
	p.ReaderConnectionID = readerConnectionID
	p.PartitionContext = partitionContext
	p.Topic = topic
	p.PartitionID = partitionID
	p.PartitionSessionID = partitionSessionID
	p.CommittedOffset = committedOffset
	p.Graceful = graceful
	t.onPartitionReadStop(p)
}
func TopicOnPartitionCommittedNotify(t Topic, readerConnectionID string, topic string, partitionID int64, committedOffset int64) {
	var p OnPartitionCommittedInfo
	p.ReaderConnectionID = readerConnectionID
	p.Topic = topic
	p.PartitionID = partitionID
	p.CommittedOffset = committedOffset
	t.onPartitionCommittedNotify(p)
}
func TopicOnReaderStreamConnect(t Topic) func(error) {
	var p OnReaderStreamConnectStartInfo
	res := t.onReaderStreamConnect(p)
	return func(e error) {
		var p OnReaderStreamConnectDoneInfo
		p.Error = e
		res(p)
	}
}
func TopicOnReaderStreamClose(t Topic, readerConnectionID string, closeReason error) func(readerConnectionID string, closeReason error, closeError error) {
	var p OnReaderStreamCloseStartInfo
	p.ReaderConnectionID = readerConnectionID
	p.CloseReason = closeReason
	res := t.onReaderStreamClose(p)
	return func(readerConnectionID string, closeReason error, closeError error) {
		var p OnReaderStreamCloseDoneInfo
		p.ReaderConnectionID = readerConnectionID
		p.CloseReason = closeReason
		p.CloseError = closeError
		res(p)
	}
}
func TopicOnReadStreamInit(t Topic, preInitReaderConnectionID string) func(preInitReaderConnectionID string, readerConnectionID string, _ error) {
	var p OnReadStreamInitStartInfo
	p.PreInitReaderConnectionID = preInitReaderConnectionID
	res := t.onReadStreamInit(p)
	return func(preInitReaderConnectionID string, readerConnectionID string, e error) {
		var p OnReadStreamInitDoneInfo
		p.PreInitReaderConnectionID = preInitReaderConnectionID
		p.ReaderConnectionID = readerConnectionID
		p.Error = e
		res(p)
	}
}
func TopicOnReadStreamError(t Topic, baseContext context.Context, readerConnectionID string, e error) {
	var p OnReadStreamErrorInfo
	p.BaseContext = baseContext
	p.ReaderConnectionID = readerConnectionID
	p.Error = e
	t.onReadStreamError(p)
}
func TopicOnReadUnknownGrpcMessage(t Topic, readerConnectionID string, baseContext context.Context, serverMessage readStreamServerMessageDebugInfo, e error) {
	var p OnReadUnknownGrpcMessageInfo
	p.ReaderConnectionID = readerConnectionID
	p.BaseContext = baseContext
	p.ServerMessage = serverMessage
	p.Error = e
	t.onReadUnknownGrpcMessage(p)
}
func TopicOnReadStreamRawReceived(t Topic, readerConnectionID string, baseContext context.Context, serverMessage readStreamServerMessageDebugInfo, e error) {
	var p OnReadStreamRawReceivedInfo
	p.ReaderConnectionID = readerConnectionID
	p.BaseContext = baseContext
	p.ServerMessage = serverMessage
	p.Error = e
	t.onReadStreamRawReceived(p)
}
func TopicOnReadStreamRawSent(t Topic, readerConnectionID string, baseContext context.Context, clientMessage readStreamClientMessageDebugInfo, e error) {
	var p OnReadStreamRawSentInfo
	p.ReaderConnectionID = readerConnectionID
	p.BaseContext = baseContext
	p.ClientMessage = clientMessage
	p.Error = e
	t.onReadStreamRawSent(p)
}
func TopicOnReadStreamUpdateToken(t Topic, readerConnectionID string, baseContext context.Context, tokenLen int, e error) {
	var p OnReadStreamUpdateTokenInfo
	p.ReaderConnectionID = readerConnectionID
	p.BaseContext = baseContext
	p.TokenLen = tokenLen
	p.Error = e
	t.onReadStreamUpdateToken(p)
}
