package internal

import "github.com/golang/protobuf/proto"

type Operation struct {
	method string
	req    proto.Message
	res    proto.Message
}

func Wrap(method string, req, res proto.Message) Operation {
	return Operation{
		method: method,
		req:    req,
		res:    res,
	}
}

func Unwrap(op Operation) (method string, req, res proto.Message) {
	return op.method, op.req, op.res
}

type StreamOperation struct {
	method    string
	req       proto.Message
	res       proto.Message
	processor func(error)
}

func WrapStreamOperation(
	method string, req, res proto.Message,
	p func(error),
) StreamOperation {
	return StreamOperation{
		method:    method,
		req:       req,
		res:       res,
		processor: p,
	}
}

func UnwrapStreamOperation(op StreamOperation) (
	method string, req, res proto.Message,
	processor func(error),
) {
	return op.method, op.req, op.res, op.processor
}
