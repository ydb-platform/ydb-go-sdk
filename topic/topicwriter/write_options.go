package topicwriter

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"

type writeOptions struct {
	transaction tx.Identifier
	commit      bool
}

type WriteOption interface {
	applyWriteOption(options *writeOptions)
}

type writeOptionWithTx struct {
	tx tx.Identifier
}

func (o writeOptionWithTx) applyWriteOption(options *writeOptions) {
	options.transaction = o.tx
}

type writeOptionCommitTx struct {
	commit bool
}

func (o writeOptionCommitTx) applyWriteOption(options *writeOptions) {
	options.commit = o.commit
}
