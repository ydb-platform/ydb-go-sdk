package topiclistener

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"

var (
	_ EventHandler                       = BaseHandler{} // check implementation
	_ topiclistenerinternal.EventHandler = BaseHandler{} // check implementation
)
