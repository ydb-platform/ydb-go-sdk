package operation

import (
	"google.golang.org/protobuf/types/known/durationpb"
	"time"
)

func timeoutParam(d time.Duration) *durationpb.Duration {
	if d > 0 {
		return durationpb.New(d)
	}
	return nil
}
