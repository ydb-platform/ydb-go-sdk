package topicreadercommon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamTraceDrainsAfterObserverPanic(t *testing.T) {
	retired := make(chan error, 1)
	observations := make(chan int, 3)
	release := make(chan struct{})
	observer := func(i trace.TopicReaderCreditBalanceChangedInfo) {
		<-release
		observations <- i.BytesDelta
		if i.BytesDelta == 2 {
			panic("bad observer")
		}
	}
	tr, closeQueue := StreamTrace(&trace.Topic{OnReaderCreditBalanceChanged: observer}, ReaderInfo{}, func(err error) {
		retired <- err
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, n := range []int{1, 2, -3} {
		tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{Context: &ctx, BytesDelta: n})
	}
	closeQueue()
	close(release)
	for _, want := range []int{1, 2, -3} {
		select {
		case got := <-observations:
			require.Equal(t, want, got)
		case <-ctx.Done():
			t.Fatal("queue did not drain")
		}
	}
	select {
	case err := <-retired:
		require.ErrorContains(t, err, "bad observer")
	case <-ctx.Done():
		t.Fatal("stream did not retire")
	}
}
