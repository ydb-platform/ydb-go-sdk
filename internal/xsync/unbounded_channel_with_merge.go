package xsync

type UnboundedChan[T any] struct {
	in      chan T
	out     chan T
	stop    chan struct{}
	stopped chan struct{}

	buffer    []T
	mergeFunc func(first, second T) (result T, merged bool)
}

func NewUnboundedChan[T any]() *UnboundedChan[T] {
	ch := &UnboundedChan[T]{
		in:      make(chan T),
		out:     make(chan T),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go ch.run()
	return ch
}

func NewUnboundedChanWithMergeLastItem[T any](f func(first, second T) (result T, merged bool)) *UnboundedChan[T] {
	ch := NewUnboundedChan[T]()
	ch.mergeFunc = f
	return ch
}

func (c *UnboundedChan[T]) In() chan<- T {
	return c.in
}

func (c *UnboundedChan[T]) Send(message T) {
	c.in <- message
}

func (c *UnboundedChan[T]) Out() <-chan T {
	return c.out
}

func (c *UnboundedChan[T]) Close() {
	close(c.in)
}

func (c *UnboundedChan[T]) CloseAndStop() {
	c.Close()
	close(c.stop)

	<-c.stopped
}

func (c *UnboundedChan[T]) run() {
	defer close(c.stopped)
	defer close(c.out)

	for {
		// check if buffer is closed, receiving the message is side effect
		select {
		case message, ok := <-c.in:
			if ok {
				c.addMessageToBuffer(message)
			} else {
				c.runFlushBuffer()

				return
			}
		default:
			// in channel is not closed
		}

		if len(c.buffer) == 0 {
			c.runReceiveFirstMessage()
		} else {
			c.pumpMessages()
		}
	}
}

func (c *UnboundedChan[T]) runReceiveFirstMessage() {
	message, ok := <-c.in
	if ok {
		c.addMessageToBuffer(message)
	}
}

func (c *UnboundedChan[T]) addMessageToBuffer(message T) {
	if c.mergeFunc == nil || len(c.buffer) == 0 {
		c.buffer = append(c.buffer, message)
	} else {
		res, merged := c.mergeFunc(c.buffer[len(c.buffer)-1], message)
		if merged {
			c.buffer[len(c.buffer)-1] = res
		} else {
			c.buffer = append(c.buffer, message)
		}
	}
}

func (c *UnboundedChan[T]) runFlushBuffer() {
	for i := range c.buffer {
		select {
		case c.out <- c.buffer[i]:
		case <-c.stop:
			return
		}
	}
}

func (c *UnboundedChan[T]) pumpMessages() {
	for {
		if len(c.buffer) == 0 {
			return
		}

		select {
		case message, ok := <-c.in:
			if !ok {
				return
			}
			c.addMessageToBuffer(message)
		case c.out <- c.buffer[0]:
			c.buffer = c.buffer[1:]
		}
	}
}
