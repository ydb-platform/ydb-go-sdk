# gtrace

Command line tool **gtrace** generates boilerplate code for Go components tracing (aka _instrumentation_).

## Usage

As a developer of some module (whenever its library or application module) you should define _trace points_ (or _hooks_) which user of your code can then initialize with some function (aka _probes_) during runtime.

### TL;DR

**gtrace** suggests you to use structures (tagged with `//gtrace:gen`) holding all _hooks_ related to your component and generates helper functions around them so that you can merge such structures and call the _hooks_ without any checks for `nil`. It also can generate context aware helpers to call _hooks_ associated with context.

Example of generated code [is here](examples/pinger/main_gtrace.go).

### Basic

Lets assume that we have some package called `lib` and some `lib.Client` structure which holds `net.Conn` internally and pings it every time before making some request when user calls `Client.Do()`.
For the sake of simplicity lets not cover how dial, ping or any other thing happens.

```go
type Client struct {
	conn net.Conn
}

func (c *Client) Do(ctx context.Context) error {
	if err := c.ping(ctx); err != nil {
		return err
	}
	// Some client logic.
}

func (c *Client) ping(ctx context.Context) error {
	return doPing(ctx, c.conn)
}
```

What if we need to write some logs right before and after ping happens? There are several ways to do it, but with **gtrace** we start by defining _trace points_ in our package:

```go
package lib

type ClientTrace struct {
	OnPing func() func(error)
}

type Client struct {
	Trace ClientTrace
	...
}
```

That is, we export _hook functions_ for every code point that might be interesting for the _user_ of our package. The `ClientTrace` structure contains definitions of all _trace points_ for the `Client`. For this example it has only one point. It defines pair of _ping start_ and _ping done_ callbacks. A user of our package can use it like so:

```go
c := lib.Client{
	Trace: ClientTrace{
		OnPing: func() {
			log.Println("ping start")
			return func(err error) {
				log.Printf("ping done; err=%v", err)
			}
		},	
	},
}
```

How the `Client` should call that _hooks_? Well, thats the one of the reason of **gtrace** exists: it generates few useful (and very annoying to be manually typed) helpers to use this tracing approach. Lets do this:

```go
package lib

//go:generate gtrace

//gtrace:gen
type ClientTrace struct {
	OnPing func() func(error)
}
```

And after `go generate` we can instrument our pinging facility as this:

```go
func (c *Client) ping(ctx context.Context) error {
	done := c.Trace.onPing() // added this line.
	err := doPing(ctx, c.conn)
	done(err) // and this line.
	return err
}
```

*grace* has generated that `lib.Client.onPing()` unexported method which checks if appopriate _probe_ function is non-nil (as well as the returned _ping done_ callback). If any of the callbacks is nil it returns noop functions to avoid branching in the `Client.ping()` code.

### Composing

Lets return to the user of our package and cover another feature that **gtrace** generates for us: _trace points composing_. Composing is about merging two structures of the same trace and resulting a third one which calls _hooks_ from both of them. It is useful when user wants to instrument our ping facility with different measure types (to write logs as well as measure call latency):

```go
var t ClientTrace
t = t.Compose(ClientTrace{
	OnPing: func() {
		log.Println("ping start")
		return func(err error) {
			log.Printf("ping done; err=%v", err)
		}
	},	
})
t = t.Compose(ClientTrace{
	OnPing: func() {
		start := time.Now()
		return func(error) {
			sendLatency(time.Since(start))
		}
	},	
})
c := lib.Client{
	Trace: t,
}
```

### Context

_Trace points composing_ gives us additional way to instrument our package: a context based tracing. We can setup `ClientTrace` not for the whole `Client`, but for some particular context (and probably do this on some particular condition). To do this we should ask **gtrace** to generate context aware tracing:

```go
//gtrace:gen
//gtrace:set context
type ClientTrace struct {
	OnPing func() func(error)
}
```

After `go generate` command signature of `lib.Client.onPing` changed to `onPing(context.Context)`, as well as two additional _exported_ functions added: `lib.WithClientTrace()` and `lib.ContextClientTrace()`. The former is to associate some `ClientTrace` with some context; and the latter is to obtain associated `ClientTrace` from context. So on the `Client` side we should only pass the context to the `onPing()` method:

```go
func (c *Client) ping(ctx context.Context) error {
	done := c.Trace.onPing(ctx) // this line has changed.
	err := doPing(ctx, c.conn)
	done(err)
	return err
}
```

And on the user side we can do this:

```go
c := lib.Client{
	Trace: t, // Note that both traces are used.
}
// Send 100 requests with every 5th being instrumented additionally.
for i := 0; i < 100; i++ {
	ctx := context.Background()
	if i % 5 == 0 {
		ctx = lib.WithClientTrace(ctx, ClientTrace{
			...
		})
	}
	if err := c.Do(ctx); err != nil {
		// handle error.
	}
}
```

### Shortcuts

Thats it for basic tracing. But usually _trace points_ define _hooks_ with number of arguments way bigger than one or two. In that case we can declare a struct holding all _hook's_ arguments instead:

```go
type ClientTrace struct {
	OnPing func(ClientTracePingStart) func(ClientTracePingDone)
}
```

This makes _hooks_ more readable and extensible. But it also makes calling such _hooks_ a bit more verbose:

```go
func (c *Client) ping(ctx context.Context) error {
	done := c.Trace.onPing(ClientTracePingStart{
		Foo: 1,
		Bar: 2,
		Baz: 3,
	}) 
	err := doPing(ctx, c.conn)
	done(ClientTracePingDone{
		Foo: 1,
		Bar: 2,
		Baz: 3,
		Err: err,
	}) 
	return err
}
```

**gtrace** can generate functions called **shortcuts** to call the _hook_ in more "flat" way:

```go
//gtrace:gen
//gtrace:set shortcut
type ClientTrace struct {
	OnPing func(ClientTracePingStart) func(ClientTracePingDone)
}
```

After `go generate` we able to call _hooks_ like this:

```go
func (c *Client) ping(ctx context.Context) error {
	done := clientTraceOnPing(c.Trace, 1, 2, 3)
	err := doPing(ctx, c.conn)
	done(1, 2, 3, err)
	return err
}
```

### Examples

For more details feel free to read the `examples` package of this repo as well as delve into `test/test_grace.go`.
