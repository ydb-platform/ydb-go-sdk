# Package xtest

## Overview

The `xtest` package provides essential testing utilities that enhance Go's standard testing capabilities for YDB Go SDK development. It offers specialized utilities for time manipulation, synchronization testing, concurrency verification, and test execution patterns that are commonly needed when testing database operations and concurrent systems.

**Core Concept**: Extend standard Go testing with utilities optimized for testing concurrent, asynchronous, and time-dependent operations typical in database SDK scenarios.

**Target Use Cases**: 
- Testing concurrent operations with proper synchronization
- Time-dependent test scenarios with controllable clocks
- Repetitive stress testing to catch race conditions
- Assertion and validation utilities
- Context management for tests

## Components

### FastClock
Creates a fake clock with accelerated time progression for testing time-dependent functionality.

```go
func FastClock(t testing.TB) *clockwork.FakeClock
```

**Purpose**: Provides a fast-advancing fake clock that automatically stops when the test ends  
**Parameters**: `t testing.TB` - test instance for cleanup registration  
**Returns**: `*clockwork.FakeClock` - fake clock that advances by 1 second every microsecond  
**Usage**: Use when testing time-based operations like timeouts, delays, or periodic tasks  

**Example**:
```go
func TestWithFastClock(t *testing.T) {
    clock := xtest.FastClock(t)
    start := clock.Now()
    
    // Clock automatically advances rapidly
    time.Sleep(time.Microsecond * 10)
    elapsed := clock.Since(start)
    
    // elapsed will be approximately 10 seconds
    assert.True(t, elapsed >= 10*time.Second)
}
```

### Must
Generic utility for panic-based error handling in tests.

```go
func Must[R any](res R, err error) R
```

**Purpose**: Converts error-returning function calls to panic-based assertions  
**Parameters**: `res R` - result value, `err error` - error to check  
**Returns**: `R` - the result value if no error  
**Usage**: Use for test setup where errors should cause immediate test failure  

**Example**:
```go
func TestMustExample(t *testing.T) {
    // Instead of: conn, err := sql.Open(...); if err != nil { t.Fatal(err) }
    conn := xtest.Must(sql.Open("driver", "dsn"))
    defer conn.Close()
    
    // Test continues with guaranteed valid connection
}
```

### Wait Group Functions
Utilities for waiting on sync.WaitGroup with timeout protection.

```go
func WaitGroup(tb testing.TB, wg *sync.WaitGroup)
func WaitGroupWithTimeout(tb testing.TB, wg *sync.WaitGroup, timeout time.Duration)
```

**Purpose**: Wait for WaitGroup completion with automatic timeout handling  
**Parameters**: `tb` - test instance, `wg` - WaitGroup to wait on, `timeout` - maximum wait duration  
**Returns**: Nothing (calls `t.Fatal` on timeout)  
**Usage**: Prevent tests from hanging when goroutines don't complete  

**Example**:
```go
func TestConcurrentOperations(t *testing.T) {
    var wg sync.WaitGroup
    
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            // Some concurrent operation
        }()
    }
    
    xtest.WaitGroup(t, &wg) // Fails test if not done within 1 second
}
```

### Channel Wait Functions
Utilities for waiting on channel operations with timeout protection.

```go
func WaitChannelClosed(t testing.TB, ch <-chan struct{})
func WaitChannelClosedWithTimeout(t testing.TB, ch <-chan struct{}, timeout time.Duration)
```

**Purpose**: Wait for channel closure with automatic timeout handling  
**Parameters**: `t` - test instance, `ch` - channel to wait on, `timeout` - maximum wait duration  
**Returns**: Nothing (calls `t.Fatal` on timeout)  
**Usage**: Verify that channels are properly closed in concurrent scenarios  

**Example**:
```go
func TestChannelClosure(t *testing.T) {
    ch := make(chan struct{})
    
    go func() {
        time.Sleep(100 * time.Millisecond)
        close(ch)
    }()
    
    xtest.WaitChannelClosed(t, ch) // Waits up to 1 second
}
```

### Spin Wait Functions
Polling-based condition waiting with configurable synchronization.

```go
func SpinWaitCondition(tb testing.TB, l sync.Locker, cond func() bool)
func SpinWaitConditionWithTimeout(tb testing.TB, l sync.Locker, condWaitTimeout time.Duration, cond func() bool)
func SpinWaitProgress(tb testing.TB, progress func() (progressValue interface{}, finished bool))
func SpinWaitProgressWithTimeout(tb testing.TB, timeout time.Duration, progress func() (progressValue interface{}, finished bool))
```

**Purpose**: Poll conditions until they become true or timeout occurs  
**Parameters**: `tb` - test instance, `l` - optional locker for condition check, `cond` - condition function, `timeout` - maximum wait time  
**Returns**: Nothing (calls `t.Fatal` on timeout)  
**Usage**: Wait for complex state changes in concurrent systems  

**Example**:
```go
func TestStateChange(t *testing.T) {
    var counter int32
    var mu sync.Mutex
    
    go func() {
        for i := 0; i < 5; i++ {
            time.Sleep(10 * time.Millisecond)
            mu.Lock()
            counter++
            mu.Unlock()
        }
    }()
    
    xtest.SpinWaitCondition(t, &mu, func() bool {
        return counter >= 5
    })
}
```

### TestManyTimes
Repetitive test execution for catching race conditions and intermittent failures.

```go
func TestManyTimes(t testing.TB, test TestFunc, opts ...TestManyTimesOption)
func TestManyTimesWithName(t *testing.T, name string, test TestFunc)
func StopAfter(stopAfter time.Duration) TestManyTimesOption

type TestFunc func(t testing.TB)
```

**Purpose**: Execute the same test multiple times to catch intermittent failures  
**Parameters**: `t` - test instance, `test` - test function to repeat, `opts` - configuration options  
**Returns**: Nothing  
**Usage**: Stress test concurrent code to find race conditions  

**Example**:
```go
func TestRaceCondition(t *testing.T) {
    xtest.TestManyTimes(t, func(t testing.TB) {
        // Test that might have race conditions
        var counter int32
        var wg sync.WaitGroup
        
        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func() {
                defer wg.Done()
                atomic.AddInt32(&counter, 1)
            }()
        }
        
        xtest.WaitGroup(t, &wg)
        assert.Equal(t, int32(10), counter)
    }, xtest.StopAfter(5*time.Second))
}
```

### SyncedTest
Thread-safe test wrapper for concurrent test execution.

```go
func MakeSyncedTest(t *testing.T) *SyncedTest

type SyncedTest struct {
    *testing.T
    // ... (implements testing.TB interface with synchronization)
}

func (s *SyncedTest) RunSynced(name string, f func(t *SyncedTest)) bool
```

**Purpose**: Provide thread-safe access to testing.T methods from multiple goroutines  
**Parameters**: `t` - original test instance  
**Returns**: `*SyncedTest` - synchronized wrapper  
**Usage**: When multiple goroutines need to call test methods concurrently  

**Example**:
```go
func TestConcurrentLogging(t *testing.T) {
    syncTest := xtest.MakeSyncedTest(t)
    var wg sync.WaitGroup
    
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            syncTest.Logf("Goroutine %d completed", id) // Thread-safe
        }(i)
    }
    
    xtest.WaitGroup(t, &wg)
}
```

### Context Functions
Context creation and management utilities for tests.

```go
func Context(t testing.TB) context.Context
func ContextWithCommonTimeout(ctx context.Context, t testing.TB) context.Context
```

**Purpose**: Create properly configured contexts for test scenarios  
**Parameters**: `t` - test instance, `ctx` - parent context  
**Returns**: `context.Context` - configured context  
**Usage**: Standard context setup with test lifecycle management  

**Example**:
```go
func TestWithContext(t *testing.T) {
    ctx := xtest.Context(t) // Automatically cancelled when test ends
    
    // Test operations with context
    result, err := someOperationWithContext(ctx)
    assert.NoError(t, err)
    assert.NotNil(t, result)
}
```

### Utility Functions
Additional helper functions for common test operations.

```go
func ToJSON(v interface{}) string
func CurrentFileLine() string
func AllowByFlag(t testing.TB, flag string)
```

**Purpose**: Various utilities for test setup and debugging  
**Usage**: JSON serialization, stack introspection, conditional test execution  

**Example**:
```go
func TestUtilities(t *testing.T) {
    data := map[string]int{"key": 42}
    json := xtest.ToJSON(data) // Pretty-printed JSON
    t.Logf("Data: %s", json)
    
    location := xtest.CurrentFileLine() // Current file:line
    t.Logf("Location: %s", location)
    
    xtest.AllowByFlag(t, "RUN_SLOW_TESTS") // Skip unless flag set
}
```

## Usage Patterns

### Concurrent Testing Pattern
Combine multiple utilities for comprehensive concurrent testing.

```go
func TestConcurrentPattern(t *testing.T) {
    ctx := xtest.Context(t)
    syncTest := xtest.MakeSyncedTest(t)
    
    xtest.TestManyTimes(syncTest, func(t testing.TB) {
        var wg sync.WaitGroup
        results := make(chan int, 10)
        
        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func(id int) {
                defer wg.Done()
                // Simulate work with context
                select {
                case <-ctx.Done():
                    return
                case results <- id:
                }
            }(i)
        }
        
        xtest.WaitGroup(t, &wg)
        assert.Equal(t, 10, len(results))
    })
}
```

### Time-Based Testing Pattern
Use fast clock for time-dependent scenarios.

```go
func TestTimeBasedPattern(t *testing.T) {
    clock := xtest.FastClock(t)
    timeout := 30 * time.Second
    
    start := clock.Now()
    
    // Operation that should complete within timeout
    done := make(chan struct{})
    go func() {
        defer close(done)
        // Some time-consuming operation
        time.Sleep(time.Millisecond) // Real time
    }()
    
    xtest.WaitChannelClosedWithTimeout(t, done, timeout)
    elapsed := clock.Since(start)
    
    // Verify timing constraints
    assert.True(t, elapsed < timeout)
}
```

## Best Practices

- **Use appropriate timeouts**: Default timeout is 1 second; adjust based on expected operation duration
- **Combine utilities**: Use `TestManyTimes` with other utilities to catch intermittent issues
- **Thread safety**: Use `SyncedTest` when multiple goroutines need to access test methods
- **Context management**: Always use `xtest.Context(t)` for operations that accept contexts
- **Error handling**: Use `Must` for test setup operations that should never fail
- **Resource cleanup**: All utilities automatically handle cleanup through `t.Cleanup()`

## Integration

The `xtest` package integrates with other YDB SDK components by:

- **xsync integration**: Works seamlessly with xsync utilities for testing concurrent primitives
- **Context integration**: Uses internal/xcontext for context management
- **Stack integration**: Uses internal/stack for runtime introspection
- **Empty channel integration**: Uses internal/empty for channel utilities
- **Standard library enhancement**: Extends testing, sync, and context packages with YDB-specific needs

The package is designed to be imported and used throughout the YDB SDK test suite, providing consistent testing patterns and utilities across all components. 