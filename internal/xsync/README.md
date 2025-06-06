# Package xsync

## Overview

The `xsync` package provides advanced synchronization utilities that extend Go's standard sync package with specialized primitives optimized for concurrent database operations and high-performance scenarios. It offers generic, type-safe alternatives to standard library primitives along with unique synchronization patterns not available in the standard library.

**Core Concept**: Enhance Go's synchronization primitives with type safety, additional functionality, and specialized patterns needed for database SDK operations, particularly focusing on unbounded channels, soft semaphores, and enhanced mutexes.

**Design Principles**:
- **Type Safety**: Generic implementations eliminate type assertions and improve compile-time safety
- **Enhanced Functionality**: Extended capabilities beyond standard library primitives
- **Performance Optimization**: Specialized implementations for high-throughput scenarios
- **Concurrent Safety**: All utilities are designed for heavy concurrent usage

## Components

### UnboundedChan
Generic unbounded channel implementation with message merging capabilities.

```go
type UnboundedChan[T any] struct { /* ... */ }

func NewUnboundedChan[T any]() *UnboundedChan[T]
func (c *UnboundedChan[T]) Send(msg T)
func (c *UnboundedChan[T]) SendWithMerge(msg T, mergeFunc func(last, new T) (T, bool))
func (c *UnboundedChan[T]) Receive(ctx context.Context) (T, bool, error)
func (c *UnboundedChan[T]) Close()
```

**Purpose**: Provides an unbounded channel that never blocks on send operations and supports message merging  
**Generic Constraints**: `T any` - any type can be used as message type  
**Thread Safety**: All operations are thread-safe and can be called concurrently  
**Performance**: Non-blocking sends with efficient internal buffering  

**Example**:
```go
func ExampleUnboundedChan() {
    ch := xsync.NewUnboundedChan[string]()
    defer ch.Close()
    
    // Non-blocking sends
    ch.Send("message1")
    ch.Send("message2")
    
    // Send with merging logic
    ch.SendWithMerge("update", func(last, new string) (string, bool) {
        if strings.HasPrefix(last, "update") {
            return new, true // Replace last update
        }
        return new, false // Don't merge
    })
    
    // Receive with context
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()
    
    for {
        msg, ok, err := ch.Receive(ctx)
        if err != nil {
            break // Context timeout or cancellation
        }
        if !ok {
            break // Channel closed and empty
        }
        fmt.Println("Received:", msg)
    }
}
```

### SoftWeightedSemaphore
Extended semaphore that allows overflow acquisition when completely free.

```go
type SoftWeightedSemaphore struct { /* ... */ }

func NewSoftWeightedSemaphore(n int64) *SoftWeightedSemaphore
func (s *SoftWeightedSemaphore) Acquire(ctx context.Context, n int64) error
func (s *SoftWeightedSemaphore) Release(n int64)
func (s *SoftWeightedSemaphore) TryAcquire(n int64) bool
```

**Purpose**: Semaphore that allows one request to exceed capacity if semaphore is completely free  
**Parameters**: `n int64` - semaphore capacity  
**Thread Safety**: All operations are thread-safe  
**Performance**: Optimized for scenarios where occasional large requests are acceptable  

**Example**:
```go
func ExampleSoftSemaphore() {
    sem := xsync.NewSoftWeightedSemaphore(10)
    
    // Normal acquisition within capacity
    ctx := context.Background()
    err := sem.Acquire(ctx, 5)
    if err == nil {
        defer sem.Release(5)
        // Use 5 units of resource
    }
    
    // Large acquisition when semaphore is free
    err = sem.Acquire(ctx, 15) // Exceeds capacity but allowed if free
    if err == nil {
        defer sem.Release(15)
        // Use 15 units of resource (overflow mode)
    }
    
    // Non-blocking attempt
    if sem.TryAcquire(3) {
        defer sem.Release(3)
        // Successfully acquired 3 units
    }
}
```

### Pool
Generic type-safe object pool with configurable creation function.

```go
type Pool[T any] struct {
    New func() *T
    // ... (private fields)
}

func (p *Pool[T]) GetOrNew() *T
func (p *Pool[T]) GetOrNil() *T
func (p *Pool[T]) Put(t *T)
```

**Purpose**: Type-safe object pool that eliminates type assertions  
**Generic Constraints**: `T any` - any type can be pooled  
**Thread Safety**: All operations are thread-safe  
**Performance**: Reduces allocations by reusing objects  

**Example**:
```go
func ExamplePool() {
    pool := &xsync.Pool[bytes.Buffer]{
        New: func() *bytes.Buffer {
            return &bytes.Buffer{}
        },
    }
    
    // Get buffer from pool or create new
    buf := pool.GetOrNew()
    buf.WriteString("Hello, World!")
    
    // Reset and return to pool
    buf.Reset()
    pool.Put(buf)
    
    // Try to get from pool (might be nil if empty)
    buf2 := pool.GetOrNil()
    if buf2 != nil {
        defer pool.Put(buf2)
        // Use recycled buffer
    }
}
```

### Map
Generic concurrent map with atomic size tracking.

```go
type Map[K comparable, V any] struct { /* ... */ }

func (m *Map[K, V]) Get(key K) (value V, ok bool)
func (m *Map[K, V]) Must(key K) (value V)
func (m *Map[K, V]) Has(key K) bool
func (m *Map[K, V]) Set(key K, value V)
func (m *Map[K, V]) Delete(key K) bool
func (m *Map[K, V]) Extract(key K) (value V, ok bool)
func (m *Map[K, V]) Len() int
func (m *Map[K, V]) Range(f func(key K, value V) bool)
func (m *Map[K, V]) Clear() (removed int)
```

**Purpose**: Type-safe concurrent map with additional utility methods  
**Generic Constraints**: `K comparable` - key type must be comparable, `V any` - any value type  
**Thread Safety**: All operations are thread-safe  
**Performance**: Atomic size tracking without full iteration  

**Example**:
```go
func ExampleMap() {
    m := &xsync.Map[string, int]{}
    
    // Set values
    m.Set("key1", 42)
    m.Set("key2", 84)
    
    // Get with ok pattern
    if value, ok := m.Get("key1"); ok {
        fmt.Println("Found:", value)
    }
    
    // Must get (panics if not found)
    value := m.Must("key1") // Returns 42
    
    // Check existence
    if m.Has("key2") {
        fmt.Println("key2 exists")
    }
    
    // Atomic size
    fmt.Println("Size:", m.Len())
    
    // Range over entries
    m.Range(func(key string, value int) bool {
        fmt.Printf("%s: %d\n", key, value)
        return true // Continue iteration
    })
    
    // Extract (get and delete atomically)
    if value, ok := m.Extract("key1"); ok {
        fmt.Println("Extracted:", value)
    }
}
```

### Enhanced Mutexes
Mutexes with convenient closure-based locking.

```go
type Mutex struct {
    sync.Mutex
}

type RWMutex struct {
    sync.RWMutex
}

func (l *Mutex) WithLock(f func())
func (l *RWMutex) WithLock(f func())
func (l *RWMutex) WithRLock(f func())

func WithLock[T any](l mutex, f func() T) T
func WithRLock[T any](l rwMutex, f func() T) T
```

**Purpose**: Provide closure-based locking to prevent lock/unlock mismatches  
**Thread Safety**: Standard mutex semantics with automatic unlock  
**Performance**: Zero overhead wrapper around standard library mutexes  

**Example**:
```go
func ExampleMutex() {
    var mu xsync.Mutex
    var data int
    
    // Closure-based locking
    mu.WithLock(func() {
        data++
        // Automatically unlocked when function returns
    })
    
    // Generic function with return value
    result := xsync.WithLock(&mu, func() int {
        return data * 2
    })
    
    // RWMutex example
    var rwmu xsync.RWMutex
    var cache map[string]string
    
    // Read lock
    value := xsync.WithRLock(&rwmu, func() string {
        return cache["key"]
    })
    
    // Write lock
    rwmu.WithLock(func() {
        cache["key"] = "value"
    })
}
```

### Set
Generic concurrent set implementation.

```go
type Set[T comparable] struct { /* ... */ }

func (s *Set[T]) Has(key T) bool
func (s *Set[T]) Add(key T) bool
func (s *Set[T]) Remove(key T) bool
func (s *Set[T]) Size() int
func (s *Set[T]) Range(f func(key T) bool)
func (s *Set[T]) Values() []T
func (s *Set[T]) Clear() (removed int)
```

**Purpose**: Type-safe concurrent set with atomic size tracking  
**Generic Constraints**: `T comparable` - element type must be comparable  
**Thread Safety**: All operations are thread-safe  
**Performance**: Efficient membership testing and atomic size tracking  

**Example**:
```go
func ExampleSet() {
    s := &xsync.Set[string]{}
    
    // Add elements
    added := s.Add("item1") // Returns true if newly added
    s.Add("item2")
    
    // Check membership
    if s.Has("item1") {
        fmt.Println("item1 is in set")
    }
    
    // Get size
    fmt.Println("Size:", s.Size())
    
    // Get all values
    values := s.Values()
    fmt.Println("Values:", values)
    
    // Range over elements
    s.Range(func(item string) bool {
        fmt.Println("Item:", item)
        return true // Continue iteration
    })
    
    // Remove element
    removed := s.Remove("item1") // Returns true if was present
    
    // Clear all
    count := s.Clear()
    fmt.Println("Removed", count, "items")
}
```

### Value
Generic atomic value with transformation support.

```go
type Value[T any] struct { /* ... */ }

func NewValue[T any](initValue T) *Value[T]
func (v *Value[T]) Get() T
func (v *Value[T]) Change(change func(old T) T)
```

**Purpose**: Type-safe atomic value with transformation function support  
**Generic Constraints**: `T any` - any type can be stored  
**Thread Safety**: All operations are thread-safe  
**Performance**: RWMutex-based implementation for high read throughput  

**Example**:
```go
func ExampleValue() {
    // Initialize with value
    counter := xsync.NewValue(0)
    
    // Read current value
    current := counter.Get()
    fmt.Println("Current:", current)
    
    // Atomic transformation
    counter.Change(func(old int) int {
        return old + 1
    })
    
    // Complex transformation
    config := xsync.NewValue(map[string]string{
        "host": "localhost",
        "port": "8080",
    })
    
    config.Change(func(old map[string]string) map[string]string {
        new := make(map[string]string)
        for k, v := range old {
            new[k] = v
        }
        new["timeout"] = "30s"
        return new
    })
}
```

### EventBroadcast
Event broadcasting mechanism for notifying multiple waiters.

```go
type EventBroadcast struct { /* ... */ }

func (b *EventBroadcast) Waiter() OneTimeWaiter
func (b *EventBroadcast) Broadcast()

type OneTimeWaiter struct { /* ... */ }
func (w *OneTimeWaiter) Done() <-chan struct{}
```

**Purpose**: Broadcast events to multiple waiting goroutines  
**Thread Safety**: All operations are thread-safe  
**Performance**: Efficient notification of multiple waiters  

**Example**:
```go
func ExampleEventBroadcast() {
    var broadcast xsync.EventBroadcast
    
    // Multiple goroutines waiting for event
    for i := 0; i < 5; i++ {
        go func(id int) {
            waiter := broadcast.Waiter()
            
            // Check condition first
            if !conditionMet() {
                // Wait for broadcast
                <-waiter.Done()
            }
            
            // Process event
            fmt.Printf("Goroutine %d processed event\n", id)
        }(i)
    }
    
    // Trigger event for all waiters
    time.Sleep(100 * time.Millisecond)
    broadcast.Broadcast()
}
```

### Once
Generic once execution with error handling and resource management.

```go
type Once[T closer.Closer] struct { /* ... */ }

func OnceFunc(f func(ctx context.Context) error) func(ctx context.Context) error
func OnceValue[T closer.Closer](f func() (T, error)) *Once[T]
func (v *Once[T]) Get() (T, error)
func (v *Once[T]) Must() T
func (v *Once[T]) Close(ctx context.Context) error
```

**Purpose**: Execute function once with error handling and resource cleanup  
**Generic Constraints**: `T closer.Closer` - type must implement Close method  
**Thread Safety**: All operations are thread-safe  
**Performance**: Efficient once execution with proper cleanup  

**Example**:
```go
func ExampleOnce() {
    // Function that runs once
    initFunc := xsync.OnceFunc(func(ctx context.Context) error {
        fmt.Println("Initializing...")
        return nil
    })
    
    // Multiple calls, but function runs only once
    ctx := context.Background()
    initFunc(ctx)
    initFunc(ctx) // No-op
    
    // Once value with resource management
    dbOnce := xsync.OnceValue(func() (*sql.DB, error) {
        return sql.Open("driver", "dsn")
    })
    
    // Get database connection (created once)
    db, err := dbOnce.Get()
    if err == nil {
        // Use database
        defer dbOnce.Close(ctx) // Properly close resource
    }
    
    // Must variant (panics on error)
    db2 := dbOnce.Must()
    _ = db2
}
```

### LastUsage
Track last usage time with active usage protection.

```go
type LastUsage interface {
    Get() time.Time
    Start() (stop func())
}

func NewLastUsage(opts ...lastUsageOption) *lastUsage
func WithClock(clock clockwork.Clock) lastUsageOption
```

**Purpose**: Track when a resource was last used, with protection during active usage  
**Thread Safety**: All operations are thread-safe  
**Performance**: Atomic operations for high-frequency usage tracking  

**Example**:
```go
func ExampleLastUsage() {
    usage := xsync.NewLastUsage()
    
    // Start using resource
    stop := usage.Start()
    
    // While in use, Get() returns current time
    fmt.Println("In use, current time:", usage.Get())
    
    // Stop using resource
    stop()
    
    // After stopping, Get() returns the stop time
    time.Sleep(100 * time.Millisecond)
    lastUsed := usage.Get()
    fmt.Println("Last used:", lastUsed)
    
    // Multiple concurrent usages
    stop1 := usage.Start()
    stop2 := usage.Start()
    
    // Still returns current time while any usage is active
    fmt.Println("Multiple usage:", usage.Get())
    
    stop1()
    // Still active due to second usage
    fmt.Println("One stopped:", usage.Get())
    
    stop2()
    // Now returns actual last usage time
    fmt.Println("All stopped:", usage.Get())
}
```

## Usage Patterns

### Producer-Consumer with Unbounded Channel
Efficient producer-consumer pattern without blocking.

```go
func ProducerConsumerPattern() {
    ch := xsync.NewUnboundedChan[WorkItem]()
    defer ch.Close()
    
    // Producer goroutines (never block)
    for i := 0; i < 5; i++ {
        go func(id int) {
            for j := 0; j < 100; j++ {
                ch.Send(WorkItem{ID: id, Data: j})
            }
        }(i)
    }
    
    // Consumer with context
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    for {
        item, ok, err := ch.Receive(ctx)
        if err != nil || !ok {
            break
        }
        processWorkItem(item)
    }
}
```

### Resource Pool Management
Combine multiple utilities for resource management.

```go
func ResourcePoolPattern() {
    pool := &xsync.Pool[Connection]{
        New: func() *Connection {
            return &Connection{/* ... */}
        },
    }
    
    sem := xsync.NewSoftWeightedSemaphore(10)
    usage := xsync.NewLastUsage()
    
    // Acquire resource
    ctx := context.Background()
    if err := sem.Acquire(ctx, 1); err != nil {
        return
    }
    defer sem.Release(1)
    
    conn := pool.GetOrNew()
    defer pool.Put(conn)
    
    stop := usage.Start()
    defer stop()
    
    // Use connection
    conn.Execute("SELECT 1")
}
```

### Configuration Management
Thread-safe configuration updates with broadcasting.

```go
func ConfigManagementPattern() {
    config := xsync.NewValue(Config{Host: "localhost"})
    broadcast := &xsync.EventBroadcast{}
    watchers := &xsync.Set[string]{}
    
    // Configuration updater
    go func() {
        for newConfig := range configUpdates {
            config.Change(func(old Config) Config {
                return newConfig
            })
            broadcast.Broadcast()
        }
    }()
    
    // Configuration watchers
    for i := 0; i < 5; i++ {
        go func(id string) {
            watchers.Add(id)
            defer watchers.Remove(id)
            
            for {
                waiter := broadcast.Waiter()
                currentConfig := config.Get()
                
                if !shouldContinue(currentConfig) {
                    break
                }
                
                <-waiter.Done()
            }
        }(fmt.Sprintf("watcher-%d", i))
    }
}
```

## Best Practices

- **Generic Type Safety**: Always use generic versions to avoid type assertions and improve compile-time safety
- **Resource Management**: Use `Once` for expensive resource initialization with proper cleanup
- **Unbounded Channels**: Use for producer-consumer scenarios where blocking producers is unacceptable
- **Soft Semaphores**: Use when occasional overflow is acceptable for better resource utilization
- **Closure-based Locking**: Use `WithLock` methods to prevent lock/unlock mismatches
- **Event Broadcasting**: Use for notifying multiple goroutines about state changes
- **Usage Tracking**: Use `LastUsage` for idle resource cleanup and monitoring

## Integration

The `xsync` package integrates with other YDB SDK components by:

- **Context Integration**: All blocking operations support context cancellation and timeouts
- **Clock Integration**: Uses clockwork for testable time operations
- **Closer Integration**: Integrates with internal/closer for resource management
- **Empty Channel Integration**: Uses internal/empty for efficient signaling
- **Standard Library Enhancement**: Extends sync package with type-safe and enhanced primitives
- **Performance Optimization**: Provides specialized primitives for high-throughput database operations

The package is designed to be the foundation for concurrent operations throughout the YDB SDK, providing both performance and safety improvements over standard library primitives. 