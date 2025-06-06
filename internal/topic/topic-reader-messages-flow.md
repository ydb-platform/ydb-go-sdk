# Topic Reader Message Flow

## Message Types

### Server Messages
1. **InitResponse**
   - First message received after connection
   - Contains session ID and initial configuration
   - Must be handled before other messages

2. **StartPartitionSessionRequest**
   - Server requests to start reading a partition
   - Contains partition metadata and initial offset
   - Must be confirmed by client

3. **StopPartitionSessionRequest**
   - Server requests to stop reading a partition
   - Can be graceful or immediate
   - Must be confirmed by client

4. **ReadResponse**
   - Contains actual message data
   - Includes partition session ID
   - May contain multiple messages in a batch

5. **CommitOffsetResponse**
   - Response to client's commit request
   - Contains commit status and any errors

### Client Messages
1. **InitRequest**
   - Initial connection setup
   - Contains consumer name and topic selectors

2. **StartPartitionSessionResponse**
   - Confirms partition start
   - Contains client-side session ID

3. **StopPartitionSessionResponse**
   - Confirms partition stop
   - Contains final committed offset

4. **CommitOffsetRequest**
   - Requests offset commit
   - Contains partition session ID and offset

## Partition Lifecycle

1. **Creation**
   - Triggered by StartPartitionSessionRequest
   - Creates new PartitionSession
   - Initializes offset tracking
   - Must be confirmed to server

2. **Operation**
   - Receives ReadResponse messages
   - Tracks message offsets
   - Commits offsets periodically
   - Maintains message order within partition

3. **Destruction**
   - Triggered by StopPartitionSessionRequest
   - Can be graceful or immediate
   - Commits final offset
   - Removes from session storage
   - Must be confirmed to server

## Message Processing Patterns

1. **Connection Management**
   - Single gRPC stream per listener
   - Handles reconnection automatically
   - All partition sessions are reset on reconnect
   - New connections always start fresh sessions

2. **Partition Management**
   - Dynamic partition assignment
   - Thread-safe session storage
   - Garbage collection for removed sessions

3. **Message Delivery**
   - Ordered delivery within partition
   - Batch processing for efficiency
   - Offset tracking and commit management

4. **Error Handling**
   - Graceful degradation on errors
   - Automatic reconnection
   - Error propagation to user handlers

## PartitionWorker Message Handling Patterns

### Clean Worker Design
- **Dependency Injection**: All external interactions through interfaces
- **No Side Effects**: Worker only interacts with injected dependencies
- **Testable Design**: Can mock all external components for unit testing
- **Sequential Processing**: Messages within a partition processed in order

### Context-Aware Message Queue Integration
- **UnboundedChan with Context**: Uses `Receive(ctx)` method for built-in context cancellation support
- **Simplified Processing**: Direct integration eliminates need for custom goroutines
- **Immediate Cancellation**: Context cancellation respected during message receiving
- **Clean Error Handling**: Distinguishes context errors from normal queue closure
- **Performance Optimized**: Minimal overhead when context is not cancelled

### Message Queue Patterns
- **Queue-Based Delivery**: Uses UnboundedChan for asynchronous message processing
- **Built-in Merging**: ReadResponse messages merged automatically to optimize performance
- **Context Integration**: `Receive(ctx)` returns immediately on context cancellation
- **Error Propagation**: Context errors properly reported via callback mechanism

### Safe Message Merging Patterns
- **Metadata Validation**: Messages only merged when ServerMessageMetadata is identical
- **Hierarchical Comparison**: Uses nested Equals() methods for deep metadata comparison
- **Status Code Matching**: StatusCode fields must be identical for merge compatibility
- **Issues Collection Matching**: Issues arrays must be identical including nested structures
- **Merge Prevention Strategy**: When metadata differs, messages processed separately
- **Data Integrity Guarantee**: No loss of status or error information during merging
- **Performance Optimization**: Efficient comparison with early termination on differences

### Metadata Validation Requirements for Merging
- **Complete Metadata Equality**: All metadata fields must match exactly
- **Nested Structure Validation**: Deep comparison of nested Issues structures
- **Nil Safety**: Proper handling of nil metadata and nested components
- **Type-Safe Comparison**: All comparisons respect Go type safety
- **Fail-Safe Behavior**: Uncertain comparisons default to preventing merge
- **Hierarchical Delegation**: Top-level Equals() delegates to nested structure methods

### Message Type Handling
- **StartPartitionSession**: Creates user event, waits for confirmation, sends response
- **StopPartitionSession**: Handles both graceful and non-graceful termination
- **ReadResponse**: Converts raw batches to public batches, processes each batch individually
- **Message Merging**: ReadResponse messages merged to optimize user handler calls

### Context Handling Best Practices
- **Direct Integration**: Use `UnboundedChan.Receive(ctx)` for context-aware receiving
- **Error Distinction**: Handle `context.Canceled` and `context.DeadlineExceeded` appropriately
- **Graceful Shutdown**: Distinguish context cancellation from queue closure for proper cleanup
- **Resource Management**: Context cancellation prevents goroutine leaks during shutdown

### Integration with Existing Components
- **streamListener Compatibility**: Uses existing event creation patterns
- **topicreadercommon.PartitionSession**: Leverages existing session management
- **background.Worker**: Integrates with existing worker lifecycle patterns
- **Context Propagation**: Proper context flow from streamListener to PartitionWorker

## Integration Points

1. **User Handler Interface**
   - OnStartPartitionSessionRequest
   - OnStopPartitionSessionRequest
   - OnReadMessages
   - OnReaderCreated

2. **Background Worker**
   - Message sending loop
   - Message receiving loop
   - Commit management
   - **PartitionWorker Integration**: Individual workers for each partition with context awareness

3. **Partition Session Storage**
   - Thread-safe session management
   - Session lifecycle tracking
   - Garbage collection
   - **Worker Coordination**: Session sharing between streamListener and workers

4. **Commit Management**
   - Synchronous commit operations
   - Offset tracking
   - Error handling
   - **Worker Integration**: Commit operations initiated from worker-processed events 