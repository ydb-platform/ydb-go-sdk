# TopicListener Internal Architecture

## Overview
The TopicListener is a component that allows users to subscribe to and process messages from YDB topics. It maintains a gRPC stream connection to the server and processes incoming messages, managing partition sessions and delivering messages to user handlers.

## Key Components

### streamListener
- **Purpose**: Core component that manages the gRPC stream connection and processes incoming messages
- **Key Fields**:
  - `stream`: Raw gRPC stream connection
  - `sessions`: Storage for partition sessions
  - `handler`: User-provided event handler
  - `background`: Background worker for async operations
  - `syncCommitter`: Handles message commit operations
- **Key Methods**:
  - `receiveMessagesLoop`: Main message processing loop
  - `onReceiveServerMessage`: Processes different types of server messages
  - `onStartPartitionRequest`: Handles new partition assignments
  - `onStopPartitionRequest`: Handles partition stop requests
  - `onReadResponse`: Processes incoming messages

### Integration with Common Components
- **PartitionSession**: Represents a single partition being read
  - Tracks committed and received offsets
  - Manages partition lifecycle
  - Provides context for partition operations
- **PartitionSessionStorage**: Manages collection of partition sessions
  - Thread-safe storage with RWMutex
  - Handles session addition/removal
  - Implements garbage collection for removed sessions

## Current Message Processing Flow

### Sequence Diagram (Text Format)
```
gRPC Stream -> streamListener.receiveMessagesLoop -> onReceiveServerMessage -> [Message Type Handler] -> User Handler
```

### Detailed Steps
1. Messages arrive through gRPC stream
2. `receiveMessagesLoop` reads messages and calls `onReceiveServerMessage`
3. Based on message type:
   - StartPartitionSessionRequest: Creates new partition session
   - StopPartitionSessionRequest: Removes partition session
   - ReadResponse: Processes messages and calls user handler
4. User handlers are called sequentially for each partition
5. Messages are committed after processing

## Current Threading Model
- **Single-threaded**: All user handler calls are made sequentially
- **User Handler Calls**: Made in `onReadResponse` method
- **Bottleneck**: Single message processing loop handles all partitions

## Critical Points for Multithreading
- **User Handler Call Sites**: 
  - `onReadResponse` method in streamListener
  - `onStartPartitionSessionRequest` for partition start events
  - `onStopPartitionSessionRequest` for partition stop events
- **Partition Management**: 
  - Partition sessions are created in `onStartPartitionRequest`
  - Removed in `onStopPartitionRequest`
  - Stored in thread-safe `PartitionSessionStorage`
- **Message Routing**: 
  - Messages are routed based on partition session ID
  - Each message batch contains partition session ID
- **Synchronization Points**:
  - Partition session storage (already thread-safe)
  - Message commit operations
  - Background worker task management

## PartitionWorker Architecture

### Overview
The PartitionWorker is a clean, testable component that processes messages for a single partition in the multithreaded TopicListener design. Each partition gets its own worker that processes messages sequentially within the partition while allowing different partitions to process in parallel.

### Key Design Principles
- **Clean Architecture**: All dependencies injected through interfaces
- **No Global State**: All state contained in struct fields
- **Easy Unit Testing**: All external interactions through mockable interfaces
- **Thread Safety**: Proper synchronization for concurrent operations
- **Graceful Lifecycle**: Proper startup and shutdown handling
- **Context-Aware Processing**: Integrated context handling for graceful cancellation

### Core Components
- **MessageSender Interface**: Abstraction for sending responses back to server
- **EventHandler Interface**: User-provided handler for partition events
- **WorkerStoppedCallback**: Notification mechanism for worker lifecycle events
- **UnboundedChan Integration**: Context-aware queue-based message processing with merging capability

### Message Processing Flow
1. Messages arrive via `SendMessage()` and are queued with optional merging
2. Background worker processes messages sequentially from the queue using context-aware `Receive(ctx)`
3. Context cancellation is properly handled without custom goroutines
4. Each message type (Start/Stop/Read) is handled by dedicated methods
5. User handlers are called with proper event objects
6. Responses are sent back to server via MessageSender interface

### Context Integration
- **Simplified Processing**: Uses `UnboundedChan.Receive(ctx)` for direct context integration
- **No Custom Goroutines**: Eliminated complex workarounds for context handling
- **Clean Error Handling**: Distinguishes between context cancellation and queue closure
- **Immediate Cancellation**: Context cancellation is respected immediately during message receiving

### Message Merging Strategy
- **ReadResponse Merging**: Multiple consecutive ReadResponse messages are merged to reduce user handler calls
- **Non-mergeable Types**: Start/Stop requests are never merged to preserve event semantics
- **Performance Benefit**: Reduces overhead during high-throughput scenarios

### Enhanced Merge Safety with Metadata Validation
- **Hierarchical Metadata Comparison**: Messages are only merged when ServerMessageMetadata is identical at all levels
- **Status Code Validation**: StatusCode fields must match exactly using StatusCode.Equals() method
- **Issues Validation**: Issues collections must be identical using hierarchical Issues.Equals() comparison
- **Nested Issue Validation**: Deep comparison of nested issue structures ensures complete metadata compatibility
- **Merge Prevention**: Messages with different metadata are processed separately to maintain data integrity
- **Safety Guarantee**: No message merging occurs when metadata differs, preventing loss of important status/error information
- **Performance Impact**: Minimal overhead from metadata comparison due to efficient hierarchical delegation pattern
- **Fail-Safe Approach**: When metadata comparison is uncertain, merging is prevented to ensure correctness

### Metadata Comparison Implementation
- **Hierarchical Delegation**: ServerMessageMetadata.Equals() delegates to nested structure Equals() methods
- **Nil Safety**: All comparison methods handle nil pointer cases gracefully at every level
- **Deep Comparison**: Nested Issues structures are compared recursively using their own Equals() methods
- **Type Safety**: Comparison is type-safe and handles all field types correctly
- **Efficient Implementation**: Comparison stops at first difference for optimal performance

### Worker Lifecycle Management
- **Start**: Initializes background worker and begins message processing
- **Processing**: Handles context cancellation and queue closure gracefully with built-in context support
- **Stop**: Closes queue and waits for background worker completion
- **Error Handling**: All errors propagated via WorkerStoppedCallback

## background.Worker Integration
- **Current Usage**: 
  - Manages message sending loop
  - Handles message receiving loop
  - Manages committer operations
  - **PartitionWorker Integration**: Each worker uses background.Worker for lifecycle management
- **Shutdown Process**:
  - Graceful shutdown with context cancellation
  - Waits for all background tasks to complete
  - Handles error propagation during shutdown
  - **PartitionWorker Shutdown**: Queue closure triggers worker termination with context awareness 