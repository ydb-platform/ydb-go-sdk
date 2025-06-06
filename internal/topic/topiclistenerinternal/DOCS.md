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

## background.Worker Integration
- **Current Usage**: 
  - Manages message sending loop
  - Handles message receiving loop
  - Manages committer operations
- **Shutdown Process**:
  - Graceful shutdown with context cancellation
  - Waits for all background tasks to complete
  - Handles error propagation during shutdown 