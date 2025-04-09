# Coordination Examples

Coordination Examples demonstrate how to implement various distributed consistency primitives using the 
`coordination.Client` API.

## Locks

Strictly speaking, this is an implementation of a [lease](https://en.wikipedia.org/wiki/Lease_(computer_science)), not a
lock in the traditional sense. 

Consider an application which is running a number of different instances. You want to ensure that some shared resource 
is accessed by only one instance at a time.

The `lock` application is an example of that instance. When it starts, it waits until the distributed lock is acquired.
When the application cannot consider the lock acquired anymore (for example, in the case of network issues) it stops 
working and tries to acquire the lock again.

You may start any number of applications, only one of them will consider itself the holder of the lock at a time.

Although we call it a lock, this mechanism **cannot be used to implement mutual exclusion by itself**. Session relies on 
physical time clocks. The server and the client clocks may be, and actually always are, out of sync. This results in a
situation where for example the server releases the lease but the client still assumes it owns the lease. However, 
leases can be used as optimization in order to significantly reduce the possibility of resource contention.

To start the application with the [YDB Docker database instance](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_docker) run

```bash
$ go build 
$ YDB_ANONYMOUS_CREDENTIALS=1 ./lock -ydb grpc://localhost:2136/local --path /local/test --semaphore lock
```

When you stop the application which is currently holding the semaphore, another one should immediately acquire the lock 
and start doing its work.

This example uses an ephemeral semaphore which is always acquired exclusive. The following pseudocode shows how it is 
used.

```go
for {
    session, err := db.Coordination().OpenSession(ctx, path)
    if err != nil {
		// The context is canceled.
        break		
    }

    lease, err := session.AcquireSemaphore(ctx, semaphore, coordination.Exclusive, options.WithEphemeral(true))
    if err != nil {
        session.Close(ctx)
        continue
    }

    // The lock is acquired. 
    go doWork(lease.Context())
	
    // Wait until the lock is released.
    <-lease.Context().Done():
	
    // The lock is not acquired anymore.
    cancelWork()
}
```

## Workers

This is an example of distributing long-running tasks among multiple workers. Consider there is a number of tasks that 
need to be processed simultaneously by a pool of workers. Each task is processed independently and the order of
processing does not matter. A worker has a fixed capacity that defines how many tasks it is ready to process at a time.

Any single task is associated with a Coordination Service semaphore. When the application starts, it grabs all task 
semaphores in order to acquire at least `capacity` of them. When it happens, it releases excessive ones and wait until 
the application finishes. Until then, it starts a new task for every acquired semaphore and waits until the number of 
running tasks becomes equal to the capacity of the worker.

To start the application with the [YDB Docker database instance](https://ydb.tech/en/docs/getting_started/self_hosted/ydb_docker) run

```bash
$ go build 
$ YDB_ANONYMOUS_CREDENTIALS=1 ./lock -ydb grpc://localhost:2136/local -path /local/test --semaphore-prefix job- --tasks 10 --capacity 4
```

This example uses ephemeral semaphores which are always acquired exclusive. However, in a real application you may 
want to use persistent semaphores in order to store the state of workers in attached data. The following pseudocode 
shows how it is used.

```go
for {
    session, err := db.Coordination().OpenSession(ctx, path)
    if err != nil {
        // The context is canceled.
        break
    }

    semaphoreCtx, semaphoreCancel := context.WithCancel(ctx)
    capacitySemaphore := semaphore.NewWeighted(capacity)
    leaseChan := make(chan *coordination.Lease)
    for _, name := range tasks {
        go awaitSemaphore(semaphoreCtx, semaphoreCancel, session, name, capacitySemaphore, leaseChan)
    }

    tasksStarted := 0
loop:
    for {
        lease := <-leaseChan
			
        // Run a new task every time we acquire a semaphore.
        go doWork(lease.Context())
        
        tasksStarted++
        if tasksStarted == capacity {
            break
        }
    }

    // The capacity is full, cancel all Acquire operations.
    semaphoreCancel()
	
    // Wait until the session is alive.
    <-session.Context().Done()
	
    // The tasks must be stopped since we do not own the semaphores anymore.
    cancelTasks()
}

func awaitSemaphore(
	ctx context.Context,
	cancel context.CancelFunc,
	session coordination.Session, 
	semaphoreName string,
	capacitySemaphore *semaphore.Weighted,
	leaseChan chan *coordination.Lease) {
    lease, err := session.AcquireSemaphore(
        ctx,
        semaphoreName,
        coordination.Exclusive,
        options.WithEphemeral(true),
    )
    if err != nil {
        // Let the main loop know that something is wrong.
        // ...
        return
    }

    // If there is a need in tasks for the current worker, provide it with a new lease.
    if capacitySemaphore.TryAcquire(1) {
        leaseChan <- lease
    } else {
        // This may happen since we are waiting for all existing semaphores trying to grab the first available to us.
        err := lease.Release()
        if err != nil {
            // Let the main loop know that something is wrong.
            // ...
        }
    }	
}
```
