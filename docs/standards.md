# Coding standards

Use these examples when reviewing a signature, a memory lifetime, or the
placement of a test. The examples come from the SDK source.

## Ownership

- Pair an owning handle or iterator with `deinit` and name it in the doc
  comment. `WorkflowExecutionIterator` owns its query and page token
  through the client's allocator and tells the caller to call `deinit`.
  [src/client.zig](../src/client.zig)
- Document a resource's lifecycle method at creation. `Client.connect` tells
  the caller to release the returned client with `close`.
  [src/client.zig](../src/client.zig)
- For data returned in a caller's arena, say so in the doc comment. The caller
  releases the arena when done with the data, as with `Client.fetchHistory`.
  [src/client.zig](../src/client.zig)
- For a borrowed view, name the owner and how long the view is usable. `Kind`
  from the failure module borrows strings and payloads for the life of the
  source failure's memory. [src/failure.zig](../src/failure.zig)
- A result can mix owned and borrowed fields. `payloadWithEncoding` documents
  that the metadata list comes from `arena` and the data and encoding still
  borrow their inputs. [src/converter.zig](../src/converter.zig)
- At the C boundary, keep `TemporalCoreByteArrayRef` inputs alive through the
  core's documented last use, which may be the return from a synchronous call
  or a later callback. `Connection.call` keeps its request and metadata valid
  through the callback, including on cancellation.
  [src/bridge/Connection.zig](../src/bridge/Connection.zig)
- Take ownership of each `TemporalCoreByteArray` returned by the core and free
  it exactly once with `temporal_core_byte_array_free`. `Owned.deinit` pairs
  the buffer with its core deallocator. [src/bridge/bytes.zig](../src/bridge/bytes.zig)
- Treat the core callback as a handoff from another thread and document which
  side can clean up. `Completion.wait` gives the result to the waiter on success
  and leaves cleanup of the cell and result to the callback on abandonment.
  [src/bridge/completion.zig](../src/bridge/completion.zig)
- Callback state accessed concurrently must be thread-safe, as `GrpcOverride`
  documents for its handler and context. [src/bridge/Connection.zig](../src/bridge/Connection.zig)

## Error sets

- Declare a narrow error set at application-facing SDK boundaries so callers
  can handle the outcomes of the operation. `Client.checkHealth` returns
  `CheckHealthError`. [src/client.zig](../src/client.zig)
- Use inferred `!T` for local helpers whose callers can follow the operations
  inside them, such as `registeredNames`. [src/worker.zig](../src/worker.zig)
- Test helpers can also infer the errors they forward, including public
  wrappers such as `WorkflowEnvironment.registerWorkflow`.
  [src/testing.zig](../src/testing.zig)
- Use `anyerror` where a callback adapter must carry arbitrary user errors.
  `Def(...).invoke` forwards errors from the registered activity's code.
  [src/activity.zig](../src/activity.zig)
- Translate core and server errors where the SDK can give them meaning.
  `Client.sendInContext` maps expected gRPC statuses to `NotFound` and
  `AlreadyExists`, other RPC failures to `RpcFailed`, and malformed responses
  to `InvalidResponse` while preserving `OutOfMemory` and `Canceled`.
  [src/client.zig](../src/client.zig)
- Return an error for a failed workflow or activity and expose the full
  Temporal failure through the future or handle. `ActivityFuture` returns
  `ActivityFailed` from `get` and exposes the details through `failure()`.
  [src/workflow.zig](../src/workflow.zig)
- Follow the lifetime of the data behind the accessor. `WorkflowRun.get`
  allocates both result and failure data in the caller's arena, which must
  still be live when reading the failure's fields. [src/client.zig](../src/client.zig)
- The low-level service call exposes the gRPC status as a specific error and
  writes its details to an optional output parameter. See `Client.callService`.
  [src/client.zig](../src/client.zig)
- Return an error for bad runtime input from callers, the server, or the core.
  `fromPayloadWithContext` returns `InvalidPayload` for malformed JSON and
  `UnknownEncoding` for an unrecognized payload encoding.
  [src/converter.zig](../src/converter.zig)

## Allocator passing

- Take an explicit `Allocator` when the caller controls individually released
  memory. `Owned.dupe` takes a `gpa` and gives the caller a copy that can
  outlive the core buffer. [src/bridge/bytes.zig](../src/bridge/bytes.zig)
- Take an `Allocator` named `arena` for results and scratch allocations the
  caller can release together. `copyChain` takes that allocator interface and
  deep-copies a failure's cause chain into it. [src/failure.zig](../src/failure.zig)
- In workflow code use `Context.allocator()` for memory that lives while the
  run is cached. Inside a query handler it gives you query memory, released
  after the answer is sent. [src/workflow.zig](../src/workflow.zig)
- In activity code use `Context.allocator()` for memory that lives until the
  attempt's result is reported. [src/activity.zig](../src/activity.zig)
- Keep the allocator on a long-lived object for allocations the object owns.
  `Worker.init` stores its `gpa` for worker state such as registrations and
  the copied task queue. [src/worker.zig](../src/worker.zig)
- Bridge wrappers can take an allocator per call for temporary work and owned
  return data. `Connection.call` builds its request metadata in a scratch arena
  and copies the reply into memory from the supplied `gpa`.
  [src/bridge/Connection.zig](../src/bridge/Connection.zig)

## Test layout

- Put focused tests beside the code, as with `newlineMap`, or put tests that
  exercise an API across modules in a `src/*_test.zig` file.
  [src/bridge/bytes.zig](../src/bridge/bytes.zig)
- The root test block uses `refAllDecls` to reach public modules such as
  `activity` and references the private `bridge` module explicitly. Import
  standalone suites in that block, as for `client_service_test.zig`, so
  `zig build test` discovers them.
  [src/temporal.zig](../src/temporal.zig)
- Use `snake_case` for standalone test files. `zig build lint` checks namespace
  filenames, including the file with
  `test "every gRPC status returns its error, and the failure carries the status"`.
  [src/client_service_test.zig](../src/client_service_test.zig)
- Back test allocations and test arenas with `std.testing.allocator` so leaks
  fail the test, as in `test "newline maps join keys and values, and reject newlines in either"`.
  `zig build test-allocators` scans test bodies for other allocator names and
  supports per-test exceptions. [src/bridge/bytes.zig](../src/bridge/bytes.zig)
- Use `WorkflowEnvironment.start` for tests that exercise a dev server's
  service behavior. Use `WorkflowEnvironment.startTimeSkipping` when a test
  needs to advance timers or retry delays quickly. [src/testing.zig](../src/testing.zig)
- For workflow runtime changes, replay a recorded history to verify that the
  commands still match. `WorkflowReplayer.replayHistoryJson` drives the
  continue-as-new replay test, which also checks that an incompatible workflow
  returns `Nondeterminism`. [src/testing.zig](../src/testing.zig), [src/workflow_api_test.zig](../src/workflow_api_test.zig)
- Wait for the condition the test needs and avoid fixed wall-clock sleeps for
  synchronization. `Worker.waitUntilRunning` waits for polling to start or
  reports a failed start.
  [src/worker.zig](../src/worker.zig)
