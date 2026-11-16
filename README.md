# Temporal Zig SDK (unofficial) [![CI](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml) [![Samples](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml)

> [!WARNING]
> This is an unofficial, community-built SDK. Temporal Technologies does not maintain, support, or endorse it. It is in early development, its APIs will change without notice, and it is not ready for production use. For supported SDKs, see [Temporal's SDK documentation](https://docs.temporal.io/develop).

[Temporal](https://github.com/temporalio/temporal) is a distributed, scalable, durable, and highly available orchestration engine used to execute asynchronous long-running business logic in a scalable and resilient way.

The Temporal Zig SDK is a framework for authoring workflows and activities in Zig. It is built on Temporal's [Rust Core SDK](https://github.com/temporalio/sdk-core), the same core several of Temporal's official SDKs use. It is a standalone, Zig-first SDK: it aims for feature parity with Temporal's other SDKs, with every feature designed the Zig way.

## Contents

- [Quick start](#quick-start)
  - [Installation](#installation)
  - [Implementing a workflow](#implementing-a-workflow)
  - [Running a worker](#running-a-worker)
  - [Starting a workflow](#starting-a-workflow)
  - [Next steps](#next-steps)
- [Usage](#usage)
  - [Client](#client)
    - [Connecting](#connecting)
    - [Proxy, keep-alive, and worker heartbeats](#proxy-keep-alive-and-worker-heartbeats)
    - [Starting workflows](#starting-workflows)
    - [Signals, queries, and updates](#signals-queries-and-updates)
    - [Describing, listing, cancelling, and terminating](#describing-listing-cancelling-and-terminating)
    - [Schedules](#schedules)
    - [Standalone activities](#standalone-activities)
    - [Completing activities asynchronously](#completing-activities-asynchronously)
    - [Worker deployments](#worker-deployments)
    - [Workflow reset and execution options](#workflow-reset-and-execution-options)
    - [Namespaces and raw service access](#namespaces-and-raw-service-access)
  - [Workflows](#workflows)
    - [Definition](#definition)
    - [Determinism](#determinism)
    - [Activities and local activities](#activities-and-local-activities)
    - [Timers and conditions](#timers-and-conditions)
    - [Concurrency](#concurrency)
    - [Signals, queries, and updates](#signals-queries-and-updates-1)
    - [Child workflows](#child-workflows)
    - [External workflows](#external-workflows)
    - [Continue-as-new](#continue-as-new)
    - [Cancellation](#cancellation)
    - [Versioning and patching](#versioning-and-patching)
    - [Side effects, memo, and search attributes](#side-effects-memo-and-search-attributes)
    - [Sessions](#sessions)
    - [Dynamic workflows](#dynamic-workflows)
    - [Workflow info](#workflow-info)
    - [Failure policy](#failure-policy)
    - [Stack trace and metadata queries](#stack-trace-and-metadata-queries)
  - [Activities](#activities)
    - [Definition](#definition-1)
    - [Context](#context)
    - [Heartbeating and cancellation](#heartbeating-and-cancellation)
    - [Asynchronous completion](#asynchronous-completion)
    - [Failures](#failures)
    - [Dynamic activities](#dynamic-activities)
  - [Workers](#workers)
    - [Running and stopping](#running-and-stopping)
    - [Options](#options)
    - [Shutdown](#shutdown)
    - [Versioning](#versioning)
    - [Sessions](#sessions-1)
    - [Workflow memory limit](#workflow-memory-limit)
    - [Deadlock detection](#deadlock-detection)
    - [Tuners and poller behavior](#tuners-and-poller-behavior)
    - [Plugins](#plugins)
  - [Data conversion](#data-conversion)
    - [Payloads](#payloads)
    - [Payload codecs](#payload-codecs)
    - [Failures](#failures-1)
    - [External payload storage](#external-payload-storage)
    - [Remote codec client](#remote-codec-client)
  - [Testing](#testing)
    - [Workflow environment](#workflow-environment)
    - [Time skipping](#time-skipping)
    - [Activity environment](#activity-environment)
    - [Replaying histories](#replaying-histories)
    - [Metrics recorder](#metrics-recorder)
  - [Observability](#observability)
    - [Logging](#logging)
    - [Metrics](#metrics)
    - [Interceptors](#interceptors)
    - [Context propagation](#context-propagation)
    - [Tracing](#tracing)
  - [Nexus](#nexus)
    - [Services and handlers](#services-and-handlers)
    - [Calling operations from workflows](#calling-operations-from-workflows)
    - [Serving operations](#serving-operations)
    - [Standalone operations](#standalone-operations)
  - [Contrib](#contrib)
- [Parity](#parity)
- [Versioning](#versioning-1)
- [Development](#development)
  - [Prerequisites](#prerequisites)
  - [Building and testing](#building-and-testing)
  - [Generated code and the Rust core](#generated-code-and-the-rust-core)
  - [Repository layout](#repository-layout)
- [Contributing](#contributing)
- [License](#license)

## Quick start

This section builds a workflow that calls one activity, a worker that runs both, and a program that starts the workflow and prints its result.

### Installation

You need Zig 0.17.1. Add a release to your project's `build.zig.zon`:

```bash
zig fetch --save=temporal https://github.com/mikeacjones/temporal-sdk-zig/releases/download/v<version>/temporal-sdk-zig-<version>.tar.gz
```

and import the module in `build.zig`:

<!-- example: unchecked -->
```zig
const temporal = b.dependency("temporal", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("temporal", temporal.module("temporal"));
```

A release needs no Rust toolchain: its build links a prebuilt core library for your target, which Zig fetches on first use. Prebuilt libraries exist for `aarch64-macos`, `x86_64-macos`, `aarch64-linux-gnu`, `x86_64-linux-gnu`, `aarch64-linux-musl`, and `x86_64-linux-musl`; for any other target, the build stops and says so. [docs/releasing.md](docs/releasing.md) has the support policy, including the glibc the gnu libraries need. No release has been published yet; the [releases page](https://github.com/mikeacjones/temporal-sdk-zig/releases) lists them once there are.

To build from source instead, for another target or for unreleased changes, clone the SDK with its submodule next to your project and depend on it by path. The build then compiles the Rust core with cargo, so it needs a Rust toolchain installed through [rustup](https://rustup.rs), which installs the pinned Rust version on first use. You don't need `protoc`.

```bash
git clone --recursive https://github.com/mikeacjones/temporal-sdk-zig
```

<!-- example: unchecked -->
```zig
.dependencies = .{
    .temporal = .{ .path = "../temporal-sdk-zig" },
},
```

### Implementing a workflow

A workflow is deterministic code that orchestrates activities, and an activity is ordinary code that may do I/O and fail. `temporal.activity.Def` and `temporal.workflow.Def` name each one; workers register the resulting types and clients start them, so argument and result types are checked at compile time.

<!-- example: quickstart-workflow -->
```zig
const activity = temporal.activity;
const workflow = temporal.workflow;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

pub const Greet = activity.Def("greet", greet);

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return future.get();
}

pub const Greeting = workflow.Def("greeting", greeting);
```
Notes:
- Allocate an activity's result from `ctx.allocator()`, which lives until the result has been reported.
- `ctx.executeActivity` returns a future; `get` waits for the activity's result or error. `start_to_close_timeout` or `schedule_to_close_timeout` is required.
- Workflow code must stay deterministic: no threads, wall-clock time, randomness, or I/O outside the workflow's `std.Io`.
- The name given to `Def` ("greeting", "greet") is the type name the server records, not the Zig identifier.

### Running a worker

A worker polls one task queue and runs the workflows and activities registered with it. `temporal.client.Client.connect` opens a connection, and `temporal.worker.Worker.init` creates a worker on it.

<!-- example: quickstart-worker -->
```zig
pub fn runWorker(gpa: std.mem.Allocator, io: std.Io, options: temporal.client.Options) !void {
    var client = try temporal.client.Client.connect(gpa, io, options);
    defer client.close();

    var worker = try temporal.worker.Worker.init(gpa, io, &client, "greetings", .{});
    defer worker.deinit();
    try worker.registerWorkflow(Greeting);
    try worker.registerActivity(Greet);

    // Polls the task queue until the process gets SIGINT or SIGTERM.
    try worker.runUntilSignal();
}
```
Notes:
- In a program, `options` usually comes from `temporal.client.loadDefaultOptions(arena)`: the Temporal config file and `TEMPORAL_*` environment variables, defaulting to `localhost:7233` and the `default` namespace, where `temporal server start-dev` runs a local server.
- `Worker.runUntilSignal` returns after SIGINT or SIGTERM once in-flight work has finished. `Worker.run` runs until another thread calls `Worker.stop`. Both return an error instead when the worker can't start or polling fails for good; see [Running and stopping](#running-and-stopping).
- Registering a name twice returns `error.AlreadyRegistered`.
- The client must outlive its workers. Both are heavyweight: create them once per process.

### Starting a workflow

A client starts a workflow by its definition and gets back a handle to the run, which waits for the result.

<!-- example: quickstart-starter -->
```zig
pub fn startGreeting(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator, options: temporal.client.Options) ![]const u8 {
    var client = try temporal.client.Client.connect(gpa, io, options);
    defer client.close();

    var run = try client.executeWorkflow(Greeting, .{
        .id = "greeting-world",
        .task_queue = "greetings",
    }, .{"World"});
    defer run.deinit();

    // Waits for the workflow to complete; the result is allocated from `arena`.
    const result = try run.get(arena);
    std.log.info("workflow result: {s}", .{result});
    return result;
}
```
Notes:
- `get` waits for the workflow to close, following continue-as-new, and allocates the result from the allocator passed to it. Free the handle with `deinit`.
- A failed workflow is `error.WorkflowFailed` from `get`; read the failure with `run.failure()`.
- By default, starting a workflow ID that a running run holds, or a closed run that `id_reuse_policy` keeps from reuse, returns `error.WorkflowExecutionAlreadyStarted`. An empty `task_queue` returns `error.InvalidOptions` before any call.

### Next steps

- [samples](samples) has a complete program for each feature, each with tests that run against a dev server.
- [PARITY.md](PARITY.md) has the feature table, which says for each feature of Temporal's SDKs whether this SDK supports it, and the list of samples with the features they demonstrate.
- [Temporal's documentation](https://docs.temporal.io) explains the concepts: workflows, activities, task queues, and workers.
- The rest of this README documents the API area by area.

## Usage

The SDK's public API is the `temporal` module: `temporal.client`, `temporal.worker`, `temporal.workflow`, `temporal.activity`, `temporal.converter`, `temporal.testing`, and the smaller namespaces the sections below introduce. The API follows a few conventions throughout:

- Functions that allocate or do I/O take an explicit `std.mem.Allocator` and `std.Io`.
- Options are struct literals with defaults, not functional options or context derivation.
- Workflows, signals, queries, updates, and activities have comptime-typed definitions shared by the client and the worker, so calls check argument and result types at compile time.
- Workflow code runs on a deterministic `std.Io`: `io.sleep` is a durable timer, `io.now` is workflow time, and `std.Io` futures, `Select`, `Mutex`, and `Queue` work inside workflows unchanged.

### Client

`temporal.client.Client` connects to one server and namespace. It starts and manages workflows, schedules, and standalone activities, and completes activities from outside a worker. Every handle it returns owns its IDs: free it with `deinit`.

#### Connecting

`Client.connect` takes `client.Options`: address, namespace, TLS, API key, headers, retry policy, codecs, interceptors, and more. `client.loadOptions` reads them from a config file profile and the environment; `client.loadDefaultOptions` uses the default profile.

<!-- example: client-connect -->
```zig
var cloud = try temporal.client.Client.connect(gpa, io, .{
    .host_port = "my-namespace.a1b2c.tmprl.cloud:7233",
    .namespace = "my-namespace.a1b2c",
    .api_key = api_key,
    .tls = .{}, // mutual TLS: .{ .client_cert_path = "client.pem", .client_private_key_path = "client.key" }
});
defer cloud.close();
// A profile of the Temporal config file, overridden by TEMPORAL_* environment variables.
var staging = try temporal.client.Client.connect(gpa, io, try temporal.client.loadOptions(arena, .{ .profile = "staging" }));
defer staging.close();
```
Notes:
- `connect` checks the connection. It returns `error.ConnectFailed`, `error.InvalidOptions`, or `error.RuntimeInitFailed` (sdk-core's runtime couldn't be created), logged with the reason, or `error.OutOfMemory`. Free the client with `close`.
- `api_key` alone doesn't turn TLS on; `loadOptions` does for a profile with an API key or TLS settings, and returns `error.InvalidConfig`, logged, for a named profile that doesn't exist (in a config file or a missing one), a config file that exists but can't be read or parsed, a key `strict` rejects, a certificate given as both a path and data, or both `disable_file` and `disable_env` set. A missing config file without a named profile isn't an error: its defaults are used. TLS file paths are read on each connect, so `Client.reconnect` picks up rotated certificates.
- `Options.rpc_timeout` (default 30 s) bounds each call, but not waits for a result. `connect` copies `namespace` and `identity`; `codecs`, `logger`, `propagators`, `worker_interceptors`, and `interceptors` are borrowed and must outlive the client and its workers.

`Client.init` takes the same options but connects on first use, so a process can build its client before the server is reachable. `Client.updateApiKey` and `Client.updateHeaders` rotate the API key and the gRPC headers of a client, connected or not.

<!-- example: client-lazy -->
```zig
// Nothing goes over the network yet: the first call that needs the server connects.
var client = try temporal.client.Client.init(gpa, io, options);
defer client.close();
// Optional: connect now, to see a failure here rather than on the first call.
try client.ensureConnected();
// Rotate the API key and headers: every later call, the workers' included, sends these.
try client.updateApiKey(rotated_key);
try client.updateHeaders(&.{.{ .key = "x-tenant", .value = "blue" }});
```
Notes:
- `init` makes no network call, and an address nobody listens on doesn't fail it. It rejects the options `connect` rejects (header names and values, durations, a TLS certificate given as both PEM and path) with `error.InvalidOptions`, logged; TLS files are read when the client connects. It returns `error.RuntimeInitFailed` or `error.OutOfMemory` as `connect` does.
- The first call that needs the server connects and checks it: a workflow, schedule, or activity call, `checkHealth`, `currentConnection`, `ensureConnected`, or `worker.Worker.run` on the client. Calls made together from several threads share one attempt, and each waits for it only until its own deadline, `Options.rpc_timeout` (30 s without one) from when the call began, so a server that accepts connections but never answers fails every call within its deadline. Canceling a call's task returns `error.Canceled` from it (`Worker.run` returns `error.ConnectFailed` and keeps the cancel request), and `Worker.stop` ends `run`'s wait at once, after which `run` returns success. A later call joins the attempt if it is still running, and starts another once no call waits for it. `updateApiKey` and `updateHeaders` don't wait, and what they change is sent on the new connection. `ensureConnected` connects explicitly and returns `client.ConnectError`.
- A first connection that fails is logged as a warning with the reason and leaves the client unconnected, so the next call tries again. `ensureConnected` and `currentConnection` return `error.ConnectFailed` (or `error.InvalidOptions` for a TLS file that can't be read), the client's other calls `error.RpcFailed`, and `Worker.run` `error.ConnectFailed`.
- `updateApiKey` replaces the API key every later call sends, this client's and its workers'; empty sends none. `updateHeaders` replaces `Options.headers` the same way, and returns `error.InvalidOptions`, logged, for a header name that isn't a gRPC ASCII header (or ends in `-bin`) or a value with bytes other than visible ASCII, space, and tab, leaving the headers unchanged. Calls already sent keep what they had. On a client that hasn't connected yet, they change what it connects with.
- `Client.reconnect` connects again with its own `client.ConnectionOptions`, so after it the client sends that `api_key` and those `headers`, not those of earlier updates. On a client that hasn't connected yet, `reconnect` makes the first connection with its options, and calls waiting for a first connection get it; if that fails the client stays unconnected and keeps its own. `checkHealth` on such a client connects first. `close` frees a client whether it connected or not, without waiting on the network. If every call gave up on a first connection that sdk-core is still making (sdk-core gives it no deadline, and a server that never answers a TLS handshake keeps it going for good), sdk-core still uses the client's runtime, so `close` leaves the runtime and that attempt allocated for the rest of the process instead of freeing them. sdk-core's logs stop reaching `Options.logger`, but a custom metrics handler stays in use by that runtime and must then outlive the process.

A program that works in several namespaces of one server connects once: `Client.forNamespace` returns a client for another namespace that uses the existing client's connection and sdk-core runtime.

<!-- example: client-namespaces -->
```zig
var client = try temporal.client.Client.connect(gpa, io, options);
defer client.close();
// Another namespace of the same server, over the same connection and runtime.
var billing = try client.forNamespace("billing");
defer billing.close();
// Its calls, and workers on it, work in "billing".
const open_invoices = try billing.countWorkflows("ExecutionStatus = 'Running'");
```
Notes:
- `forNamespace` makes no network call and creates no connection or runtime; it returns only `error.OutOfMemory`. The new client copies the namespace, and has the first client's identity, codecs, logger, propagators, interceptors, metrics, and `rpc_timeout`, borrowed from the same `Options`. Workers on it poll its namespace. A client made from it shares the same connection too.
- The clients own the connection and the runtime together: `close` on one leaves them to the others, and the last to close frees them, so they may be closed in any order, each once, after the calls and workers on it have finished. sdk-core's logs go to the first client's `Options.logger` until then.
- `reconnect`, `updateApiKey`, and `updateHeaders` on any of them change the connection of all of them, and `reconnect` moves the workers running on any of them. A client from `init` shares its first connection the same way: whichever client needs the server first connects for all.
- Each `Client.connect` and `Client.init` still creates its own runtime: clients of different servers don't share one.

`Client.withRpcOptions` gives a single call, or a group of calls, its own deadline and gRPC metadata without changing `client.Options`: it returns a view of the client whose calls send `client.RpcOptions`.

<!-- example: client-rpc-options -->
```zig
// A view of the client whose calls get their own deadline and extra gRPC metadata.
var quick = try client.withRpcOptions(.{
    .timeout = .fromSeconds(2),
    .metadata = &.{.{ .key = "x-request-id", .value = request_id }},
    .binary_metadata = &.{.{ .key = "x-trace-bin", .value = &.{ 0x01, 0x02 } }},
});
// Calls through it, and the handles they return, send those; `client`'s own calls don't.
const running = try quick.countWorkflows("ExecutionStatus = 'Running'");
```
Notes:
- Every call made through the view sends its options: workflow start, signal, query, update, describe, list and count, cancel, terminate and reset, history, schedules, worker deployments, standalone activities, asynchronous activity completion and heartbeats, `checkHealth`, and the calls of the handles and iterators the view returns.
- A view is for single calls: build workers on the client, not on a view. A worker built on a view keeps the view, so `ctx.client()` in its activities and Nexus handlers returns the view and their calls send its timeout and metadata; on a client from `Client.init` that hasn't connected yet, the worker's first connection waits only as long as the view's timeout.
- `RpcOptions.timeout` replaces `Options.rpc_timeout` for each call (null keeps it). A call that runs out fails with `error.RpcFailed`, logged with gRPC status 1 ("Timeout expired") or 4 (DEADLINE_EXCEEDED). As with `rpc_timeout`, calls that long-poll aren't limited by it: waiting for a workflow's or an activity's result with `get`, for an update to reach its stage or its outcome, and following history with `wait_new_event`. Cancel the calling task to stop those.
- `metadata` (ASCII) and `binary_metadata` (names ending in `-bin`, any bytes) are sent along with `Options.headers` and the API key, the long-polls included; for a name in both, the view's value is sent, and an `authorization` entry replaces the API key's. `withRpcOptions` returns `error.InvalidOptions`, logged, for a timeout that isn't positive or a name or value gRPC can't send.
- The view borrows the client and the options' slices, which must outlive it and the handles it returns. It owns nothing: `close` on it does nothing. `forNamespace` on a view returns a full client without the view's options; a view of a client from `forNamespace` works in that client's namespace.

#### Proxy, keep-alive, and worker heartbeats

Three more `client.Options` fields shape the connection and what workers send over it: `http_connect_proxy` routes the connection through an HTTP CONNECT proxy, `keep_alive` sets the gRPC keep-alive pings, and `worker_heartbeat_interval` sets how often workers report themselves to the server.

<!-- example: client-transport -->
```zig
var client = try temporal.client.Client.connect(gpa, io, .{
    .host_port = "temporal.internal:7233",
    // Tunnel the connection through an HTTP CONNECT proxy, with basic auth (both or neither).
    .http_connect_proxy = .{ .host_port = "proxy.corp:3128", .username = "svc", .password = proxy_password },
    // Ping every 10 s and drop the connection when a ping goes unanswered for 5 s.
    .keep_alive = .{ .interval = .fromSeconds(10), .timeout = .fromSeconds(5) },
    // Workers on this client heartbeat to the server every 30 s; `.zero` turns that off.
    .worker_heartbeat_interval = .fromSeconds(30),
});
defer client.close();
```
Notes:
- `http_connect_proxy` (default null: connect directly) sends the connection to `host_port` through an HTTP CONNECT tunnel; TLS, when set, still runs end to end with the server inside the tunnel. `username` and `password` send basic auth to the proxy and go together: set both or neither. An empty `host_port`, or only one of the credentials, makes `connect` and `init` return `error.InvalidOptions`, logged. Set it to null to connect directly.
- `keep_alive` defaults to a ping every 30 s, sent even while the connection is idle; a ping not answered within 15 s drops the connection, and later calls connect again. Both durations must be positive, or `connect` and `init` return `error.InvalidOptions`, logged. Set `keep_alive` to null to send no pings.
- `worker_heartbeat_interval` (default null: every 60 s) applies to every worker on the client's sdk-core runtime, so clients from `forNamespace` share it. It must be from 1 s to 60 s, or `.zero` to turn worker heartbeats off; any other value makes `connect` and `init` return `error.InvalidOptions`, logged. Workers send heartbeats only to a server whose namespace reports support for them, and skip them silently otherwise.
- `Client.reconnect` takes a new `http_connect_proxy` and `keep_alive` in `client.ConnectionOptions`; `worker_heartbeat_interval` stays as `connect` or `init` set it.

#### Starting workflows

`Client.executeWorkflow` starts a workflow defined with `workflow.Def` and returns a `client.WorkflowRun`. `client.StartWorkflowOptions` sets the ID, task queue, timeouts, ID policies, retry policy, memo, search attributes, priority, and start delay.

<!-- example: client-start -->
```zig
var run = try client.executeWorkflow(Greeting, .{ .id = "greeting-ada", .task_queue = "greetings", .execution_timeout = .fromSeconds(60) }, .{"Ada"});
defer run.deinit();
const greeting = try run.get(arena);
// By type name, giving the result type. No ID: the client generates a UUID.
var by_name = try client.executeWorkflowByName([]const u8, "greeting", .{ .task_queue = "greetings" }, .{"Grace"});
defer by_name.deinit();
_ = try by_name.get(arena);
```
Notes:
- `id_conflict_policy` applies while a run with the ID is open: `.unspecified` and `.fail` return `error.WorkflowExecutionAlreadyStarted` (with `error_when_already_started = false`, a handle to the run holding the ID instead), `.use_existing` returns the running run, `.terminate_existing` replaces it.
- `get` returns `error.WorkflowFailed`, `WorkflowCanceled`, `WorkflowTerminated`, or `WorkflowTimedOut`. Only after `WorkflowFailed` does `run.failure()` hold a failure; after the other three it is null.
- `Client.getWorkflow` makes a handle to an existing workflow without a call; an empty run ID follows the latest run.

#### Signals, queries, and updates

`workflow.SignalDef`, `QueryDef`, and `UpdateDef` name a message and its signature; the workflow (here `Greeter`, whose handlers are declared as in the workflow section) handles it and the client sends it by the same type. Signal-with-start and update-with-start start the workflow first if it isn't running.

<!-- example: client-messages -->
```zig
pub const GreeterWorkflow = workflow.Def("greeter", Greeter);
pub const Finish = workflow.SignalDef("finish", fn () void);
pub const CurrentGreeting = workflow.QueryDef("current_greeting", fn () []const u8);
pub const SetGreeting = workflow.UpdateDef("set_greeting", fn (greeting: []const u8) []const u8);

pub const Replies = struct { previous: []const u8, current: []const u8, result: []const u8 };

pub fn sendMessages(client: *temporal.client.Client, arena: Allocator) !Replies {
    var run = try client.executeWorkflow(GreeterWorkflow, .{ .id = "greeter-1", .task_queue = "greetings" }, .{"World"});
    defer run.deinit();
    var update = try client.updateWorkflow(SetGreeting, run.id, "", .{"Hi"}, .{});
    defer update.deinit();
    const previous = try update.get(arena); // the previous greeting, "Hello"
    const current = try client.queryWorkflow(CurrentGreeting, arena, run.id, "", .{}); // "Hi"
    try client.signalWorkflow(Finish, run.id, "", .{});
    var signaled = try client.signalWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-2", .task_queue = "greetings" }, .{"Zig"}, Finish, .{});
    defer signaled.deinit();
    var updated = try client.updateWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-3", .task_queue = "greetings", .id_conflict_policy = .use_existing }, .{"Ziggy"}, SetGreeting, .{"Howdy"}, .{});
    defer updated.deinit();
    return .{ .previous = previous, .current = current, .result = try run.get(arena) }; // result: "Hi, World!"
}
```
Notes:
- An empty run ID targets the latest run; a missing workflow is `error.NotFound`. `signalWorkflow` returns once the server has recorded the signal.
- A failed query, or one with no handler, is `error.QueryFailed`. `updateWorkflow` waits for `UpdateOptions.wait_for_stage` (`.completed` by default). A rejected update is `error.UpdateFailed` from the handle's `get`, with the reason in `failure()`.
- `updateWithStartWorkflow` requires `id_conflict_policy`. `signalWithStartWorkflow` ignores the start arguments when the workflow is running.

`updateWorkflow`, `updateWithStartWorkflow`, and the handle's `get` wait on the server for as long as the update takes: `Options.rpc_timeout` doesn't limit them. To bound the wait, cancel the task that waits, here by racing it against a timeout. The wait ends with `error.Canceled` and the update goes on in the workflow; `getWorkflowUpdateHandle` with the same update ID reads its result later. The handle from `updateWithStartWorkflow` carries the workflow ID and the run it started or found, so `getWorkflow` waits for that workflow's result without another call.

<!-- example: client-update-wait -->
```zig
const UpdateWaitError = temporal.client.UpdateError || temporal.client.UpdateResultError;
const Reply = union(enum) {
    greeting: UpdateWaitError![]const u8,
    timeout: Io.Cancelable!void,
};

fn setGreeting(client: *temporal.client.Client, arena: Allocator, workflow_id: []const u8, greeting: []const u8) UpdateWaitError![]const u8 {
    var update = try client.updateWorkflow(SetGreeting, workflow_id, "", .{greeting}, .{ .update_id = "set-greeting-1" });
    defer update.deinit();
    return update.get(arena);
}

/// The previous greeting, or null when the update didn't complete within `timeout`. The
/// update goes on in the workflow either way.
pub fn setGreetingWithin(client: *temporal.client.Client, io: Io, arena: Allocator, workflow_id: []const u8, timeout: Io.Duration) !?[]const u8 {
    var buffer: [2]Reply = undefined;
    var select: Io.Select(Reply) = .init(io, &buffer);
    defer select.cancelDiscard(); // cancels the task still waiting
    try select.concurrent(.greeting, setGreeting, .{ client, arena, workflow_id, "Hey" });
    try select.concurrent(.timeout, Io.sleep, .{ io, timeout, .awake });
    return switch (try select.await()) {
        .greeting => |previous| try previous,
        .timeout => null,
    };
}

/// Updates the workflow `greeter-4`, starting it first if it isn't running, then waits for
/// that workflow's result.
pub fn updateThenWait(client: *temporal.client.Client, arena: Allocator) ![]const u8 {
    var update = try client.updateWithStartWorkflow(GreeterWorkflow, .{ .id = "greeter-4", .task_queue = "greetings", .id_conflict_policy = .use_existing }, .{"Lin"}, SetGreeting, .{"Hey"}, .{});
    defer update.deinit();
    // The handle names the run that the call started or found.
    var run = try client.getWorkflow([]const u8, update.workflow_id, update.run_id);
    defer run.deinit();
    try client.signalWorkflow(Finish, run.id, run.run_id, .{});
    return run.get(arena); // "Hey, Lin!"
}
```

#### Describing, listing, cancelling, and terminating

`Client.describeWorkflow` reports a run's status and times, and `Client.fetchHistory` reads its events. `Client.listWorkflows` and `Client.countWorkflows` take a visibility query. `Client.cancelWorkflow` asks a workflow to stop, and `Client.terminateWorkflow` stops it with no cleanup.

<!-- example: client-manage -->
```zig
var run = try client.executeWorkflow(GreeterWorkflow, .{ .task_queue = "greetings" }, .{"Ada"});
defer run.deinit();
const description = try client.describeWorkflow(arena, run.id, run.run_id);
const history = try client.fetchHistory(arena, run.id, run.run_id);
std.log.info("{t}, {d} events", .{ description.status, history.events.items.len });

var it = try client.listWorkflows("WorkflowType = 'greeter' AND ExecutionStatus = 'Running'", .{});
defer it.deinit();
while (try it.next(arena)) |info| std.log.info("running: {s}", .{info.workflow_id});
std.log.info("{d} in all", .{try client.countWorkflows("WorkflowType = 'greeter'")});

try client.cancelWorkflow(run.id, run.run_id, "no longer needed");
```
Notes:
- Results are allocated from the arena passed in. A description's `raw` is the full server response, and a listed workflow's `raw` is the server's record of that execution.
- Visibility is eventually consistent. An invalid query is `error.RpcFailed`, logged with the server's reason.
- A cancelled workflow sees `error.Canceled` from its next cancelable wait and may clean up first. `Client.historyIterator` reads history a page at a time, optionally waiting for new events.
- `Client.terminateWorkflowWithOptions` records details (any value the converter encodes, or `{}` for none) in the terminated event. With `TerminateOptions.first_execution_run_id`, it terminates only the chain of runs that started with that run; another chain is `error.NotFound`, and the workflow keeps running.
<!-- stub: temporal-sdk-zig-d51.5.30 -->
- Not implemented yet: a first-execution run ID on `cancelWorkflow`, and links on a termination.

#### Schedules

A schedule starts a workflow on a calendar, at intervals, or by cron expression. `Client.createSchedule` takes a `temporal.schedule.Schedule` and returns a `client.ScheduleHandle`.

<!-- example: client-schedule -->
```zig
var handle = try client.createSchedule("daily-greeting", .{ .schedule = .{
    .action = try temporal.schedule.startWorkflow(arena, Greeting, .{"World"}, .{ .id = "daily-greeting", .task_queue = "greetings" }),
    .spec = .{ .calendars = &.{.{ .hour = &.{.{ .start = 9 }} }}, .time_zone_name = "Europe/Paris" },
} });
defer handle.deinit();
try handle.trigger(.unspecified); // once now, as well as daily at 9:00
try handle.pause("maintenance");
const description = try handle.describe(arena);
std.log.info("paused: {}", .{description.schedule.state.paused});
try handle.delete();
```
Notes:
- An ID in use returns `error.ScheduleAlreadyExists`; an invalid spec is `error.RpcFailed`, logged.
- `ScheduleHandle.update` is read-modify-write: its callback edits the description and returns a `schedule.Update`. `backfill` takes past periods' actions.
- `Client.getScheduleHandle` and `Client.listSchedules` find existing schedules. Deleting a schedule leaves its workflows running.

#### Standalone activities

`Client.executeActivity` runs an activity with no workflow: a worker polling the task queue runs it, and the server applies its timeouts and retries. It returns a `client.ActivityHandle`.

<!-- example: client-standalone-activity -->
```zig
var handle = try client.executeActivity(Greet, .{
    .task_queue = "greetings",
    .start_to_close_timeout = .fromSeconds(10),
}, .{"Ada"});
defer handle.deinit();
const greeting = try handle.get(arena);
const description = try handle.describe(arena);
std.log.info("{s} took {d} attempt(s)", .{ description.activity_type, description.attempt });
```
Notes:
- The server must enable standalone activities (`testing.DevServer.standalone_activity_args` on a dev server); otherwise the start is `error.RpcFailed`.
- `start_to_close_timeout` or `schedule_to_close_timeout` is required: the client doesn't check and sends the request as given; the server validates it, and a start it refuses is `error.RpcFailed`, logged. `get` returns `error.ActivityFailed`, `ActivityCanceled`, `ActivityTerminated`, or `ActivityTimedOut`, with `failure()`.
- `cancel` and `terminate` return `error.ActivityClosed` for a closed activity. `listActivities` and `countActivities` cover only standalone activities.

#### Completing activities asynchronously

An activity that finishes elsewhere returns `ctx.completeAsync()` after handing off its task token. Any process that can reach the server then completes it through the client.

<!-- example: client-async-completion -->
```zig
const Outbox = struct {
    gpa: Allocator,
    task_token: []u8 = &.{},
    ready: Io.Event = .unset,

    fn greetLater(o: *Outbox, ctx: *activity.Context, _: []const u8) ![]const u8 {
        o.task_token = try o.gpa.dupe(u8, ctx.info().task_token); // hand off a copy
        o.ready.set(ctx.io());
        return ctx.completeAsync(); // the activity stays open on the server
    }
};
pub const GreetLater = activity.Def("greet_later", Outbox.greetLater);
// Later, in any process that can reach the server:
pub fn finishGreeting(client: *temporal.client.Client, task_token: []const u8) !void {
    try client.completeActivity(task_token, "Hello, Lin!");
}
```
Notes:
- Copy `ctx.info().task_token`: it is freed with the activity's other memory.
- `failActivity`, `recordActivityHeartbeat` (true once cancellation is requested), and `reportActivityCancellation` cover the other outcomes; each has a `ById` variant taking `client.ActivityId`.
- Each call returns `error.NotFound` once the activity is no longer waiting. Local activities can't complete asynchronously.

#### Worker deployments

A Worker Deployment is a named set of workers that roll out workflow code a version at a time; a worker joins one with `worker.Options.deployment`.

Notes:
- `Client.workerDeployment(name)` returns a `client.WorkerDeploymentHandle` without a call. `describe`, `setCurrentVersion`, `setRampingVersion`, and the version methods manage routing.
- `Client.listWorkerDeployments` lists them, and `StartWorkflowOptions.versioning_override` pins one workflow to a version. The server's refusals are `error.RpcFailed`, logged.

#### Workflow reset and execution options

`Client.resetWorkflow` starts a new run of a workflow from an earlier point in a run's history, the end of a workflow task, and returns the new run's ID. `Client.updateWorkflowExecutionOptions` changes a running workflow's execution options, such as its versioning override.

<!-- example: client-reset -->
```zig
// Reset the latest run to the end of its first workflow task: the new run keeps the
// history up to there. Reading the run ID once makes both calls name the same run.
const run_id = (try client.describeWorkflow(arena, workflow_id, "")).run_id;
const history = try client.fetchHistory(arena, workflow_id, run_id);
const first_task = for (history.events.items) |event| {
    if (event.attributes) |attrs| if (attrs == .workflow_task_completed_event_attributes) break event.event_id;
} else return error.NoCompletedWorkflowTask;
const new_run_id = try client.resetWorkflow(arena, workflow_id, run_id, .{
    .workflow_task_finish_event_id = first_task,
    .reason = "greet again",
    .reapply_exclude = .{ .signals = true }, // the new run doesn't get the earlier `finish`
});
var new_run = try client.getWorkflow([]const u8, workflow_id, new_run_id);
defer new_run.deinit();
```

<!-- example: client-execution-options -->
```zig
// Auto-upgrade from the next workflow task on, whatever the workflow's definition says.
const options = try client.updateWorkflowExecutionOptions(arena, workflow_id, "", .{
    .versioning_override = .{ .set = .auto_upgrade },
});
std.log.info("versioning override: {s}", .{if (options.versioning_override) |o| @tagName(o) else "none"});
// Back to the definition's behavior.
_ = try client.updateWorkflowExecutionOptions(arena, workflow_id, "", .{ .versioning_override = .remove });
```
Notes:
- The reset point is the event ID of a workflow task's completed, failed, or timed-out event, or its started event, found with `Client.fetchHistory`; a non-positive one is `error.InvalidOptions`, and one the server refuses is `error.RpcFailed`. An unknown workflow or run is `error.NotFound`.
- A running base run ends terminated, as does the workflow's current run; a closed base run stays as it was. A handle that names the base run's ID, such as one `executeWorkflow` returned, doesn't follow the reset: its `get` returns `error.WorkflowTerminated` (or the run's own result if it had closed). A handle made with an empty run ID reads the latest run when `get` runs, so it gets the new run's result.
- Signals, updates, and Nexus events recorded after the reset point are reapplied to the new run unless `ResetOptions.reapply_exclude` excludes them. Don't rely on `request_id` to deduplicate: a repeated request may reset again.
- A reset is sent once, without the automatic retries other calls get. A transport failure is `error.RpcFailed` too, and after it the reset may or may not have happened: before resetting again, describe the base run, whose `raw.workflow_extended_info.reset_run_id` names the new run once it has been reset.
- In `WorkflowExecutionOptionsChanges`, `null` leaves an option as it is, `.remove` clears the override, and `.set` replaces it; changing nothing is `error.InvalidOptions`. A pin (`.set = .{ .pinned = version }`) needs a worker of that version to have polled the workflow's task queue, or it is `error.RpcFailed`; auto-upgrade needs none. A closed workflow is `error.NotFound`.
- The server's refusals are logged as warnings with its reason.
<!-- stub: temporal-sdk-zig-d51.5.30 -->
- Not implemented yet: operations applied after a reset, helpers that pick a reset point, and changing a workflow's priority or time-skipping configuration.

#### Namespaces and raw service access
<!-- stub: temporal-sdk-zig-d51.5.11 -->
Not implemented yet.
The client will register, describe, and update namespaces, and make typed calls to service methods it has no wrapper for.

### Workflows
#### Definition
A workflow is a function taking `*workflow.Context` first, or a struct whose fields are the run's state and whose `pub fn run` is the workflow; `workflow.Def` (or `workflow.DefWithOptions`) names it.

<!-- example: workflow-definition -->
```zig
const workflow = temporal.workflow;
const activity = temporal.activity;
fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const Greet = activity.Def("greet", greet);
const greet_options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, greet_options, .{name}).get();
}
pub const Greeting = workflow.Def("greeting", greeting);

pub const Greeter = struct {
    greeted: usize = 0,
    pub fn run(g: *Greeter, ctx: *workflow.Context, name: []const u8) ![]const u8 {
        g.greeted += 1;
        return ctx.executeActivity(Greet, greet_options, .{name}).get();
    }
};
pub const GreeterWorkflow = workflow.DefWithOptions("greeter", Greeter, .{ .versioning_behavior = .auto_upgrade });
```

Notes:
- `Args` and `Result` come from the signature; a bad signature or a struct field without a default is a compile error.
- Arguments, results, and `ctx.allocator()` memory live in the run's arena while the run stays cached.
- A returned error fails the workflow, `error.Canceled` cancels it, and `error.OutOfMemory` fails only the workflow task, which is retried.

#### Determinism
Workflow code runs again on replay and must make the same calls in the same order. `ctx.io()` is a `std.Io` that makes this hold:
- Coroutines run one at a time in FIFO order; `io.sleep` is a durable timer, `io.now` is workflow time, and `io.random` is seeded from the run.
- File, network, and process calls and secure randomness fail on it.
- Don't use threads, the wall clock, other randomness, unordered hash map iteration, or other I/O; use an activity or `ctx.sideEffect`.

#### Activities and local activities
`workflow.Context.executeActivity` schedules an activity and returns a `workflow.ActivityFuture`; `executeLocalActivity` runs a short one on the workflow's own worker.

<!-- example: workflow-activities -->
```zig
fn greetTwice(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    // Both start now; `get` waits for each result.
    const remote = ctx.executeActivity(Greet, .{
        .start_to_close_timeout = .fromSeconds(10),
        .retry_policy = .{ .maximum_attempts = 3 },
        .summary = "greet the caller", // shown on the scheduled event in the UI
        .priority = .{ .priority_key = 2, .fairness_key = "tenant-a" },
        .versioning_intent = .compatible, // with build-ID versioning: this workflow's build
    }, .{name});
    const local = ctx.executeLocalActivityByName([]const u8, "greet", .{
        .start_to_close_timeout = .fromSeconds(5),
        .summary = "greet a friend",
    }, .{"local friend"});
    return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ try remote.get(), try local.get() });
}
pub const GreetTwice = workflow.Def("greet_twice", greetTwice);
```

Notes:
- `start_to_close_timeout` or `schedule_to_close_timeout` is required. Without either, a local activity's `get` returns `error.InvalidOptions` (logged); `executeActivity` doesn't check and sends the command as given, leaving validation to the server.
- On `error.ActivityFailed`, `future.failure()` has the failure; returning the error fails the workflow with that failure.
- `future.cancel()`, or canceling the waiting coroutine, cancels the activity as `cancellation_type` says.
- `summary` (on `workflow.ActivityOptions` and `workflow.LocalActivityOptions`) is a single line the UI and CLI show on the activity's scheduled or marker event. It is sent as a JSON string payload through the worker's codecs; empty sends none.
- `priority` is a `workflow.Priority`; null uses the workflow's, and each zero field inherits the workflow's value. A value the server rejects fails the workflow task.
- `versioning_intent` (`workflow.VersioningIntent`: `unspecified`, `compatible`, `default`) picks the build an activity runs on, only on task queues with build-ID-based versioning rules. `unspecified` means `compatible` on the workflow's own task queue and `default` elsewhere.
- Adding any of the three to existing workflow code doesn't break replay of histories recorded without them.

#### Timers and conditions
`ctx.sleep` blocks on a durable timer, as `ctx.io().sleep` does, and takes `workflow.SleepOptions`: a `summary` the UI and CLI show on the timer's started event. `workflow.Context.await` blocks until a condition holds, and `awaitTimeout` gives up after a timeout.

<!-- example: workflow-timers -->
```zig
pub const Answer = workflow.SignalDef("answer", fn () void);
fn setTrue(b: *bool) void {
    b.* = true;
}
fn isTrue(b: *bool) bool {
    return b.*;
}
fn patientGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    try ctx.sleep(.fromSeconds(60 * 60), .{ .summary = "give them an hour" }); // a durable timer
    const answered = try ctx.allocator().create(bool); // handler state outlives this call
    answered.* = false;
    try ctx.setSignalHandler(Answer, answered, setTrue);
    if (try ctx.awaitTimeout(.fromSeconds(24 * 60 * 60), answered, isTrue)) return name;
    return "no answer";
}
pub const PatientGreeting = workflow.Def("patient_greeting", patientGreeting);
```

Notes:
- The condition reruns whenever all coroutines have blocked, so it must be cheap and change nothing.
- `awaitTimeout` returns false if the timeout passed first; both return `error.Canceled` if the coroutine is canceled.
- To race or cancel a timer, run `std.Io.sleep` or `workflow.Context.sleep` as a coroutine (`io.async`, or a `std.Io.Select` case, as the next example does): canceling the coroutine cancels the timer, and the sleep returns `error.Canceled`.
- The summary is sent as a JSON string payload through the worker's payload codecs; an empty one sends none, so the timer is the one `ctx.io().sleep` starts. A summary doesn't affect replay. `awaitTimeout`'s timer has no summary.

#### Concurrency
`ctx.io()` gives workflows `std.Io` concurrency: `io.async` futures, `std.Io.Select`, `std.Io.Group`, `std.Io.Mutex`, `std.Io.Semaphore`, `std.Io.Event`, `std.Io.Condition`, and `std.Io.Queue`. Blocked coroutines are woken in the order they started waiting, but a woken coroutine is not guaranteed to get what it waited for: a coroutine that releases a `std.Io.Mutex` or a `std.Io.Semaphore` permit and takes it again before it blocks gets it first, so code must not depend on the order in which coroutines acquire them. A coroutine canceled while blocked on any of them gets `error.Canceled`.

<!-- example: workflow-select -->
```zig
const Outcome = union(enum) { greeting: workflow.ActivityError![]const u8, timeout: std.Io.Cancelable!void };

fn greetWithin(ctx: *workflow.Context, name: []const u8, patience: std.Io.Duration) ![]const u8 {
    var buffer: [2]Outcome = undefined;
    var select: std.Io.Select(Outcome) = .init(ctx.io(), &buffer);
    defer select.cancelDiscard(); // cancels the loser: the activity or the timer
    select.async(.greeting, workflow.ActivityFuture([]const u8).get, .{ctx.executeActivity(Greet, greet_options, .{name})});
    select.async(.timeout, std.Io.sleep, .{ ctx.io(), patience, .awake });
    return switch (try select.await()) {
        .greeting => |result| try result,
        .timeout => "Nobody answered.",
    };
}
pub const GreetWithin = workflow.Def("greet_within", greetWithin);
```

<!-- example: workflow-group -->
```zig
fn greetInto(ctx: *workflow.Context, name: []const u8, out: *std.Io.Queue([]const u8)) void {
    const hello = ctx.executeActivity(Greet, greet_options, .{name}).get() catch "(no greeting)";
    out.putOne(ctx.io(), hello) catch {};
}
fn greetAll(ctx: *workflow.Context, names: []const []const u8) ![]const []const u8 {
    const io = ctx.io();
    const greetings = try ctx.allocator().alloc([]const u8, names.len);
    var buffer: [4][]const u8 = undefined;
    var queue: std.Io.Queue([]const u8) = .init(&buffer);
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (names) |name| group.async(io, greetInto, .{ ctx, name, &queue });
    for (greetings) |*g| g.* = try queue.getOne(io); // in completion order
    return greetings;
}
pub const GreetAll = workflow.Def("greet_all", greetAll);
```

A value one coroutine sets and another waits for is a field plus a `std.Io.Event`: the setter writes the field, then calls `set`; the waiter calls `wait`, then reads the field. This is the idiom; the SDK has no separate type for this.

<!-- example: workflow-event-value -->
```zig
const Reply = struct { value: []const u8 = "", ready: std.Io.Event = .unset };
fn fetchReply(ctx: *workflow.Context, name: []const u8, out: *Reply) void {
    ctx.setCoroutineName("fetch-reply") catch {}; // shown by stack-trace queries
    out.value = ctx.executeActivity(Greet, greet_options, .{name}).get() catch "(no greeting)";
    out.ready.set(ctx.io()); // after the value is written
}
fn greetWhenReady(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const io = ctx.io();
    var reply: Reply = .{};
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    group.async(io, fetchReply, .{ ctx, name, &reply });
    try reply.ready.wait(io);
    return reply.value;
}
pub const GreetWhenReady = workflow.Def("greet_when_ready", greetWhenReady);
```

Notes:
- Canceling a coroutine cancels what it waits on: its timer, activity, child workflow, or unrecorded external signal.
- Call `cancelDiscard`, `cancel`, or `await` before a `Select` or `Group` leaves scope.
- A `Select` case can receive from a channel: `select.async(.received, std.Io.Queue(T).getOne, .{ &queue, io })`. Canceling a receive that is still waiting takes nothing from the queue, but `cancelDiscard` drops the value of a receive that has already completed and not been awaited; use `cancel` in a loop to get those values back.
- To poll without blocking (a select's "default" case), use `queue.get(io, buffer, 0)`, which returns 0 when the queue is empty, or a future's `isReady()` (`ActivityFuture` and the other SDK futures), `Event.isSet()`, or `ctx.await` on workflow state.
- `ctx.setCoroutineName(name)` names the calling coroutine for stack-trace queries and copies `name` into `ctx.allocator()`; `ctx.coroutineName()` reads it. Unnamed coroutines are `workflow`, `signal`, `update`, `async`, or `group`.
- The workflow ends when its function returns, whatever coroutines are still running.

#### Signals, queries, and updates
`workflow.SignalDef`, `QueryDef`, and `UpdateDef` name messages that change, read, or change and answer a workflow's state; a struct workflow maps them to methods.

<!-- example: workflow-handlers -->
```zig
pub const AddGuest = workflow.SignalDef("add_guest", fn (name: []const u8) void);
pub const GuestCount = workflow.QueryDef("guest_count", fn () usize);
pub const Close = workflow.UpdateDef("close", fn (farewell: []const u8) usize);
pub const Party = struct {
    guests: usize = 0,
    closed: bool = false,
    pub const signals = .{ .addGuest = AddGuest };
    pub const queries = .{ .guestCount = GuestCount };
    pub const updates = .{ .close = .{ Close, .checkClose } };
    pub fn run(p: *Party, ctx: *workflow.Context) !usize {
        try ctx.await(p, isClosed);
        try ctx.await(ctx, workflow.Context.allHandlersFinished);
        return p.guests;
    }
    pub fn addGuest(p: *Party, _: []const u8) void {
        p.guests += 1;
    }
    pub fn guestCount(p: *Party) usize {
        return p.guests;
    }
    pub fn close(p: *Party, _: []const u8) usize {
        defer p.closed = true;
        return p.guests;
    }
    pub fn checkClose(_: *Party, farewell: []const u8) error{EmptyFarewell}!void {
        if (farewell.len == 0) return error.EmptyFarewell;
    }
    fn isClosed(p: *Party) bool {
        return p.closed;
    }
};
pub const PartyWorkflow = workflow.Def("party", Party);
```

Notes:
- Function workflows use `ctx.setSignalHandler`, `setQueryHandler`, and `setUpdateHandler(WithValidator)`, with a context from `ctx.allocator()`. `ctx.currentUpdate()` gives an update handler or validator its update's ID and name.
- Signal and update handlers run in their own coroutines and may block; query handlers and validators must only read state (see below).
- `ctx.setSignalHandlerByName` handles a signal named at run time, and `ctx.setDynamicSignalHandler` every signal without a handler.

`ctx.setDynamicQueryHandler(context, handler)` answers every query that has no handler of its own. The handler gets the query type and the arguments still encoded (`converter.EncodedValues`), like a dynamic signal handler, and returns the answer:

<!-- example: workflow-dynamic-query -->
```zig
const Lobby = struct {
    ctx: *workflow.Context,
    guests: usize = 0,
    fn addGuest(l: *Lobby, _: []const u8) void {
        l.guests += 1;
    }
    fn full(l: *Lobby) bool {
        return l.guests >= 2;
    }
    // Answers every query without a handler of its own; it may only read the lobby.
    fn anyQuery(l: *Lobby, name: []const u8, args: temporal.converter.EncodedValues) ![]const u8 {
        if (!std.mem.eql(u8, name, "welcome")) return error.UnknownQuery;
        const a = l.ctx.allocator();
        const guest = try args.get([]const u8, a, 0);
        return std.fmt.allocPrint(a, "Welcome, {s}! {d} here so far.", .{ guest, l.guests });
    }
};
fn lobby(ctx: *workflow.Context) !usize {
    const l = try ctx.allocator().create(Lobby); // outlives the workflow function
    l.* = .{ .ctx = ctx };
    try ctx.setSignalHandler(AddGuest, l, Lobby.addGuest);
    try ctx.setDynamicQueryHandler(l, Lobby.anyQuery);
    try ctx.await(l, Lobby.full);
    return l.guests;
}
pub const LobbyWorkflow = workflow.Def("lobby", lobby);
```

Notes:
- A handler registered for a query type with `setQueryHandler` (or a struct workflow's `queries`) still answers that type. Without a dynamic handler, a query of an unregistered type fails with `QueryNotRegistered`, naming the registered types.
- An error from the handler, or an argument that doesn't decode, fails the query with the error's name. Registering again replaces the dynamic handler; its context must outlive the run, and one on the calling coroutine's stack returns `error.QueryContextOnStack`.
- Query handlers and update validators must not change workflow state: history doesn't record queries or rejected updates, so a change would make the run differ from its replay. A call from one that would start an activity, timer, child workflow or any other command (`executeActivity`, `sleep`, `executeChildWorkflow`, `signalExternalWorkflow`, `upsertMemo`, `completeSession`, ...), register or remove a handler (`setSignalHandler`, `signalChannel`, `setUpdateHandler`, ...), take a signal from a channel (`receive`, `tryReceive`), change the run's settings (`setVersioningBehavior`), start a coroutine (`io.concurrent`), or block (`ctx.await`, a pending future's `get`) is refused: it returns `error.InvalidOptions` or `error.Canceled` (or does nothing, where it can't fail), the query fails or the update is rejected with a `ReadOnlyViolation` failure whose message names the call, whatever the handler returns, no command is sent, and the workflow task doesn't fail. A zero `sleep` returns at once, `io.async` runs its function inline, and `io.random` draws without moving the workflow's random sequence. `ctx.fail` and `ctx.failWith` describe the query's failure or the update's rejection, not the workflow's.
- `setCoroutineName` does nothing in a query handler or a validator run on delivery, which run outside any coroutine, so the query or update goes on. It is refused like the calls above in a validator that runs in a coroutine, which happens when the workflow function or a handler registers the handler of an update that is waiting for one: it would otherwise rename that coroutine.
- Query types Temporal's tools send (`__stack_trace`, `__enhanced_stack_trace`, and those starting with `__temporal`) don't go to the dynamic handler.
- In a query handler, `ctx.allocator()` is the query's memory, freed once the answer is sent, so decoding arguments there doesn't grow the run.
- Waits that can't fail can't be refused: an uncancelable wait that would block (such as `std.Io.Mutex.lockUncancelable` on a mutex the workflow holds), or awaiting or canceling a pending `std.Io.Future`, panics the worker in a query handler or a validator run on delivery. A validator run in a handler's coroutine (when the handler registers the update's handler) blocks only that coroutine, and its update is rejected with `ReadOnlyViolation` once the wait ends. Don't wait in them.
- A handler or validator that wakes a workflow coroutine through `std.Io` (`Event.set`, `Condition.signal`, `Queue.put`, `Mutex.unlock` on something a coroutine waits for) also fails the query or has the update rejected with `ReadOnlyViolation`. The wake itself still happens, since `std.Io` has already changed the primitive: the woken coroutine runs in the next workflow task that runs workflow code.
- The SDK can't undo a change a handler makes to workflow data or to a `std.Io` primitive, and doesn't detect one that wakes nothing; keeping to reads is the handler's job.
- The rules apply only to the handler's or validator's own code: other coroutines, including ones that run while a validator is blocked, are unaffected. A validator run in a coroutine neither sees nor uses up that coroutine's cancellation: `io.checkCancel` and a zero `sleep` return normally, and the cancel stays for the coroutine's own code.

`ctx.setDynamicUpdateHandler(context, handler)` takes every update that has no handler of its own, and `ctx.setDynamicUpdateHandlerWithValidator(context, handler, validator)` checks it first. Both get the update name and the arguments still encoded (`converter.EncodedValues`), and the handler returns the update's result:

<!-- example: workflow-dynamic-update -->
```zig
pub const CloseTally = workflow.SignalDef("close_tally", fn () void);
pub const Ping = workflow.SignalDef("ping", fn () void);
const Tally = struct {
    ctx: *workflow.Context,
    total: i64 = 0,
    closed: bool = false,
    // Takes every update without a handler of its own, in a coroutine of its own.
    fn anyUpdate(t: *Tally, name: []const u8, args: temporal.converter.EncodedValues) !i64 {
        const amount = try args.get(i64, t.ctx.allocator(), 0);
        t.total += if (std.mem.eql(u8, name, "subtract")) -amount else amount;
        return t.total;
    }
    // Runs before the update is accepted; its error rejects the update.
    fn checkUpdate(_: *Tally, name: []const u8, _: temporal.converter.EncodedValues) !void {
        if (!std.mem.eql(u8, name, "add") and !std.mem.eql(u8, name, "subtract")) return error.UnknownUpdate;
    }
    fn ping(t: *Tally) !void {
        try t.ctx.io().sleep(.fromSeconds(60), .awake);
    }
    fn close(t: *Tally) void {
        t.closed = true;
    }
    fn isClosed(t: *Tally) bool {
        return t.closed;
    }
};
fn tally(ctx: *workflow.Context) !i64 {
    const t = try ctx.allocator().create(Tally);
    t.* = .{ .ctx = ctx };
    try ctx.setDynamicUpdateHandlerWithValidator(t, Tally.anyUpdate, Tally.checkUpdate);
    try ctx.setSignalHandler(CloseTally, t, Tally.close);
    // A ping may still be sleeping when the tally closes; that is fine, so no warning.
    try ctx.setSignalHandlerWithOptions(Ping, t, Tally.ping, .{ .unfinished_policy = .abandon });
    try ctx.await(t, Tally.isClosed);
    return t.total;
}
pub const TallyWorkflow = workflow.Def("tally", tally);
```

Notes:
- The handler runs in a coroutine of its own, like a typed update handler: it may block, and `ctx.currentUpdate()` names the update there and in the validator. Its error rejects the update after acceptance, with the failure described as for a typed handler; the validator's error rejects it before acceptance, leaving no trace in history.
- A handler registered for an update name with `setUpdateHandler` (or a struct workflow's `updates`) still takes that name. Updates delivered before the dynamic handler is registered, in the same workflow task, reach it in arrival order. Without one, an update of an unregistered name is rejected with `UpdateNotRegistered`.
- An argument that doesn't decode is the handler's error (or the validator's) to return. Registering again replaces the dynamic handler; its context must outlive the run, and one on the calling coroutine's stack returns `error.UpdateContextOnStack`.

A workflow that completes, is canceled, or continues as new while signal or update handlers are still running cuts them short: their coroutines never resume, and an unfinished update gets no result. The SDK then logs one warning naming them, with the update names and IDs and the signal names. Wait for them with `try ctx.await(ctx, workflow.Context.allHandlersFinished)` before returning, or register a handler whose work may be cut short with `.unfinished_policy = .abandon` to leave it out of the warning:
- `ctx.setSignalHandlerWithOptions`, `setSignalHandlerByNameWithOptions`, `setDynamicSignalHandlerWithOptions`, `setUpdateHandlerWithOptions`, and `setDynamicUpdateHandlerWithOptions` take a `workflow.HandlerOptions` last; the update ones take the validator, or `null`, before it.
- A struct workflow declares it at the end of an entry: `.{ Ping, .{ .unfinished_policy = .abandon } }` for a signal, `.{ Close, .checkClose, .{ .unfinished_policy = .abandon } }` for an update.
- A workflow that fails logs no such warning, and handlers that have already returned aren't named.

A signal channel lets workflow code receive a signal where it needs it instead of in a handler. `ctx.signalChannel(Signal)` (or `ctx.signalChannelByName(name, Args)`) returns a `workflow.SignalChannel` that takes the place of the signal's handler:

<!-- example: workflow-signal-channel -->
```zig
const Arrival = union(enum) { guest: error{Canceled}![]const u8, quiet: std.Io.Cancelable!void };
fn greetArrivals(ctx: *workflow.Context) ![]const u8 {
    const io = ctx.io();
    const guests = try ctx.signalChannel(AddGuest); // takes the place of a handler
    var names: std.ArrayList([]const u8) = .empty;
    while (true) { // until a minute passes with no guest
        var buffer: [2]Arrival = undefined;
        var select: std.Io.Select(Arrival) = .init(io, &buffer);
        select.async(.guest, workflow.SignalChannel(AddGuest.Args).receive, .{guests});
        select.async(.quiet, std.Io.sleep, .{ io, .fromSeconds(60), .awake });
        const first = try select.await();
        select.cancelDiscard(); // one receive, awaited at once: no guest is dropped
        switch (first) {
            .guest => |name| try names.append(ctx.allocator(), try name),
            .quiet => break,
        }
    }
    ctx.removeSignalHandler(AddGuest); // later guests are buffered, unread
    const unread = try ctx.unhandledSignalNames(); // logged, and dropped, when the run ends
    if (unread.len > 0) ctx.logger().warn("signals left unread", .{ .first = unread[0] });
    return std.mem.join(ctx.allocator(), ", ", names.items);
}
pub const GreetArrivals = workflow.Def("greet_arrivals", greetArrivals);
```

Notes:
- `receive` blocks until a signal arrives and returns `error.Canceled` if its coroutine is canceled, so it works as a `std.Io.Select` case; a signal that arrives in the same workflow task as a raced timer wins the race. A receive that returned has taken its signal, and `cancelDiscard` drops every finished case but the one `await` returned: with receives on several channels, or a Select awaited only after its cases had time to finish, cancel with `select.cancel()` until it returns null and handle each result instead. `tryReceive` returns null instead of blocking, and `len` counts unread signals.
- A signal with one argument is received as that value, one with several as its argument tuple. Signals buffered before the channel was opened reach it in arrival order once the caller blocks; until then `len` and `tryReceive` don't see them, but `ctx.unhandledSignalNames()` lists them.
- `ctx.removeSignalHandler(Signal)`, `removeSignalHandlerByName`, and `removeDynamicSignalHandler` unregister a handler or channel: later signals go to the dynamic handler or are buffered, handler coroutines already running finish, and a channel keeps what it took.
- `ctx.unhandledSignalNames()` lists the names of the signals nothing has taken, buffered or unread in a channel, in first-arrival order; a workflow that finishes with any logs a warning counting them.

#### Child workflows
`workflow.Context.executeChildWorkflow` starts a child and returns a `workflow.ChildWorkflowFuture` for its start and result.

<!-- example: workflow-child -->
```zig
fn greetFamily(ctx: *workflow.Context, names: []const []const u8) ![]const []const u8 {
    const a = ctx.allocator();
    const children = try a.alloc(workflow.ChildWorkflowFuture([]const u8), names.len);
    for (names, children) |name, *child| {
        const id = try std.fmt.allocPrint(a, "{s}-{s}", .{ ctx.info().workflow_id, name });
        child.* = ctx.executeChildWorkflow(Greeting, .{ .id = id, .execution_timeout = .fromSeconds(60) }, .{name});
    }
    const greetings = try a.alloc([]const u8, names.len);
    for (children, greetings) |child, *out| out.* = try child.get();
    return greetings;
}
pub const GreetFamily = workflow.Def("greet_family", greetFamily);
```

Notes:
- `started` returns the child's execution; `error.ChildWorkflowAlreadyStarted` means its ID is taken.
- `get` returns `error.ChildWorkflowFailed` for a failed, timed-out, canceled, or terminated child, `future.failure()` saying which, and `error.ChildWorkflowAlreadyStarted` when the child's ID is taken.
- `parent_close_policy` defaults to `.terminate`; `future.signal` signals the child and `future.cancel()` cancels it.
- `static_summary` (a short single line) and `static_details` (longer text) describe the child; the UI and CLI show them on its started event. Each is sent as a JSON string payload through the worker's payload codecs, and empty sends none. They don't take part in replay.

#### External workflows
- `ctx.signalExternalWorkflow` and `ctx.cancelExternalWorkflow` signal or cancel a workflow by ID; an empty run ID targets the latest run.
- `get` returns `error.ExternalWorkflowNotFound` for a missing or closed target, `error.ExternalWorkflowRequestFailed` when the server refuses for another reason, `error.InvalidOptions` (logged) for an empty workflow ID, and `error.Canceled` if the waiting coroutine is canceled.
- The target runs in the same namespace; a child finds its parent in `ctx.info().parent`.

#### Continue-as-new
Returning `workflow.Context.continueAsNew` ends the run and starts a fresh one with the same workflow ID and new arguments.

<!-- example: workflow-continue-as-new -->
```zig
fn greetInBatches(ctx: *workflow.Context, names: []const []const u8, greeted: u32) !u32 {
    const batch = names[0..@min(names.len, 2)];
    for (batch) |name| _ = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    const total = greeted + @as(u32, @intCast(batch.len));
    if (batch.len < names.len) return ctx.continueAsNew(GreetInBatches, .{}, .{ names[batch.len..], total });
    return total;
}
pub const GreetInBatches = workflow.Def("greet_in_batches", greetInBatches);
```

Notes:
- Only the workflow function's return continues as new; from a handler the error fails the workflow or rejects the update.
- `ctx.continueAsNewSuggested()` says when the server suggests it; `historyLength` and `historySizeBytes` give the size.
- `ContinueAsNewOptions` fields left null keep this run's values; a client waiting on the result follows the chain.

#### Cancellation
A cancellation request reaches the workflow function as `error.Canceled` at its next cancelable wait.

<!-- example: workflow-cancellation -->
```zig
fn greetUntilCanceled(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    ctx.io().sleep(.fromSeconds(24 * 60 * 60), .awake) catch |err| switch (err) {
        error.Canceled => {
            _ = try ctx.executeActivity(Greet, greet_options, .{"cleanup"}).get();
            return error.Canceled; // the workflow ends canceled
        },
    };
    return ctx.executeActivity(Greet, greet_options, .{name}).get();
}
pub const GreetUntilCanceled = workflow.Def("greet_until_canceled", greetUntilCanceled);
```

Notes:
- Delivering the cancellation consumes it, so cleanup can still run activities and timers.
- Returning `error.Canceled` ends the workflow canceled; a result or another error completes or fails it.
- `ctx.io().swapCancelProtection(.blocked)` protects a section; `std.Io.recancel` re-arms a consumed cancellation.

#### Versioning and patching
`workflow.Context.patched` keeps old and new code paths until runs on the old one are gone; `getVersion` does the same with version numbers.

<!-- example: workflow-versioning -->
```zig
fn greetingV2(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const hello = if (ctx.patched("greet-with-activity"))
        try ctx.executeActivity(Greet, greet_options, .{name}).get()
    else
        try std.fmt.allocPrint(ctx.allocator(), "Hi, {s}.", .{name});
    const sign_off = switch (try ctx.getVersion("sign-off", workflow.default_version, 1)) {
        workflow.default_version => "",
        else => " Have a nice day.",
    };
    return std.fmt.allocPrint(ctx.allocator(), "{s}{s}", .{ hello, sign_off });
}
pub const GreetingV2 = workflow.Def("greeting_v2", greetingV2);
```

Notes:
- `patched` records a marker the first time; on replay it is true only if history has the marker.
- Retire a patch by replacing it with `ctx.deprecatePatch` and dropping the old path, then remove that call later.
- `getVersion` returns `workflow.default_version` for runs that passed before the change; `error.UnsupportedVersion` fails the task for retry.

#### Side effects, memo, and search attributes
`workflow.Context.sideEffect` records a nondeterministic value once; the memo and search attributes hold values about the workflow, the latter indexed.

<!-- example: workflow-side-effects -->
```zig
const greeted_name = temporal.search_attributes.key(.keyword, "CustomKeywordField");
var tickets: std.atomic.Value(u64) = .init(0);
fn nextTicket(_: void) u64 {
    return tickets.fetchAdd(1, .monotonic) + 1;
}
fn trackedGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const ticket = try ctx.sideEffect(u64, {}, nextTicket); // replay reads it from history
    const hello = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    try ctx.upsertMemo(.{ .last_greeting = hello });
    try ctx.upsertSearchAttributes(&.{greeted_name.set(name)});
    const language = try ctx.memoValue([]const u8, "language") orelse "en";
    return std.fmt.allocPrint(ctx.allocator(), "#{d} {s} ({s})", .{ ticket, hello, language });
}
pub const TrackedGreeting = workflow.Def("tracked_greeting", trackedGreeting);
```

Notes:
- The `sideEffect` function doesn't run on replay; it must not block or use the context, and returns a value, not an error.
- A null or empty value in `ctx.upsertMemo` removes the key; `memoValue` returns null for a missing key.
- An unregistered search attribute fails the workflow task until it exists; `error.InvalidSearchAttribute` means an unindexable value.

#### Sessions
`workflow.Context.createSession` opens a session that runs activities on one worker host, `ActivityOptions.session` runs one in it, and `completeSession` ends it.

<!-- example: workflow-session -->
```zig
fn greetOnOneHost(ctx: *workflow.Context, names: []const []const u8) ![]const u8 {
    const session = try ctx.createSession(.{
        .creation_timeout = .fromSeconds(60),
        .execution_timeout = .fromSeconds(10 * 60),
    });
    defer ctx.completeSession(session) catch {};
    const in_session: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10), .session = session };
    for (names) |name| _ = try ctx.executeActivity(Greet, in_session, .{name}).get();
    return session.info().host_name;
}
pub const GreetOnOneHost = workflow.Def("greet_on_one_host", greetOnOneHost);
```

Notes:
- The host is a worker with `worker.Options.sessions` set. `createSession` returns `error.SessionCreationFailed` when no host took the session within `creation_timeout` or the host refused it; [Sessions](#sessions-1) under Workers lists the cases and the other errors.
- A session fails when its host stops, misses heartbeats, or runs past `execution_timeout`; activities scheduled in it then return `error.SessionFailed`, and running ones aren't canceled.
- `ctx.recreateSession` opens a new session on an earlier session's host, from its `info().task_queue`.

#### Dynamic workflows
`workflow.DynamicDef` defines a workflow that runs every type the worker has no `Def` for; register it with `registerDynamicWorkflow`.

<!-- example: workflow-dynamic -->
```zig
fn anyGreeting(ctx: *workflow.Context, args: temporal.converter.EncodedValues) ![]const u8 {
    // Each workflow type may choose its own versioning behavior.
    const long_lived = std.mem.startsWith(u8, ctx.info().workflow_type, "long_");
    ctx.setVersioningBehavior(if (long_lived) .pinned else .auto_upgrade);
    const name = try args.get([]const u8, ctx.allocator(), 0);
    const hello = try ctx.executeActivity(Greet, greet_options, .{name}).get();
    return std.fmt.allocPrint(ctx.allocator(), "{s} ({s})", .{ hello, ctx.info().workflow_type });
}
pub const AnyGreeting = workflow.DynamicDef(anyGreeting);
```

Notes:
- `workflow.DynamicDefWithOptions` takes `task_failure_errors` for it; see [Failure policy](#failure-policy).
- A worker has at most one dynamic workflow, and a registered `Def` wins for its own type.
- Clients start it by type name with `executeWorkflowByName`; it reads that name from `ctx.info().workflow_type`.
- It registers handlers with the `Context` setters, as a function workflow does.

#### Workflow info
- `ctx.info()` returns the run's IDs, type, task queue, namespace, attempt, timeouts, parent, start time, retry policy, and priority.
- `ctx.isReplaying()` says whether the task replays history. `ctx.metrics()` records nothing during replay, and `ctx.logger()` drops its records then unless `worker.Options.log_during_replay` is set.
- `ctx.lastCompletionResult(T)` and `ctx.previousRunFailure()` read a cron or retried workflow's previous run.

#### Failure policy

An error that workflow code returns either fails the workflow, which closes the run, or fails the workflow task, which leaves the run open: the server retries the task, and the retry replays the history with whatever code the worker that takes it runs. By default `error.OutOfMemory` fails the task, `error.Canceled` cancels the workflow, and every other error fails the workflow, because a Zig workflow that returns an error means to fail. `worker.Options.workflow_task_failure_errors` lists errors that fail the task instead, for every workflow of a worker, and `workflow.DefOptions.task_failure_errors` lists them for one definition; an error either list names fails the task. `nondeterminism_fails_workflow`, on either, fails the workflow instead of the task when a run's code no longer matches its history.

<!-- example: workflow-failure-policy -->
```zig
fn carrierRate(carrier: []const u8) ?u32 {
    if (std.mem.eql(u8, carrier, "post")) return 5;
    if (std.mem.eql(u8, carrier, "courier")) return 12;
    return null;
}
fn shipOrder(ctx: *workflow.Context, carrier: []const u8) ![]const u8 {
    // A carrier missing from the table is a bug to fix and deploy, not a reason to fail the
    // order: listed, the error fails the workflow task, which is retried until fixed code runs.
    const rate = carrierRate(carrier) orelse return error.UnknownCarrier;
    return std.fmt.allocPrint(ctx.allocator(), "shipped by {s} for {d}", .{ carrier, rate });
}
pub const ShipOrder = workflow.DefWithOptions("ship_order", shipOrder, .{
    .task_failure_errors = &.{error.UnknownCarrier},
    // Fail this workflow, not just its task, if its code stops matching its history.
    .nondeterminism_fails_workflow = true,
});

// For every workflow of a worker.
const strict_worker: temporal.worker.Options = .{
    .workflow_task_failure_errors = &.{error.InvariantBroken},
    .nondeterminism_fails_workflow = true,
};
```

Notes:
- The lists apply to errors the workflow function, a signal handler, or an update handler returns, in function, struct, and dynamic workflows (`workflow.DynamicDefWithOptions`). They are slices of `anyerror`, compared by error value.
- The task failure is an application failure whose type is the error's name and whose message names the error and the function or handler that returned it. The run is evicted and the workflow stays running; a worker whose code no longer returns the error completes the retried task, and history keeps one failed task.
- An update handler's listed error doesn't reject or fail the update: the retried task runs the handler again and answers it. An update delivered in the failed task was never accepted, so the server delivers it again and its validator runs again. An update handler's unlisted errors, `error.OutOfMemory` included, and errors from update validators reject the update as before.
- Listing `error.Canceled` doesn't stop a cancellation: the workflow function or a signal handler that returns it cancels the workflow, listed or not. An update handler that returns a listed `error.Canceled` fails the task, where unlisted it rejects the update. Listing `error.ContinueAsNew` doesn't stop a continue-as-new the workflow function requested with `ctx.continueAsNew`; only returning it otherwise (without the request, or from a signal handler) then fails the task instead of the workflow.
- Without `nondeterminism_fails_workflow`, a nondeterministic run fails its task and waits for code that matches. With it, the run fails with the nondeterminism failure. The definition's option needs a workflow type name, so a dynamic workflow takes only the worker's (`DynamicDefWithOptions` rejects it at compile time).
- What happens to a failed workflow task: it is reported to the server only on its first attempt, which records it in history and retries it at once. A retry that fails again is not reported; the server retries it once the workflow task timeout (`StartWorkflowOptions.task_timeout`, 10 s by default) passes, so code that keeps failing retries about once per task timeout until it is fixed or the run is terminated. The worker logs each failure. [Deadlock detection](#deadlock-detection) and the [workflow memory limit](#workflow-memory-limit), under Workers, fail tasks the same way.
- `@panic` in workflow code, and in Debug and ReleaseSafe builds safety-checked illegal behavior such as integer overflow or an out-of-bounds index, ends the worker process: Zig has no way to recover from a panic. The workflow task that was running gets no completion, so the server retries it once its workflow task timeout passes, on whichever worker polls the task queue then (the restarted one, or another). Every run cached on the process is replayed from history by its next task. Code that panics on every attempt ends every worker that takes its task, so return an error instead, and list it if it should fail the task.
- `testing.WorkflowReplayer.Options.workflow_task_failure_errors` replays with the worker's list. Replay doesn't re-run a task that failed: a history in which a listed error failed a task replays with code that no longer returns it, and code that still does fails replay with `error.ReplayFailed`.

#### Stack trace and metadata queries
<!-- stub: temporal-sdk-zig-d51.6.9 -->
Not implemented yet.
The built-in `__stack_trace`, `__enhanced_stack_trace`, and `__temporal_workflow_metadata` queries will report where each coroutine is blocked and which handlers are registered.

### Activities

#### Definition

An activity is a function `fn (ctx: *activity.Context, args...) !Result` that a worker runs on its real `std.Io`, where it may do any I/O; `temporal.activity.Def` names it for workflows and workers. Activities that share state are methods taking the state first and the context second, and `activity.defs` gives one `Def` per method.

<!-- example: activity-definition -->
```zig
const activity = temporal.activity;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const Greet = activity.Def("greet", greet);

pub const Greeter = struct {
    punctuation: []const u8,
    pub fn greetLoudly(g: *Greeter, ctx: *activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "HELLO, {s}{s}", .{ name, g.punctuation });
    }
};
pub const greeter = activity.defs(Greeter, .{ .prefix = "Greeter." }); // "Greeter.greetLoudly"

fn registerGreetings(w: *temporal.worker.Worker, shared: *Greeter) !void {
    try w.registerActivity(Greet);
    try w.registerActivities(shared, greeter);
}
```

Notes:
- Arguments and result go through the payload converter; allocate the result from `ctx.allocator()`, which lives until the result has been reported.
- Workflows call method activities as fields of the `defs` value (`greeter.greetLoudly`). `registerActivities` and `registerActivityWith` take an instance that must outlive the worker and be safe to use from several activities at once.
- No pub declaration of the struct may depend on its own `defs(...)`: declare the result outside the struct, or it fails to compile with a dependency loop.
- Registering a name twice returns `error.AlreadyRegistered`; names starting with `__temporal_` return `error.ReservedActivityName`.

#### Context

`activity.Context` is the activity's view of its attempt. The activity uses it from its own task; `cancelReason`, `isWorkerStopping` and `waitForWorkerStop` are also safe from other threads.

<!-- example: activity-info -->
```zig
/// How much of the attempt's time is left, from the timeouts the workflow set.
fn timeLeft(ctx: *activity.Context) !std.Io.Duration {
    const info = ctx.info();
    ctx.logger().info("attempt", .{ .attempt = info.attempt, .max = if (info.retry_policy) |p| p.maximum_attempts else 0 });
    const deadline = info.deadline() orelse return error.NoTimeout; // none in this attempt's info
    return std.Io.Timestamp.now(ctx.io(), .real).durationTo(deadline);
}
pub const TimeLeft = activity.Def("time_left", timeLeft);
```

Notes:
- `allocator()`: memory that lives until the result has been reported. `io()`: the worker's `std.Io`.
- `info()`: IDs, types, task queue, `attempt`, `is_local`, `task_token`, `session_id`, and scheduled and started times; freed with the result.
- `info()` also has the timeouts the activity was scheduled with (`schedule_to_close_timeout`, `start_to_close_timeout`, `heartbeat_timeout`, each null when unset), its `retry_policy` (null when the task carries none) and `priority` (all zero when it carries none), as `workflow.RetryPolicy` and `workflow.Priority`. A local activity has no heartbeat timeout or priority.
- `info().deadline()`: when this attempt times out, the earlier of `scheduled_time` plus `schedule_to_close_timeout` and `started_time` plus `start_to_close_timeout`; null when neither is known.
- `isWorkerStopping()` and `waitForWorkerStop()`: see [Worker shutdown](#worker-shutdown).
- `logger()`: a logger carrying the activity's and its workflow's IDs and types, writing to `worker.Options.logger`.
- `cancelReason()`: `not_found`, `canceled`, `timed_out`, `worker_shutdown`, `paused`, or `reset`, or null before a cancel.
- `client()`: the worker's client (don't `close` it); `error.NoClient` in an `ActivityEnvironment` without one.
- `headers()`, `propagated()`, `metrics()`: the header, its propagated values, and a metrics handler valid until the activity returns.

#### Heartbeating and cancellation

`activity.Context.heartbeat` reports that the activity is alive, with details a retry reads back with `heartbeatDetails`. A cancel (from the workflow, a timeout, or worker shutdown) makes the activity's next wait on `ctx.io()` return `error.Canceled`.

<!-- example: activity-heartbeat -->
```zig
fn sendGreetings(ctx: *activity.Context, names: []const []const u8) !u32 {
    var sent: u32 = if (ctx.hasHeartbeatDetails()) try ctx.heartbeatDetails(u32) else 0;
    while (sent < names.len) : (sent += 1) {
        try ctx.io().checkCancel(); // error.Canceled after a cancel; ctx.cancelReason() says why
        ctx.logger().info("Hello!", .{ .name = names[sent] });
        ctx.heartbeat(sent + 1);
    }
    return sent;
}
pub const SendGreetings = activity.Def("send_greetings", sendGreetings);
```

Notes:
- A cancel from the workflow arrives only in the server's answer to a heartbeat, so an activity that should notice it must heartbeat. Heartbeats are throttled to 80% of the heartbeat timeout, at most the worker's `max_heartbeat_throttle_interval` (60 seconds), and to its `default_heartbeat_throttle_interval` (30 seconds) without a heartbeat timeout, so calling `heartbeat` often is cheap.
- `heartbeat({})` sends no details; pass a struct for several values. `heartbeatDetails(T)` decodes the first detail, and returns `error.NoHeartbeatDetails` when the previous attempt left none (a first attempt, or one that never heartbeated or heartbeated with `{}`); `encodedHeartbeatDetails()` returns every payload undecoded.
- Returning `error.Canceled` after a cancel reports the activity canceled; any other outcome is reported as usual. Without a cancel, `error.Canceled` is reported as a failure.
- A local activity (`info().is_local`) gets cancels straight from the worker, and its heartbeats are dropped.

#### Worker shutdown

When its worker begins stopping (`Worker.stop`, or `run` ending on a poll failure), an activity sees `ctx.isWorkerStopping()` turn true and `ctx.waitForWorkerStop()` return. The worker's `graceful_shutdown_period` is the time it then has to finish or save its progress; an activity still running when it has passed is canceled with reason `worker_shutdown`.

<!-- example: activity-worker-stop -->
```zig
/// Waits for new mail, standing in for real I/O with a sleep, or for the worker to stop.
fn waitForMail(ctx: *activity.Context) ![]const u8 {
    const Woken = union(enum) { stopping: error{Canceled}!void, mail: error{Canceled}!void };
    var buffer: [2]Woken = undefined;
    var select: std.Io.Select(Woken) = .init(ctx.io(), &buffer);
    defer select.cancelDiscard();
    // `concurrent`, not `async`: an `async` the Io can't give a thread runs at once, here.
    try select.concurrent(.stopping, activity.Context.waitForWorkerStop, .{ctx});
    try select.concurrent(.mail, std.Io.sleep, .{ ctx.io(), .fromSeconds(30), .awake });
    switch (try select.await()) {
        // Within the worker's graceful_shutdown_period: return, and let a retry carry on.
        .stopping => |stopped| {
            try stopped; // error.Canceled when the activity was canceled first
            return "worker stopping";
        },
        .mail => |slept| {
            try slept;
            return "mail";
        },
    }
}
pub const WaitForMail = activity.Def("wait_for_mail", waitForMail);

/// A loop can check the same signal between steps instead.
fn copyFiles(ctx: *activity.Context, count: u32) !u32 {
    var copied: u32 = if (ctx.hasHeartbeatDetails()) try ctx.heartbeatDetails(u32) else 0;
    while (copied < count and !ctx.isWorkerStopping()) : (copied += 1) {
        ctx.heartbeat(copied + 1);
    }
    return copied;
}
pub const CopyFiles = activity.Def("copy_files", copyFiles);
```

Notes:
- Run `waitForWorkerStop` as a coroutine beside the work, with `io.concurrent` or as a `std.Io.Select` case started with `select.concurrent`. Not with `async`: when the Io has no thread to spare (always on a one-CPU host, and on any host once enough activities run), `async` runs the wait at once on the activity's own task, which then blocks until the worker stops. It returns `error.Canceled` when the activity is canceled first, or when its own task is canceled.
- With the default `graceful_shutdown_period` of zero, the cancel follows the stop at once, so set a period to give activities time to react.
- Regular activities, local activities and activities in sessions all see the signal. There is no "am I in an activity" check: code that holds an `*activity.Context` is in one.

#### Asynchronous completion

An activity that finishes elsewhere (a person approves it, another system calls back) hands off `info().task_token` and returns `activity.Context.completeAsync`. It stays running on the server until a client completes it or it times out.

<!-- example: activity-async -->
```zig
pub const Approvals = struct {
    gpa: std.mem.Allocator, // thread-safe: activities run concurrently
    mutex: std.Io.Mutex = .init,
    tokens: std.ArrayList([]u8) = .empty,
    pub fn requestGreeting(a: *Approvals, ctx: *activity.Context, name: []const u8) ![]const u8 {
        ctx.logger().info("awaiting approval", .{ .name = name });
        {
            const token = try a.gpa.dupe(u8, ctx.info().task_token); // freed after return
            errdefer a.gpa.free(token);
            try a.mutex.lock(ctx.io());
            defer a.mutex.unlock(ctx.io());
            try a.tokens.append(a.gpa, token);
        }
        return ctx.completeAsync(); // an error: errdefers in scope here would run
    }
};
pub const RequestGreeting = activity.Def("request_greeting", Approvals.requestGreeting);

fn approve(client: *temporal.client.Client, token: []const u8) !void {
    try client.completeActivity(token, "Hello, Ana!");
}
```

Notes:
- `completeAsync` returns `error.CompleteAsync`, so `errdefer`s still in scope run; do the hand-off in an inner block.
- Finish with `Client.completeActivity`, `failActivity`, or `reportActivityCancellation` (by token, or `*ById`), and heartbeat with `recordActivityHeartbeat`, which returns true once cancellation is requested. All return `error.NotFound` once the activity is no longer waiting.
- A local activity that returns `completeAsync` fails with the non-retryable type `LocalActivityCompleteAsync`.

#### Failures

An activity that returns an error fails its attempt, and the retry policy decides whether another runs. `activity.Context.fail` describes the failure: message, type, details, retryability, category, and the delay before the next attempt.

<!-- example: activity-failures -->
```zig
fn greetMember(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{
        .message = "greet_member: the name is empty",
        .type = "InvalidName",
        .non_retryable = true,
    });
    if (!directoryReady(ctx.io())) return ctx.fail(.{
        .message = "greet_member: the member directory is still loading",
        .category = .benign, // expected: logged at debug level, not counted as an error
        .next_retry_delay = .fromSeconds(30), // instead of the retry policy's backoff
    });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
pub const GreetMember = activity.Def("greet_member", greetMember);
```

Notes:
- Return the error `fail` gives back (`error.ApplicationFailure`); the described failure is reported in its place.
- Any other error is reported as a retryable failure whose type and message are the error's name.
- `non_retryable = true` stops retries whatever the policy says, and `type` is what a policy's non-retryable types match; `next_retry_delay` is ignored when the failure isn't retried.

#### Dynamic activities

A dynamic activity, `fn (*Context, converter.EncodedValues) !Result` wrapped in `activity.DynamicDef`, runs every activity type the worker has no `Def` for, such as types a workflow calls with `executeActivityByName`. Register it with `Worker.registerDynamicActivity`; it reads its type from `ctx.info().activity_type`.

Notes:
- A registered `Def` always wins; a second dynamic activity returns `error.AlreadyRegistered`.
- Decode arguments with `args.get(T, allocator, index)` or `args.decode(Tuple, allocator)`. Past the last argument, `get` returns null for an optional `T` and `error.WrongArgumentCount` for any other type, and `decode` does the same for each missing field. A payload that doesn't decode as its type returns `fromPayload`'s errors: `error.InvalidPayload` or `error.UnknownEncoding` (see [Payloads](#payloads)).
- A method form is registered with `registerDynamicActivityWith`; `activity.defs` skips methods of that shape. `testing.ActivityEnvironment` can't run a dynamic activity.

### Workers

#### Running and stopping

A `worker.Worker` polls one task queue and runs the workflows, activities, and [Nexus services](#serving-operations) registered on it. `Worker.run` polls until `Worker.stop` is called; a program's `main` usually calls `Worker.runUntilSignal`, which stops on SIGINT or SIGTERM.

<!-- example: worker-run -->
```zig
var w = try Worker.init(gpa, io, &client, "greetings", .{});
defer w.deinit();
try w.registerWorkflow(Greeting);
try w.registerActivity(Greet);

// `run` returns once `stop` is called and in-flight work has finished.
var running = try io.concurrent(Worker.run, .{&w});
defer {
    w.stop();
    running.await(io) catch |err|
        std.log.err("greetings worker failed: {t} {s}", .{ err, w.fatalMessage() orelse "" });
}
try w.waitUntilRunning();

var run = try client.executeWorkflow(Greeting, .{ .id = "greeting-ana", .task_queue = "greetings" }, .{"Ana"});
defer run.deinit();
const result = try run.get(arena); // "Hello, Ana!"
```

Notes:
- The client must outlive the worker; call `deinit` after `run` has returned.
- There is no separate start function: to keep working while the worker polls, run `run` on `io.concurrent` as above, and read its error from `await`.
- `run` returns `error.NothingRegistered`, `error.InvalidOptions`, `error.WorkerInitFailed`, or `error.ValidationFailed` before polling starts (the last three logged with the reason); `Worker.RunError` lists every error.
- When sdk-core gives up polling for tasks, the worker logs it, shuts down as `stop` does, and `run` (or `runUntilSignal`) returns `error.PollFailed`, or `error.OutOfMemory` when the poll itself ran out of memory. `Worker.fatalMessage` then has `polling for <workflow|activity|nexus> tasks on task queue <queue> failed: <reason>`, owned by the worker until the next `run` or `deinit`; it is null if that text couldn't be allocated.
- `stop` is safe from any thread, before or during `run`. `waitUntilRunning` returns `error.WorkerFailedToStart` when `run` failed first.
- Several workers can share a process and a client, one per task queue; one signal stops every worker in `runUntilSignal`.

#### Options

`worker.Options` is the struct literal passed to `Worker.init`; every field has a default, so `.{}` works.

<!-- example: worker-options -->
```zig
var w = try Worker.init(gpa, io, client, "greetings", .{
    .identity = "greetings-worker-1", // default: the client's identity
    .max_concurrent_activity_executions = 20,
    .max_concurrent_local_activity_executions = 50,
    .max_concurrent_activity_task_pollers = 4,
    .max_cached_workflows = 500,
    .sticky_schedule_to_start_timeout = .fromSeconds(5),
    .max_activities_per_second = 50, // this worker
    .max_task_queue_activities_per_second = 200, // all workers on "greetings"
    .default_heartbeat_throttle_interval = .fromSeconds(10),
    .max_heartbeat_throttle_interval = .fromSeconds(30),
    .max_eager_activity_reservations_per_workflow_task = 0, // no eager activities
    .graceful_shutdown_period = .fromSeconds(10),
    .workflow_memory_limit = 16 * 1024 * 1024,
});
```

Notes:
- `max_concurrent_workflow_task_executions` (100): workflow activations processed at once. Activations of different runs run in parallel, each on its own thread; one run's activations run one at a time, possibly on different threads. Payload codecs, interceptors, and log sinks are therefore called concurrently.
- `max_concurrent_activity_executions` (100): activities run at once, each on its own task. `max_concurrent_local_activity_executions` (100): local activities run at once, a separate pool; a local activity waits for a slot up to its `schedule_to_start_timeout`.
- `max_concurrent_nexus_task_executions` (100): Nexus requests (starts and cancels of operations) handled at once, each on its own task. Used only with a registered Nexus service.
- `max_concurrent_workflow_task_pollers`, `max_concurrent_activity_task_pollers`, `max_concurrent_nexus_task_pollers` (2 each): polls in flight at once. Each must be at least 1, and the workflow pollers at least 2 while `max_cached_workflows` isn't 0; sdk-core rejects other values and `run` returns `error.WorkerInitFailed`, logged. The Nexus pollers are checked only with a registered Nexus service.
- `max_cached_workflows` (1000): runs kept in memory between tasks. Over the limit the least recently used run is evicted and replayed from its history at its next task; 0 replays every task from the start. `workflow_stack_size` (256 KiB): each coroutine's stack.
- `identity`: the worker's name to the server; empty uses the client's.
- `sticky_schedule_to_start_timeout` (10 s): how long a workflow task waits on this worker's sticky queue, where the next task of a cached run goes, before the server hands it to any worker on the task queue, which replays the run.
- `max_activities_per_second` (null, no limit): the most activities per second this worker starts; it stops polling while one more would go over, and eager activities, which would bypass it, are turned off. `max_task_queue_activities_per_second` (null): the most the server hands out from the task queue across all its workers, sent with each poll, so workers with different values override each other; a limit also turns off eager activities. Both must be positive and finite, and apply to the task queue's own activities only, not to session activities.
- `default_heartbeat_throttle_interval` (30 s) and `max_heartbeat_throttle_interval` (60 s): an activity's heartbeats reach the server at 80% of its heartbeat timeout, at most every `max_heartbeat_throttle_interval`, or every `default_heartbeat_throttle_interval` without a heartbeat timeout; heartbeats in between keep only the latest details. Session activities are throttled the same way. Neither may be negative, and the default may not be above the maximum.
- `max_eager_activity_reservations_per_workflow_task` (3): how many activities on the worker's own task queue one workflow task may ask the server to start on this worker eagerly, with free activity slots, in the answer to the task's completion; 0 turns eager activities off for the worker, as either per-second limit does. `ActivityOptions.disable_eager_execution` turns it off for one activity.
- `poll_activity_tasks` (true): false stops the worker polling its task queue for activity tasks, so its registered activities run only as local activities of its own workflows (`workflow.Context.executeLocalActivity`); activities scheduled through the task queue wait for another worker or time out. It needs a registered workflow when activities are registered, and can't be combined with `sessions`.
- `run` returns `error.InvalidOptions`, and logs the option and why, for a per-second limit that is zero, negative, infinite or NaN (or a `max_activities_per_second` below one per 2^61 seconds, about 4.4e-19 per second), a negative duration among the ones above, a default heartbeat throttle interval above the maximum, or `poll_activity_tasks = false` with `sessions` or with activities but no workflow.
- `graceful_shutdown_period` (zero) and `workflow_memory_limit` (null): see below.
- `workflow_task_failure_errors` (empty) and `nondeterminism_fails_workflow` (false): which errors and whether nondeterminism fail the workflow task or the workflow; see [Failure policy](#failure-policy).
- `logger` (null uses the client's sink) and `log_during_replay` (false): where workflow and activity logs go, and whether replay logs.
- `interceptors`: run after the client's `worker_interceptors`; borrowed, called from several threads.
- `deployment`, `build_id`, `use_build_id_versioning`, and `sessions`: see below.
- There is no option to stop polling workflow tasks: a worker polls them only when a workflow is registered, activity tasks when an activity or a workflow is, and Nexus tasks when a Nexus service is.
- There is no option for state shared by activities: register them as methods bound to a receiver with `registerActivityWith`, `registerActivitiesWith`, or `registerActivities`.

#### Shutdown

`Worker.stop` begins a graceful shutdown: polling ends, in-flight workflow tasks finish, and running activities and Nexus handlers get `graceful_shutdown_period` before they are canceled. `run` returns once all of that is done.

Notes:
- With the default period of zero, running activities and Nexus handlers are canceled at once, with `cancelReason()` `worker_shutdown`.
- `runUntilSignal` replaces the SIGINT and SIGTERM handlers while any worker runs that way, and restores them afterwards.
- A failed core shutdown is logged, and `run` returns `error.ShutdownFailed`.

#### Versioning

`worker.Options.deployment` makes the worker a version of a Worker Deployment: the deployment name plus this build's ID. The server routes workflows to the deployment's current (or ramping) version, and each workflow's `workflow.VersioningBehavior` keeps a run on its version (`pinned`) or moves it to the current one (`auto_upgrade`).

<!-- example: worker-versioning -->
```zig
/// Runs stay on the build they started on; other workflows follow the worker's default.
const PinnedGreeting = workflow.DefWithOptions("pinned_greeting", greeting, .{ .versioning_behavior = .pinned });

fn runVersionedWorker(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !void {
    var w = try Worker.init(gpa, io, client, "greetings", .{
        .deployment = .{ .deployment_name = "greetings", .build_id = "2.0", .default_versioning_behavior = .auto_upgrade },
    });
    defer w.deinit();
    try w.registerWorkflow(Greeting);
    try w.registerWorkflow(PinnedGreeting);
    try w.registerActivity(Greet);
    try w.runUntilSignal();
}
```

Notes:
- With `use_worker_versioning` (the default) and no `default_versioning_behavior`, every workflow must declare a behavior, or `run` returns `error.InvalidOptions`.
- A dynamic workflow chooses its behavior with `workflow.Context.setVersioningBehavior` before it first blocks.
- Manage deployments (current and ramping versions) with `Client.workerDeployment`; see `temporal.deployment`.
- `build_id` alone identifies the build without versioning; `use_build_id_versioning` is the older build-ID routing. Neither may be set with `deployment`.

#### Sessions

A session runs a group of activities on one worker host, for work that needs host-local state such as files. `worker.Options.sessions` (for example `.{ .max_concurrent_sessions = 4 }`) makes a worker a host; a workflow opens a session with `workflow.Context.createSession` and passes it in `ActivityOptions.session`.

<!-- example: worker-sessions -->
```zig
/// Writes a card to the host's disk and prints it there: both activities run on one host.
fn greetingCard(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const session = try ctx.createSession(.{
        .creation_timeout = .fromSeconds(60),
        .execution_timeout = .fromSeconds(600),
    });
    defer ctx.completeSession(session) catch {};
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(30), .session = session };
    const path = try ctx.executeActivity(WriteCard, options, .{name}).get();
    return ctx.executeActivity(PrintCard, options, .{path}).get();
}
const GreetingCard = workflow.Def("greeting_card", greetingCard);
```

Notes:
- `createSession` returns `error.SessionCreationFailed` when the session didn't open: no host took it within `creation_timeout`, or the host stopped or refused it as full (failure type `SessionWorkerFull`: a full host stops taking new sessions, so this comes mostly from `recreateSession`). Returning the error fails the workflow with the session's creation failure. A timeout that isn't positive returns `error.InvalidOptions` (logged), and canceling the waiting coroutine `error.Canceled`.
- `max_concurrent_sessions` (1000, must be positive) bounds a host's open sessions; session activities get their own slot pool.
- A session fails when its host stops, goes silent past `heartbeat_timeout`, or exceeds `execution_timeout`; activities scheduled in it then return `error.SessionFailed`.
- `recreateSession` opens a new session on an earlier session's host, by its `info().task_queue`.

#### Workflow memory limit

`worker.Options.workflow_memory_limit` caps the bytes one cached run may hold: its live `ctx.allocator()` memory plus one `workflow_stack_size` per coroutine stack. Null (the default) means no limit.

Notes:
- It is checked at the end of each activation that runs workflow code; allocations are never refused, and query-only and final activations aren't checked.
- A run over the limit fails its workflow task with type `WorkflowMemoryLimitExceeded`, not the workflow; the retried task fails the same way until the limit is raised or the workflow keeps less state.
- `testing.WorkflowReplayer.Options.workflow_memory_limit` checks that recorded histories fit a limit.

#### Deadlock detection

Workflow code that never yields, such as a loop that never waits or a call that blocks outside the workflow's `std.Io`, would hold its workflow task until the server times it out. `worker.Options.deadlock_detection_timeout` limits how long workflow code may run without returning to the workflow scheduler: without blocking on a future, timer, or other wait on `ctx.io()`, or finishing. It is 2 seconds by default; null turns detection off.

<!-- example: worker-deadlock-detection -->
```zig
// Workflow code may run up to 5 s without waiting before its task fails (default: 2 s).
var patient = try Worker.init(gpa, io, &client, "reports", .{ .deadlock_detection_timeout = .fromSeconds(5) });
defer patient.deinit();
// No detection, for workflow code stopped at a debugger's breakpoints.
var debugging = try Worker.init(gpa, io, &client, "reports-debug", .{ .deadlock_detection_timeout = null });
defer debugging.deinit();
```

Notes:
- A watchdog on the worker's own `Io` and real clock, never the workflow's deterministic `std.Io`, times each stretch of an activation's workflow code between returns to the scheduler. Payload codecs decoding the activation and encoding its completion run outside workflow code and aren't counted; a workflow that waits often may run much longer than the limit in total.
- Over the limit, the worker fails the workflow task with an application failure of type `worker.deadlock_failure_type` (`"WorkflowDeadlockDetected"`) whose message names the workflow type and the limit, logs one error with the workflow and run IDs, and evicts the run, so its next task replays it from history. The workflow does not fail; the server retries the task. As with any failed workflow task (see [Failure policy](#failure-policy)), only the first attempt's failure is reported: code that gets stuck on every later attempt sends no failure, so those attempts end in a workflow task timeout. Detection takes up to about 1.25 times the limit.
- The SDK cannot stop stuck code: it keeps its thread until it returns. Whatever it produces then is discarded, no second completion is sent, and only then is the run's memory freed. Other runs, and a replay of the same run, keep being processed meanwhile. `Worker.stop` doesn't wait for stuck code, but `Worker.run` returns only after it has returned.
- Turn detection off while a debugger may stop workflow code at a breakpoint, or the paused task fails.
- Detection emits no command and changes none, so replay is unaffected. `testing.WorkflowReplayer.Options.deadlock_detection_timeout` (null by default) applies the same limit to replayed code.

#### Tuners and poller behavior
<!-- stub: temporal-sdk-zig-d51.9.3, temporal-sdk-zig-d51.9.4 -->
Not implemented yet.
`worker.Options` is to take slot tuners (fixed, resource-based, composite, custom) and per-poller behavior (a simple maximum or autoscaling).

#### Plugins
<!-- stub: temporal-sdk-zig-d51.9.7 -->
Not implemented yet.
Plugins are to be registered on a client or worker, with hooks that configure its options and wrap `run`, and their names and versions reported to the server.

### Data conversion

#### Payloads

Arguments, results, signals, queries, updates, memos, headers, and failure details cross the wire as `converter.Payload` values: bytes plus an `encoding` metadata entry. `converter.toPayload` encodes a value and `converter.fromPayload` decodes one as a given type.

<!-- example: converter-payloads -->
```zig
const Request = struct { name: []const u8, language: ?[]const u8 = null };
const json = try converter.toPayload(arena, Request{ .name = "Ziggy" }); // "json/plain"
try testing.expectEqualStrings("Ziggy", (try converter.fromPayload(Request, arena, json)).name);

// Cents declares toPayload and fromPayload, so it encodes itself.
const tip = try converter.toPayload(arena, Cents{ .value = 250 });
try testing.expectEqual(250, (try converter.fromPayload(Cents, arena, tip)).value);

const Execution = temporal.proto.temporal.api.common.v1.WorkflowExecution;
const execution: Execution = .{ .workflow_id = "greeting-1" };
const proto_json = try converter.toPayload(arena, execution); // "json/protobuf"
const proto_binary = try converter.toPayload(arena, converter.ProtoBinary(Execution){ .message = execution });
try testing.expectEqualStrings("binary/protobuf", converter.encodingOf(proto_binary).?);

// A value whose type the caller learns later stays encoded until read.
const later: converter.EncodedValue = .{ .payload = proto_json };
try testing.expectEqualStrings("greeting-1", (try later.get(Execution, arena)).workflow_id);
```

Notes:

- A `converter.Payload` passes through as is. `void` and null encode as `binary/null`, `converter.Bytes` as `binary/plain`, protobuf messages as ProtoJSON (`converter.ProtoBinary` as `binary/protobuf`), and everything else, strings included, as JSON through `std.json`.
- A type that declares `pub fn toPayload(self, arena)` and `pub fn fromPayload(arena, payload)` encodes itself, also as an optional.
- Decoded strings and slices come from the arena passed in; an encoded payload may borrow the value's bytes.
- `fromPayload` returns `error.UnknownEncoding` for a payload with no `encoding` or with one the converter doesn't handle, which for a protobuf message type is anything but `binary/null` and the two protobuf encodings.
- It returns `error.InvalidPayload` for `binary/null` into a type that isn't `void` or optional, `binary/plain` into anything but `converter.Bytes` or a byte slice, and JSON or protobuf data that doesn't parse as the type.

#### Payload codecs

A `converter.PayloadCodec` transforms payloads after encoding and before decoding, to compress or encrypt them. Set codecs with `client.Options.codecs`; the client and every worker on it apply them. `converter.ZlibCodec` compresses each payload that compression makes smaller and leaves the others as they were, unless `always_encode` is set.

<!-- example: converter-codecs -->
```zig
const zlib: converter.ZlibCodec = .{};
const codecs = [_]converter.PayloadCodec{zlib.codec()};

// What a client with `.codecs = &codecs` does to every payload it sends and receives.
const plain = try converter.toPayload(arena, name);
const encoded = try converter.encodePayloads(&codecs, arena, io, &.{plain});
try testing.expectEqualStrings("binary/zlib", converter.encodingOf(encoded[0]).?);
const decoded = try converter.decodePayloads(&codecs, arena, io, encoded);
try testing.expectEqualStrings(name, try converter.fromPayload([]const u8, arena, decoded[0]));

const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{ .codecs = &codecs });
defer env.deinit();
```

Notes:

- Codecs run in order on the way out and in reverse on the way in. Search attributes are never encoded. The codecs are borrowed and must outlive the client.
- A codec returns as many payloads as it was given and passes through payloads it didn't encode. It runs on many threads at once, never in workflow code.
- `encodeWithContext` and `decodeWithContext` give a codec the call's `converter.CodecContext`: namespace, workflow, activity, and propagated values.
- `converter.codecHandler(codecs, options)` serves `POST /encode` and `/decode` for the Temporal Web UI and CLI. It doesn't authenticate callers.

#### Failures

A failed operation returns a Zig error (`error.ActivityFailed`, `error.WorkflowFailed`); the `temporal.Failure` itself, with message, type, details, and cause chain, is read afterwards from the future or handle. Code raises a failure by returning an error, by describing one with `ctx.fail`, or, in a workflow, by choosing an existing one with `ctx.failWith`.

<!-- example: converter-failures -->
```zig
fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (name.len == 0) return ctx.fail(.{ .message = "name is empty", .type = "InvalidName", .non_retryable = true });
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return future.get() catch |err| {
        if (future.failure()) |f| {
            // For an empty name: "name is empty", type "InvalidName".
            ctx.logger().warn("greet failed", .{ .reason = temporal.failure.rootCause(f).message });
        }
        return err;
    };
}
```

Cleanup that runs on the error path (an `errdefer`, a `defer`, a `catch` block) can call activities that fail too. `ctx.failWith(f)` records the failure the workflow fails with, here the charge's from `future.failure()`, and returns `error.ApplicationFailure` to return; failures that later `get` calls record don't replace it:

<!-- example: converter-fail-with -->
```zig
fn checkout(ctx: *workflow.Context, order: []const u8) !void {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    try ctx.executeActivity(Hold, opts, .{order}).get();
    // Releases the hold if anything below fails; the release can fail too.
    errdefer ctx.executeActivity(Release, opts, .{order}).get() catch {};

    const charge = ctx.executeActivity(Charge, opts, .{order});
    // Keeps the charge's failure: without failWith, a failed release would replace it.
    // Other errors, such as error.Canceled, are returned as they are.
    charge.get() catch |err| return switch (err) {
        error.ActivityFailed => ctx.failWith(charge.failure().?),
        else => err,
    };
}
```

Notes:

- A plain error becomes an application failure whose message and type are the error's name. An earlier `ctx.fail` replaces that failure, whatever the error, only for the code it was called in: one in the workflow function, or in a coroutine that it or a handler starts, applies to the error the workflow function returns; one in a signal handler applies to that handler's error when the error fails the workflow; one in an activity attempt applies to the error that attempt returns. An update handler uses its `ctx.fail` failure only when it returns `error.ApplicationFailure`; any other error rejects the update with the error's name. In a workflow, `error.Canceled` cancels it and `error.OutOfMemory` fails the workflow task for a retry; in an activity, `error.Canceled` after a cancel reports it canceled.
- `ctx.fail` takes a `failure.Application` (`type`, `non_retryable`, `details`, `category`, `next_retry_delay`, `cause`) and returns `error.ApplicationFailure`.
- On the client, `run.failure()` reads the failure after `error.WorkflowFailed`; it borrows the arena passed to `get`.
- A workflow that returns `error.ActivityFailed` (or a child, external, or session error) fails with the last failure a `get` returned; `failure.rootCause` then reaches the original one. The last failure is kept per signal or update handler run, and once for the workflow function and every coroutine it or a handler starts, so a failed `get` in a sibling coroutine, or a cleanup activity that fails after it, can replace it. Return the error as soon as `get` gives it, or choose the failure with `ctx.failWith`.
- `failWith` reports `f` as given: its kind (here an activity failure), message, details, and cause chain. `f` is copied, but what it points to is read after the function returns, so it must live as long as the run; a failure read from a future does, as does one built in `ctx.allocator()`.
- Call `failWith` only for the future's failure error. A `get` can return `error.Canceled` while `failure()` already holds the activity's failure (it failed as the workflow was canceled); returning `failWith`'s error there would fail a workflow that should be canceled.
- `ctx.fail` and `ctx.failWith` share one slot and follow the same rules: the later call wins, and once recorded it is reported for the error the code returns, so call it on the path that returns. In an update handler, returning its `error.ApplicationFailure` rejects the update with `f`; in a signal handler it fails the workflow with `f`.

A failure can carry the failure that caused it: set `cause` on the `failure.Application` given to `ctx.fail` (in a workflow, an activity, or a Nexus operation) or to `Client.failActivity`. `failure.kind(f)` reads a failure as a `failure.Kind`, a tagged union with one case per kind the server defines (`application`, `timeout`, `canceled`, `terminated`, `server`, `reset_workflow`, `activity`, `child_workflow`, `nexus_operation`, `nexus_handler`) and `unknown`:

<!-- example: converter-failure-kinds -->
```zig
fn reserve(ctx: *activity.Context, sku: []const u8) !void {
    const why = try temporal.failure.application(ctx.allocator(), .{ .message = "warehouse offline", .type = "WarehouseOffline" });
    return ctx.fail(.{
        .message = "reservation failed",
        .type = "ReserveFailed",
        .non_retryable = true,
        .details = &.{try converter.toPayload(ctx.allocator(), sku)},
        // Copied by ctx.fail, so it may live on the stack.
        .cause = &why,
    });
}

fn fulfill(ctx: *workflow.Context, sku: []const u8) ![]const u8 {
    const future = ctx.executeActivity(Reserve, .{ .start_to_close_timeout = .fromSeconds(5) }, .{sku});
    future.get() catch |err| {
        const f = future.failure() orelse return err;
        // f is the activity failure; its cause is the one the activity raised.
        const reason = switch (temporal.failure.kind(f.cause orelse f)) {
            .timeout => |t| try std.fmt.allocPrint(ctx.allocator(), "timed out ({t})", .{t.timeout_type}),
            .application => |app| try std.fmt.allocPrint(ctx.allocator(), "{s} for {s}: {s}", .{
                app.type,
                try app.details.get([]const u8, ctx.allocator(), 0),
                temporal.failure.rootCause(f).message,
            }),
            else => return err,
        };
        // The workflow's failure, with the activity's chain under it.
        return ctx.fail(.{ .message = reason, .type = "FulfillFailed", .cause = f });
    };
    return "reserved";
}
```

When the SDK builds a failure from an error a workflow function, a signal or update handler, or an activity function returned (from the error's name or from `ctx.fail`), the failure's `stack_trace` holds the error's return trace, one line per frame: `src/shop.zig:42:13: in reserve`. With `client.Options.encode_failure_attributes`, the client and its workers move the message and stack trace of every failure they send into its `encoded_attributes`, so the codecs encode them too:

<!-- example: converter-encoded-attributes -->
```zig
const zlib: converter.ZlibCodec = .{};
const codecs = [_]converter.PayloadCodec{zlib.codec()};
const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, io, .{
    .codecs = &codecs,
    // client.Options.encode_failure_attributes, for the client and its workers.
    .encode_failure_attributes = true,
});
defer env.deinit();

// What the client and its workers do to each failure they send, before the codecs run,
// and to each failure they receive, after them.
var f = try temporal.failure.application(arena, .{ .message = "card 4242 declined" });
try temporal.failure.encodeAttributes(temporal.Failure, &f, arena);
try testing.expectEqualStrings("Encoded failure", f.message);
try temporal.failure.decodeAttributes(temporal.Failure, &f, arena);
try testing.expectEqualStrings("card 4242 declined", f.message);
```

Notes:

- `cause` is borrowed only until the call it is given to returns: `ctx.fail`, `Client.failActivity` and `failActivityById`, `interceptor.rejectActivity` and `rejectChildWorkflow`, and the cause of a Nexus `ctx.handlerError` copy the chain (and the `details` list), so it may point at the stack. A future's failure (`future.failure()`) works as is.
- `failure.kind` borrows the failure it reads. Details and last heartbeat details are `converter.EncodedValues` (decode one with `get(T, arena, index)`), `next_retry_delay` is a `std.Io.Duration`, and timeout types, retry states and Nexus retry behaviors are Zig enums with an `unknown` case for values this SDK doesn't know. A failure with no info, or of a kind newer than the SDK, is `unknown`.
- Stack traces exist only in builds with error return tracing whose debug info the worker can read; otherwise, and with `worker.Options.failure_stack_traces = false`, `stack_trace` is empty. Zig traces errors in Debug by default. For traces in ReleaseSafe, set `.error_tracing = true` on your executable's root module (the one with `main`) and on the module with your workflows and activities; the SDK's module leaves the setting to you, because a dependency that turns tracing on while the root module doesn't breaks the program. It never holds bare addresses. A failure passed on unchanged (a future's failure returned through `error.ActivityFailed`, or `ctx.failWith`) keeps its own stack trace. Each workflow coroutine and each activity attempt starts with an empty trace, so frames of other failures never mix in. Traces name source paths of the build machine. The trace holds a bounded number of frames, keeping those nearest where the error started, and an error that comes from another coroutine or thread (awaited from `io.async` or a `Group`) has only the frames after it reached this one.
- Encoded attributes are a `json/plain` payload of `{"message": ..., "stack_trace": ...}` (`failure.EncodedAttributes`), the format other Temporal SDKs read and write; the failure travels with the message "Encoded failure" and an empty stack trace. Each cause in the chain is encoded the same way. Type, details and the rest stay where they are. A failure that already has encoded attributes (one passed on that this side couldn't decode) is sent as received, and a failed query's answer isn't encoded: it never reaches history, and the server returns its message in the clear.
- Received failures are decoded back after the codecs whatever the option says, so a client or worker reads failures from senders that encode them. One whose `encoded_attributes` doesn't decode (a codec this side lacks, another format) is left as received: a client without the codec reads "Encoded failure".
- `failure.encodeAttributes` and `failure.decodeAttributes` do the same to any protobuf message, for code that handles raw requests.

#### External payload storage
<!-- stub: temporal-sdk-zig-d51.4.9 -->
Not implemented yet.
Drivers that store large payloads outside history behind reference payloads, with payload size limits.

#### Remote codec client
<!-- stub: temporal-sdk-zig-d51.4.4 -->
Not implemented yet.
A `PayloadCodec` that calls a codec server's `/encode` and `/decode` endpoints over HTTP.

### Testing

#### Workflow environment

`testing.WorkflowEnvironment` starts a local dev server (downloaded and cached on first use), a client, and a worker on a fresh task queue, so a test can register workflows and activities, swap activities for test doubles, and run workflows to their result.

<!-- example: testing-workflow-environment -->
```zig
fn greetDouble(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hi, {s}!", .{name});
}

test "testing: a workflow with a test double for its activity" {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivityAs(Greet, greetDouble);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hi, Ziggy!", result);
}
```

Notes:

- Every workflow `executeWorkflow` or `getResult` waits for that completes, fails, is canceled, or times out is then replayed from its history, so a test also fails with `error.Nondeterminism`. A terminated run, or any other error, returns before the replay.
- `registerActivityAs` runs a double under the activity's name; registering the activity as well returns `error.AlreadyRegistered`.
- `startWorkflow` returns the run without waiting, for tests that signal, query, or update it before `getResult`.
- `createNexusEndpoint` creates a Nexus endpoint on the dev server that routes to a namespace and task queue (the environment's by default), for workflows that call [Nexus operations](#calling-operations-from-workflows); a name that exists returns `error.AlreadyExists`. `testing.createNexusEndpoint(client, name, target)` does the same through any client, for a server without an environment such as a `testing.DevServer`; its target names both the namespace, which must exist on the server, and the task queue.
- `startWithOptions` takes extra dev server arguments, codecs, a logger, propagators, worker and client interceptors, and metrics, borrowed for the environment's life. `startTimeSkipping` takes no options.

#### Time skipping

`WorkflowEnvironment.startTimeSkipping` uses the time-skipping test server: while a test waits for a result, time jumps ahead whenever every workflow is waiting on a timer.

<!-- example: testing-time-skipping -->
```zig
fn reminder(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    try ctx.io().sleep(.fromSeconds(std.time.s_per_day), .awake);
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

test "testing: a day-long timer fires at once" {
    const env = try WorkflowEnvironment.startTimeSkipping(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(Reminder);
    try env.registerActivity(Greet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const start = try env.now();
    _ = try env.executeWorkflow(Reminder, .{"Ziggy"}, arena.allocator(), null);
    try testing.expect(start.durationTo(try env.now()).nanoseconds >= std.time.ns_per_day);
}
```

Notes:

- Time moves at wall-clock speed except during `executeWorkflow`, `getResult`, and `env.sleep`.
- Signals, queries, and updates leave time locked, so a workflow waiting for one doesn't skip to its timeout.
- `env.now()` is the server's time, skipped time included.

#### Activity environment

`testing.ActivityEnvironment` calls an activity function directly with a test context, without a server.

<!-- example: testing-activity-environment -->
```zig
test "testing: an activity on its own" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);

    const result = try env.executeActivity(Greet, .{"Ziggy"}, arena.allocator());
    try testing.expectEqualStrings("Hello, Ziggy!", result);
    // A failure the activity describes with `ctx.fail` returns its error.
    try testing.expectError(error.ApplicationFailure, env.executeActivity(Greet, .{""}, arena.allocator()));
}
```

Notes:

- The result comes from the arena passed in; the failure a `ctx.fail` call describes isn't exposed.
- Set `info`, `heartbeat_details`, `on_heartbeat`, `client`, and `metrics` to control what the activity sees; `info.activity_type` is always the activity's name. Its timeouts and `retry_policy` start out null and its `priority` zero.
- `executeActivity` takes the environment by pointer (`var env`) and runs the activity on a task of its own while it waits; canceling the calling task cancels the activity, without a `cancelReason`.

`cancel(reason)` cancels the running activity as a worker does, and `stopWorker()` fires the worker-stop signal:

<!-- example: testing-activity-cancel -->
```zig
var waiting: std.Io.Event = .unset;

fn waitForever(ctx: *activity.Context) ![]const u8 {
    waiting.set(ctx.io());
    ctx.io().sleep(.fromSeconds(3600), .awake) catch |err| {
        const reason = ctx.cancelReason() orelse return err;
        return @tagName(reason); // a cancel the activity reports as a result, for the test
    };
    return "slept";
}
const WaitForever = activity.Def("wait_forever", waitForever);

fn runWaitForever(env: *ActivityEnvironment, arena: std.mem.Allocator) ![]const u8 {
    return env.executeActivity(WaitForever, .{}, arena);
}

test "testing: an activity sees a cancel and the worker stopping" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var env: ActivityEnvironment = .init(testing.io);
    env.info.heartbeat_timeout = .fromSeconds(5); // the new Info fields start out null or zero

    var running = try testing.io.concurrent(runWaitForever, .{ &env, arena.allocator() });
    try waiting.wait(testing.io);
    env.cancel(.timed_out); // as the worker cancels it: the next wait returns error.Canceled
    try testing.expectEqualStrings("timed_out", try running.await(testing.io));

    env.stopWorker(); // this and later activities see ctx.isWorkerStopping()
    try testing.expect(try env.executeActivity(activity.Def("stopping", isStopping), .{}, arena.allocator()));
}

fn isStopping(ctx: *activity.Context) !bool {
    try ctx.waitForWorkerStop(); // returns at once: the worker is already stopping
    return ctx.isWorkerStopping();
}
```

Notes:

- `cancel` reaches the activities running at the time, from any thread; with none running it does nothing. An activity already canceled keeps its first reason.
- After `stopWorker`, every activity the environment runs, now or later, sees `isWorkerStopping()` true.

#### Replaying histories

`testing.WorkflowReplayer` replays recorded histories against current workflow code to catch changes that would break running workflows.

<!-- example: testing-replay -->
```zig
const history = try env.client.fetchHistory(arena.allocator(), run.id, run.run_id);
var replayer: WorkflowReplayer = .init(testing.allocator, testing.io);
defer replayer.deinit();
try replayer.registerWorkflow(Greeting);
try replayer.replayHistory(history);

// ChangedGreeting is registered as "greeting" but no longer calls its activity.
var changed: WorkflowReplayer = .init(testing.allocator, testing.io);
defer changed.deinit();
try changed.registerWorkflow(ChangedGreeting);
try testing.expectError(error.Nondeterminism, changed.replayHistory(history));
```

Notes:

- `replayHistoryJson` and `replayHistoryFromJsonFile` read histories exported by the Temporal CLI or Web UI.
- `replayHistories` replays many histories and returns a result for each instead of stopping at the first failure.
- Set `workflow_id` and `initWithOptions` to the original values when the workflow's commands depend on them.

#### Metrics recorder
<!-- stub: temporal-sdk-zig-d51.11.15 -->
Not implemented yet.
A thread-safe `testing.MetricsRecorder` handler that keeps recorded metrics for tests to check.

### Observability

#### Logging

`ctx.logger()` returns a `log.Logger` that takes a message and a struct of fields and adds the workflow's or activity's context fields. Records go to a `log.Sink`, by default `std.log` under the `temporal` scope; set another with `client.Options.logger` or `worker.Options.logger`.

<!-- example: observability-logging -->
```zig
/// A `LineSink`'s log function: records arrive from many threads at once.
fn recordLine(ptr: ?*anyopaque, level: log.Level, message: []const u8, fields: []const log.Field) void {
    const s: *LineSink = @ptrCast(@alignCast(ptr.?));
    s.mutex.lockUncancelable(s.io);
    defer s.mutex.unlock(s.io);
    var line: std.Io.Writer.Allocating = .init(s.arena);
    line.writer.print("{t} {s}", .{ level, message }) catch return;
    for (fields) |f| line.writer.print(" {s}={s}", .{ f.key, f.value }) catch return;
    s.lines.append(s.arena, line.written()) catch return;
}

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = try ctx.logger().with(ctx.allocator(), .{ .customer = name });
    logger.info("greeting started", .{});
    const result = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
    logger.info("greeting done", .{ .result = result });
    return result;
}
```

Notes:

- A sink is called concurrently from workflow, activity, and sdk-core threads and must outlive its clients and workers.
- A workflow's logger drops records while it replays history, unless `worker.Options.log_during_replay` is set.
- `Logger.with` allocates from the arena it is given: in a workflow, build the logger once.
- A worker logs each workflow run it evicts from its cache, with `run_id`, `reason`, `message`, and, for a run it had cached, `workflow_id` and `workflow_type`: at error for nondeterminism or a fatal core error, at warn when fetching history failed, and at debug for routine cache evictions.

#### Metrics

Set `client.Options.metrics` to export metrics to Prometheus, an OpenTelemetry collector, or a custom `metrics.Handler`. Workers on the client export sdk-core's metrics through it, and code records its own through `ctx.metrics()` or `client.metrics()`.

<!-- example: observability-metrics -->
```zig
fn countedGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const greetings = try ctx.metrics().counter("greetings", .{ .description = "Greetings requested" });
    defer greetings.deinit();
    greetings.add(1, .{ .language = "en" });
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

// Serve metrics at http://<address>/metrics for Prometheus, or push them to a collector.
fn prometheusMetrics(address: []const u8) temporal.metrics.Options {
    return .{ .exporter = .{ .prometheus = .{ .bind_address = address } }, .global_tags = &.{.{ .key = "app", .value = "greetings" }} };
}
const otel_metrics: temporal.metrics.Options = .{ .exporter = .{ .opentelemetry = .{ .url = "http://localhost:4317" } } };
```

Notes:

- Free each instrument with `deinit` before its handler. Recording returns no error: a value whose attributes can't be allocated is dropped.
- A workflow's handler adds `namespace`, `task_queue`, and `workflow_type` and records nothing during replay; an activity's also adds `activity_type`.
- Without an exporter, every handler is `Handler.noop`.

#### Interceptors

An `interceptor.Worker` hands out each workflow run's `WorkflowInbound` and `WorkflowOutbound` interceptors and each activity attempt's `ActivityInbound` and `ActivityOutbound`. An `interceptor.Client` wraps every request a client sends, except `Client.checkHealth`. Install them with `worker.Options.interceptors`, `client.Options.worker_interceptors`, and `client.Options.interceptors`.

<!-- example: observability-interceptors -->
```zig
/// Logs each activity a workflow schedules.
const ActivityAudit = struct {
    pub fn interceptWorkflow(_: *ActivityAudit, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Scheduling);
        run.* = .{ .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(Scheduling, run) };
    }
};
const Scheduling = struct {
    ctx: *workflow.Context,
    pub fn executeActivity(s: *Scheduling, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
        s.ctx.logger().info("scheduling activity", .{ .activity = input.activity_type });
        return next.executeActivity(input);
    }
};
```

Notes:

- `from(T, impl)` builds any of them from the methods `T` declares; the others pass the call through to `next`. Activity interceptors come from `interceptActivity` the same way.
- Workflow interceptors run as part of the workflow, again on replay, and follow the workflow rules. Query and update-validation interceptors run outside any coroutine, like their handlers, and must not block or change state.
- Workflow and activity interceptor inputs are type-erased (`converter.EncodedValues`, a header, options), and results are encoded payloads. A client interceptor reads each request and response as its generated protobuf type.
- Client interceptors run on any calling thread and must be thread-safe.

#### Context propagation

A `propagation.ContextPropagator` carries values such as a tenant or trace ID in the headers of a workflow's calls. Values live in a `propagation.Values` store under typed keys from `propagation.key`.

<!-- example: observability-propagation -->
```zig
const tenant = temporal.propagation.key([]const u8, "tenant");
const propagators = [_]temporal.propagation.ContextPropagator{temporal.propagation.HeaderPropagator(tenant).propagator};
// TenantGreeting calls this activity, and the tenant goes along in its header.
fn tenantGreet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    const who = ctx.propagated().get(tenant) orelse "nobody";
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s} of {s}!", .{ name, who });
}
test "observability: a tenant travels from the client to the activity" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(TenantGreeting);
    try env.registerActivity(TenantGreet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var values: temporal.propagation.Values = .init(arena.allocator());
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(TenantGreeting, .{ .context = &values }, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ziggy of acme!", result);
}
```

Notes:

- Workers on the client use `client.Options.propagators` too.
- A workflow's `ctx.propagated()` holds its start header's values and passes them on to every activity, child, signal, and continue-as-new it sends.
- `Values.put` copies the value but borrows what it points to, such as a string's bytes.

#### Tracing
<!-- stub: temporal-sdk-zig-d51.12.5 -->
Not implemented yet.
A tracing interceptor that creates spans around workflow, activity, and client calls and exports them to OpenTelemetry.

### Nexus

A Nexus service is a named set of operations that one worker handles and that workflows call through a Nexus endpoint, often from another namespace.

#### Services and handlers

`nexus.OperationDef` names an operation with its input and output types, and `nexus.ServiceDef` names a service and lists its operations. This contract holds only names and types, so a calling program imports it without the handlers. `nexus.ServiceHandler` binds each operation to a handler function, and `Worker.registerNexusService` registers the result.

<!-- example: nexus-service -->
```zig
const nexus = temporal.nexus;

// The contract: names and types only, shared by the handling worker and its callers.
pub const Echo = nexus.OperationDef("echo", []const u8, []const u8);
pub const Greet = nexus.OperationDef("greet", []const u8, []const u8);
pub const GreetingService = nexus.ServiceDef("greeting-service", .{ Echo, Greet });

// A synchronous operation returns its output.
fn echo(ctx: *nexus.Context, message: []const u8) ![]const u8 {
    if (message.len == 0) return ctx.handlerError(.{ .type = .bad_request, .message = "the message is empty" });
    return message;
}

// A workflow-run operation returns the workflow to start, and completes with its result.
fn greet(ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(Greeting) {
    const id = try std.fmt.allocPrint(ctx.allocator(), "greet-{s}", .{ctx.info().request_id});
    return .{ .options = .{ .id = id }, .args = .{name} };
}

pub const GreetingHandler = nexus.ServiceHandler(GreetingService, .{
    nexus.OperationHandler(Echo, echo),
    nexus.OperationHandler(Greet, greet),
});

fn registerGreetingService(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(Greeting);
    try w.registerNexusService(GreetingHandler);
}
```

Notes:

- A handler is `fn (ctx: *nexus.Context, input: Input) !Output`, or takes a receiver pointer first to share state; register those with `Worker.registerNexusServiceWith` and an instance that outlives the worker. An operation whose input is `void` may leave the input parameter out. Handlers run on the worker's `std.Io`, like activities, and may do any I/O.
- A handler that returns `nexus.WorkflowStart(W)` makes a workflow-run operation: the worker's client starts workflow `W` with the returned options and arguments, and the operation completes with the workflow's result when it closes. The start carries the Nexus request ID, so a retried start finds the workflow the first one started. An empty `options.id` uses the request ID and an empty `options.task_queue` the worker's task queue. Canceling the operation requests cancellation of the workflow.
- A missing handler, a handler for an operation the service doesn't have, two operations with one name, a handler whose input or output type differs from the operation's, or a workflow whose result type isn't the operation's output is a compile error that names the service and the operation.
- `ctx.fail` and `ctx.failCanceled` end the operation as failed or canceled, which the caller doesn't retry. `ctx.handlerError` fails the request with a `nexus.HandlerErrorType`; `bad_request`, `unauthenticated`, `unauthorized`, `not_found`, `conflict`, and `not_implemented` aren't retried by default, the others are, and `retry` overrides the default. Any other error a handler returns is a retried `internal` handler error named after it, except `error.NotFound`, which is `not_found`, and `error.WorkflowExecutionAlreadyStarted`, `error.InvalidOptions`, and `error.InvalidSearchAttribute`, which are `internal` and not retried.
- When the server rejects a workflow-run operation's start or cancel, the handler error follows the gRPC status and carries the server's message: INVALID_ARGUMENT is `bad_request`; NOT_FOUND `not_found`; ALREADY_EXISTS, FAILED_PRECONDITION, and OUT_OF_RANGE an `internal` that isn't retried; ABORTED and UNAVAILABLE `unavailable`; RESOURCE_EXHAUSTED `resource_exhausted`; UNIMPLEMENTED `not_implemented`; DEADLINE_EXCEEDED `upstream_timeout`; any other a retried `internal`.
- What a handler passes to `ctx.fail`, `ctx.failCanceled`, and `ctx.handlerError`, and the strings in a `WorkflowStart`, are stored as given: they must live until the response has been sent, as memory from `ctx.allocator()` does.
- `ctx.info()` gives the service, operation, endpoint, namespace, task queue, request ID, request deadline, headers (`Info.header` looks one up by name, ignoring case), and the caller's links; `ctx.client()` gives the worker's client, and `ctx.logger()` a logger that adds the service, operation, endpoint, and task queue to each message.
- Registering a service name twice returns `error.AlreadyRegistered`.
- A worker with a registered service polls its task queue for Nexus tasks: see [Serving operations](#serving-operations).

#### Calling operations from workflows

`workflow.Context.executeNexusOperation` schedules an operation of a service contract through a Nexus endpoint and returns a `workflow.NexusOperationFuture` for its start and its result. The operation runs wherever the endpoint routes, often a worker in another namespace.

<!-- example: nexus-caller -->
```zig
fn greetThroughNexus(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeNexusOperation(GreetingService, Greet, .{
        .endpoint = "greeting-endpoint",
        .schedule_to_close_timeout = .fromSeconds(60),
    }, name);
    // The token of the started workflow-run operation; null if the result came with the start.
    if (try future.started()) |token| ctx.logger().info("greeting started", .{ .token = token });
    return future.get();
}
pub const GreetThroughNexus = workflow.Def("greet_through_nexus", greetThroughNexus);
```

Notes:

- `options.endpoint` names an endpoint that an operator created on the server, routing to a namespace and task queue. An endpoint the server doesn't know fails the workflow task, which is retried, rather than the operation; an empty one makes the future return `error.InvalidOptions`. In tests, `testing.WorkflowEnvironment.createNexusEndpoint` creates one on the environment's dev server.
- `get` returns `error.NexusOperationFailed` when the operation failed, timed out, was canceled, or didn't start; `future.failure()` is then a Nexus operation failure whose cause is the handler's failure, a timeout, or a cancellation. `started` waits for the start and returns the operation token, or null when the result arrived with the start.
- `schedule_to_close_timeout` bounds the whole operation, `schedule_to_start_timeout` the time until the handler starts it, and `start_to_close_timeout` an asynchronous operation's run; each is optional. `headers` are sent to the handler as Nexus headers and recorded in history without the payload codecs, and `summary` describes the operation in the UI.
- `future.cancel()`, or canceling the coroutine that waits in `get` or `started`, requests cancellation as `options.cancellation_type` says, and the future resolves when the core reports it. `wait_cancellation_completed` (the default) resolves once the operation has ended, `wait_cancellation_requested` once the handler has accepted the request, and `try_cancel` and `abandon` in the next activation, `abandon` without telling the handler. After `future.cancel()`, `get` returns `error.NexusOperationFailed` with the cancellation as the failure; a canceled coroutine gets `error.Canceled`.
- With `try_cancel`, a workflow that closes right after canceling may close before the server has sent the cancel to the handler, which then never receives it; use `wait_cancellation_requested` when the handler must see the cancel.
- With sdk-core v1.2.0, a workflow that is still running when an operation it abandoned ends fails its workflow tasks from then on: use `abandon` only when the workflow closes first, or use `try_cancel`.
- A service that lacks the operation, or an operation whose types differ from the service's, is a compile error naming the service and the operation. `executeNexusOperationByName` calls a service this program has no contract for, by service and operation name.
- Workflow interceptors don't see Nexus calls.

#### Serving operations

A worker with a registered Nexus service polls its task queue for Nexus tasks, beside any workflows and activities it has, and runs each request (a start or a cancel of an operation) on a task of its own on the worker's `std.Io`. A worker with only Nexus services runs too. Callers reach it through a Nexus endpoint that routes to the worker's namespace and task queue.

<!-- example: nexus-serve -->
```zig
/// A worker that serves `GreetingService` on task queue "greetings" until SIGINT or SIGTERM.
fn serveGreetings(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !void {
    var w = try temporal.worker.Worker.init(gpa, io, client, "greetings", .{
        .max_concurrent_nexus_task_executions = 20,
    });
    defer w.deinit();
    try registerGreetingService(&w);
    try w.runUntilSignal();
}
```

`testing.WorkflowEnvironment.registerNexusService` (and `registerNexusServiceWith`) registers a service on the environment's worker, and `createNexusEndpoint` creates the endpoint, so a test runs a caller workflow against real handlers:

<!-- example: nexus-test -->
```zig
test "a workflow greets through the Nexus endpoint" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetThroughNexus);
    try env.registerWorkflow(Greeting);
    try env.registerNexusService(GreetingHandler);
    // Routes to the environment's namespace and task queue, where its worker serves the service.
    try env.createNexusEndpoint("greeting-endpoint", .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetThroughNexus, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ziggy!", result);
}
```

Notes:

- The endpoint is created on the server by an operator (`temporal operator nexus endpoint create`, or the operator service's `CreateNexusEndpoint`), not by the worker. The caller's namespace may differ from the handler's; the dev server takes `--namespace` for a second one.
- The worker cancels a handler through its `std.Io` when the request's deadline passes or the worker shuts down after `graceful_shutdown_period`; `ctx.cancelReason()` is then `timed_out` or `worker_shutdown`. A canceled handler that returns `error.Canceled` sends no response: the caller's attempt times out and the server retries the request until the operation's timeout. Anything else it returns after the cancel is reported as usual.
- A request for a service or an operation the worker doesn't have fails as `not_found`, naming both; one the worker can't decode fails as `bad_request`. Neither is retried.
- A request the worker's Io has no thread for fails as `resource_exhausted`, which the server retries.
- The client's payload codecs apply to an operation's input, its result, and the details of its failure.
- The caller's links arrive in `ctx.info().links`, and a workflow-run operation's start returns a link to its workflow, which the caller's history records. Only workflow-event links are converted; a link of another kind, or one that doesn't parse, is logged and dropped.
- `max_concurrent_nexus_task_executions` and `max_concurrent_nexus_task_pollers` bound the requests handled and the polls open at once (see [Options](#options)).
- Worker interceptors don't see Nexus requests.

#### Standalone operations
<!-- stub: temporal-sdk-zig-d51.5.12 -->
Not implemented yet.
Planned: starting, describing, and canceling Nexus operations from a client, outside a workflow.

### Contrib
<!-- stub: temporal-sdk-zig-d51.13.2, temporal-sdk-zig-d51.13.4, temporal-sdk-zig-d51.13.5, temporal-sdk-zig-d51.13.6 -->
Not implemented yet.
Planned: workflow streams, cloud storage drivers, serverless worker helpers, and a static determinism check.

## Parity

The SDK measures its feature coverage with a set of samples, each demonstrating one feature in idiomatic Zig. `zig build samples-test` validates every sample, with its tests, against a dev server. Fifty-four samples pass today. [PARITY.md](PARITY.md) describes how parity is measured, holds the feature table that gives each feature's status in this SDK and the README.md section or sample that shows it, and lists the samples; the code is in [samples](samples).

## Versioning

Releases follow [SemVer 2.0](https://semver.org/spec/v2.0.0.html) and are tagged `v<version>`. Until the SDK reaches parity, releases are 0.x pre-releases starting at `0.1.0-alpha.1`, and a minor version may break the API. Each release supports one Zig version and one sdk-core release (`CORE_VERSION`). [CHANGELOG.md](CHANGELOG.md) lists the changes in each release, breaking changes first, and [docs/upgrading.md](docs/upgrading.md) shows how to move code across a release with breaking changes. [docs/releasing.md](docs/releasing.md) describes the policy, what counts as a breaking change, how deprecated and experimental declarations are marked, and how releases are made.

## Development

[CONTRIBUTING.md](CONTRIBUTING.md) describes how to report bugs, propose features, and open pull requests.

### Prerequisites

- [Zig](https://ziglang.org/download/) 0.17.1
- [rustup](https://rustup.rs). The Rust version is pinned in [deps/rust-toolchain.toml](deps/rust-toolchain.toml) and installed automatically.
- git, with submodules: `git submodule update --init --depth 1`

### Building and testing

```bash
zig build test           # unit tests; builds the Rust core with cargo on first run
zig build fmt            # check formatting
zig build samples-test   # build the samples and validate them against a dev server
zig build readme-check   # check that README.md's code blocks equal their examples
zig build readme         # rewrite README.md's code blocks from examples/readme
```

README.md's code blocks are copies of regions in `examples/readme/`, which `zig build test` compiles and runs, and `zig build test` fails when a copy differs from its region.

A clean build of the Rust core takes a minute or two. Later builds reuse it from `.zig-cache/cargo-target`. To skip cargo, pass a prebuilt core library with `-Dcore-lib=<path>`, or an extracted core bundle (library and header, from `scripts/package-core-bundle.sh`) with `-Dcore-bundle=<dir>`.

Fiber and workflow runtime code must pass in optimized builds too:

```bash
zig build test -Doptimize=ReleaseSafe
```

### Generated code and the Rust core

- `zig build gen-proto` regenerates `src/proto` from the sdk-core protos. The generated code is committed.
- `scripts/check-core-version.sh` checks that the sdk-core submodule, the cargo workspace in `deps/`, and the proto list agree.
- [docs/core-bump.md](docs/core-bump.md) describes moving to a new sdk-core release.

### Repository layout

- `src/` contains the SDK. `temporal.zig` is the root module, and `client`, `worker`, `workflow`, `activity`, `nexus`, `converter`, and `testing` are its public namespaces. `bridge.zig` wraps the sdk-core C API, and `proto/` holds the generated protobuf types.
- `deps/` holds the sdk-core submodule and the cargo workspace that builds its C bridge.
- `samples/` contains the samples, one per feature; [PARITY.md](PARITY.md) lists them and holds the feature table.
- `examples/readme/` contains the compiled examples that this README's code blocks copy.
- `tools/` contains build tools: the samples runner and the README example checker.
- `scripts/` contains maintenance scripts.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for issues and pull requests, and [AGENTS.md](AGENTS.md) for how the project is built, tested, and reviewed. Work is tracked with [beads](https://github.com/gastownhall/beads) (`bd ready` lists open work).

## License

MIT. See [LICENSE](LICENSE). The vendored [sdk-core](https://github.com/temporalio/sdk-core) is also MIT licensed.
