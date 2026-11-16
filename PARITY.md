# Parity

This file records how the SDK's feature coverage is measured, which features of Temporal's SDKs the Zig SDK has, and which samples demonstrate them. It is the one file in the repository that names other SDKs.

## How parity is measured

This SDK is not a port of another SDK. It measures its feature coverage against the features Temporal's official SDKs offer: [Go](https://github.com/temporalio/sdk-go), [Java](https://github.com/temporalio/sdk-java), [Python](https://github.com/temporalio/sdk-python), [TypeScript](https://github.com/temporalio/sdk-typescript), [.NET](https://github.com/temporalio/sdk-dotnet), [PHP](https://github.com/temporalio/sdk-php), and [Ruby](https://github.com/temporalio/sdk-ruby). Each feature is expressed the Zig way, with its own names and types, not copied from one of them.

- A feature that more than one official SDK offers is a parity requirement, and has a row in the [feature table](#feature-table). The rows come from a sweep of every topic in each SDK's developer guide (`https://docs.temporal.io/develop/<sdk>`, November 2026), together with the SDKs' public APIs where a feature has no page of its own (such as the failure converter). A feature only one SDK offers is not; [Features only one SDK offers](#features-only-one-sdk-offers) lists them, and the ones the Zig SDK has anyway.
- Each feature has a release stage: GA when it is generally available in more than one official SDK, and pre-release (Temporal's Pre-release and Public Preview stages) otherwise. Stages come from Temporal's documentation, changelog, and the SDKs' release notes, as of November 2026; a row says where the SDKs' stages differ.
- The first release needs parity with the GA features only. Pre-release features are tracked but don't block it. The owner may defer a GA feature that is still partial or missing past the first release: its row's **Stage** says `GA, deferred` and its bead stays open, but it doesn't block the first release either.
- A feature is demonstrated by a sample under [samples](samples), validated against a dev server by `zig build samples-test`. Every sample of the Go SDK's [samples](https://github.com/temporalio/samples-go) gets a Zig sample that demonstrates the same feature in idiomatic Zig, with its own names, payload shapes, and tests; the [Samples](#samples) table lists them, and the Samples badge on README.md shows whether they pass.

Where the SDKs differ because of the machinery underneath, this SDK behaves like the other SDKs built on Temporal's Rust core (.NET, Python, TypeScript, Ruby): version checks use patches as well as version numbers, and tests run against the dev server and the time-skipping test server instead of an in-memory mock.

## Feature table

The tables below are grouped by area. Each row is a feature that more than one official SDK offers. **SDKs** names the SDKs that offer it ("all" is all seven). **Stage** is `GA` or `pre-release`, or `GA, deferred` for a GA feature the owner has deferred past the first release (only on a partial or missing row that names its bead). **Zig** is the Zig SDK's status on main:

- `supported`: the feature exists; **Where** links the README.md section or the sample that shows it.
- `partial`: some of it exists; **Where** names the bead that tracks the rest (or `unfiled` where none does) and says what is missing.
- `missing`: none of it exists; **Where** names its bead or `unfiled`.
- `not applicable`: the feature has no meaning for a Zig SDK; **Where** says why.

A status counts every public declaration in `src/`, documented or not. A feature that user code can reach only by composing public API (a raw service call through `(try client.currentConnection()).call`, or a helper used for another purpose) or that README.md doesn't document is `partial`, never `missing`: its row says what reaches it today and what is still missing. A "Missing:" note lists what the SDK lacks as its own API; where the server offers an operation, a raw call can still make it.

`scripts/parity-check.sh` checks the table (statuses, stages, links, and bead IDs) and prints the GA features that are still partial or missing: those the first release still needs, and, under their own heading, those deferred past it. `zig build readme-check` runs it and its tests (`scripts/parity-check-test.sh`), and CI runs `zig build readme-check`; run it after changing this file or README.md's headings. It reads README.md's anchors with `scripts/github-anchors.awk`, which accepts only plain top-level `#` headings and fails on anything that could hide or create a heading another way.

### Client

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Connect with TLS, mutual TLS, and gRPC headers | all | GA | supported | [Connecting](README.md#connecting) |
| API key authentication | all | GA | supported | [Connecting](README.md#connecting) |
| Client RPC retry policy and a client-wide RPC timeout | all | GA | supported | [Connecting](README.md#connecting) |
| RPC timeout and gRPC metadata on a single call | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Connecting](README.md#connecting) |
| Environment configuration: profiles from a TOML file and environment variables | Go, Python, TypeScript, .NET, Ruby, Java | GA | supported | [Connecting](README.md#connecting), [external-env-conf](samples/external-env-conf) |
| HTTP CONNECT proxy for the client connection | Python, TypeScript, .NET, Ruby, Go | GA | supported | [Proxy, keep-alive, and worker heartbeats](README.md#proxy-keep-alive-and-worker-heartbeats) |
| gRPC keep-alive settings | Python, TypeScript, .NET, Ruby | GA | supported | [Proxy, keep-alive, and worker heartbeats](README.md#proxy-keep-alive-and-worker-heartbeats) |
| Lazy connection, and changing the API key or headers on a connected client | Go, Python, TypeScript, .NET, Ruby | GA | supported | [Connecting](README.md#connecting) |
| Clients for several namespaces over one connection | all | GA | supported | [Connecting](README.md#connecting) |
| Start a workflow: ID reuse and conflict policies, timeouts, retry policy, memo, search attributes | all | GA | supported | [Starting workflows](README.md#starting-workflows), [helloworld](samples/helloworld) |
| Workflow start delay | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [start-delay](samples/start-delay) |
| Cron workflows | all | GA | supported | [cron](samples/cron) |
| Eager workflow start (Public Preview) | Go, Java, Python, .NET, Ruby | pre-release | supported | [eager-workflow-start](samples/eager-workflow-start) |
| Workflow handles: result, describe, and history | all | GA | supported | [Describing, listing, cancelling, and terminating](README.md#describing-listing-cancelling-and-terminating) |
| List and count workflows with a visibility query | all | GA | supported | [Describing, listing, cancelling, and terminating](README.md#describing-listing-cancelling-and-terminating) |
| Cancel and terminate workflows | all | GA | supported | [Describing, listing, cancelling, and terminating](README.md#describing-listing-cancelling-and-terminating) |
| Signal, query, and update a workflow | all | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates), [reqrespupdate](samples/reqrespupdate) |
| Signal-with-start | all | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates) |
| Update-with-start | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates), [early-return](samples/early-return) |
| Signal, query, and update by name from the client | all | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates) |
| Schedules: create, describe, update, pause, trigger, backfill, list, delete | all | GA | supported | [Schedules](README.md#schedules), [schedule](samples/schedule) |
| Complete, fail, and heartbeat activities asynchronously from a client | all | GA | supported | [Completing activities asynchronously](README.md#completing-activities-asynchronously), [expense](samples/expense) |
| Standalone activities (GA September 2026; Public Preview before) | Go, Java, Python, TypeScript, .NET | GA | supported | [Standalone activities](README.md#standalone-activities), [standalone-activity](samples/standalone-activity) |
| Worker deployment management: current and ramping versions | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Worker deployments](README.md#worker-deployments), [worker-versioning](samples/worker-versioning) |
| Update workflow execution options (versioning override) | Go, Java, Python, TypeScript, .NET | GA | supported | [Workflow reset and execution options](README.md#workflow-reset-and-execution-options) |
| Reset a workflow | Go, Java | GA, deferred | partial | `temporal-sdk-zig-d51.5.30`. Missing: operations applied after a reset, and helpers that pick a reset point. |
| Namespace management | Go, Java, TypeScript | GA | partial | `temporal-sdk-zig-d51.2.5`. Missing: typed calls to register, describe, list, and update namespaces, and their README.md section; today a namespace is managed only through raw WorkflowService calls (`(try client.currentConnection()).call`). |
| Raw gRPC service calls (workflow, operator, and test services) | all | GA | partial | `temporal-sdk-zig-d51.2.5`. Missing: typed calls that take and return the generated protobuf types, and a README.md section; `(try client.currentConnection()).call` already sends encoded requests to the workflow, operator, test, cloud, and health services. |
| Cloud Operations API client | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.2.5`. Missing: a typed Cloud Operations client and its README.md section; raw `.cloud` service calls go through `(try client.currentConnection()).call`. |

### Workflows

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Typed workflow definitions and registration | all | GA | supported | [Definition](README.md#definition), [helloworld](samples/helloworld) |
| Workflow initialization that sees the start arguments before handlers run | Java, Python, .NET, PHP, Ruby | GA | supported | [Definition](README.md#definition) (a function workflow reads its arguments, then registers its handlers) |
| Determinism enforcement at run time (sandbox, illegal-call checks) | Python, TypeScript, .NET, Ruby | GA | supported | [Determinism](README.md#determinism) |
| Deterministic random numbers | Java, Python, TypeScript, .NET, Ruby, PHP | GA | supported | [Determinism](README.md#determinism) |
| Deterministic UUIDs | Java, Python, TypeScript, .NET, PHP | GA | partial | `temporal-sdk-zig-d51.7.33`. Missing: a workflow API for it and README.md documentation; `temporal.client.uuid(ctx.allocator(), ctx.io())` already gives a UUID that is the same on replay, because the workflow `std.Io`'s randomness is seeded from the run. |
| Activities from workflows: timeouts, retry policy, cancellation types | all | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [retryactivity](samples/retryactivity) |
| Local activities | all | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [greetingslocal](samples/greetingslocal) |
| Activities by name, without a typed definition | all | GA | supported | [Dynamic activities](README.md#dynamic-activities), [dynamic](samples/dynamic) |
| Durable timers, conditions, and conditions with a timeout | all | GA | supported | [Timers and conditions](README.md#timers-and-conditions), [timer](samples/timer) |
| Summaries and details on workflow starts, activities, timers, and child workflows | all | pre-release | supported | [Activities and local activities](README.md#activities-and-local-activities), [Timers and conditions](README.md#timers-and-conditions), [Child workflows](README.md#child-workflows) |
| Current details shown in the UI | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.7.14`. Missing: a workflow API that sets current details; a workflow can answer the `__temporal_workflow_metadata` query itself with a `WorkflowMetadata` message (the name isn't reserved). |
| Concurrency inside workflows: futures, select, mutexes, semaphores, queues | all | GA | supported | [Concurrency](README.md#concurrency), [splitmerge-select](samples/splitmerge-select), [mutex](samples/mutex) |
| Signal handlers and signal channels | all | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1), [await-signals](samples/await-signals) |
| Query handlers | all | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1), [query](samples/query) |
| Update handlers and validators | Go, Java, Python, TypeScript, .NET, Ruby, PHP | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1), [update](samples/update) |
| Dynamic signal handler | Java, Python, TypeScript, .NET, Ruby, PHP | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1) |
| Dynamic query handler | Java, Python, TypeScript, .NET, Ruby, PHP | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1) |
| Dynamic update handler | Java, Python, TypeScript, .NET, Ruby | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1) |
| Read-only query handlers and update validators | Java, Python, TypeScript, .NET, Ruby | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1) |
| Wait for all signal and update handlers to finish | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [safe_message_handler](samples/safe_message_handler) |
| Warning for signal and update handlers still running when the workflow ends | Java, Python, TypeScript, .NET, Ruby | GA | supported | [Signals, queries, and updates](README.md#signals-queries-and-updates-1) |
| Child workflows: options, parent close policy, signals, cancellation | all | GA | supported | [Child workflows](README.md#child-workflows), [child-workflow](samples/child-workflow) |
| Signal and cancel external workflows | all | GA | supported | [External workflows](README.md#external-workflows), [synchronous-proxy](samples/synchronous-proxy) |
| Continue-as-new, with the server's suggestion and history size | all | GA | supported | [Continue-as-new](README.md#continue-as-new), [child-workflow-continue-as-new](samples/child-workflow-continue-as-new) |
| Continue-as-new by workflow name | all | GA | partial | `temporal-sdk-zig-d51.7.36`. Missing: `continueAsNewByName`, continuing as a workflow type named at run time; `ctx.continueAsNew` takes any `Def`, so a run can continue as another type it has a definition for. |
| Upgrade on continue-as-new (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.7.35`. Missing: a `ContinueAsNewOptions` field that chooses the new run's initial versioning behavior; continuing as a definition whose behavior is `auto_upgrade` moves the new run to the current version only after its first workflow task, which runs on the old version. |
| Cancellation, with cleanup that runs after the cancel | all | GA | supported | [Cancellation](README.md#cancellation), [cancellation](samples/cancellation) |
| Shielding work from cancellation (cancellation scopes, non-cancellable sections) | Java, TypeScript, Python, .NET | GA | supported | [Cancellation](README.md#cancellation) |
| Patching: patched and deprecated patches | Python, TypeScript, .NET, Ruby | GA | supported | [Versioning and patching](README.md#versioning-and-patching) |
| Version numbers in code (get version) | Go, Java, PHP | GA | supported | [Versioning and patching](README.md#versioning-and-patching) |
| Side effects | Go, Java, PHP | GA | supported | [Side effects, memo, and search attributes](README.md#side-effects-memo-and-search-attributes) |
| Mutable side effects | Go, Java | GA, deferred | partial | `temporal-sdk-zig-d51.7.34`. Missing: a mutable side effect that records a marker only when the value changes; calling `ctx.sideEffect` each time returns the current value but records a marker every call. |
| Workflow-local variables: a value scoped to one run that any of its code can reach | Go, Java | GA, deferred | partial | `temporal-sdk-zig-d51.7.15`. Missing: a per-run value that code holding only the workflow context can reach, like Go's local variables and Java's workflow locals; a run's state lives in its struct fields or in `ctx.allocator()` memory and is passed down explicitly, and `ctx.propagated()` is read-only. |
| Memo, read and upserted | all | GA | supported | [memo](samples/memo) |
| Search attributes, typed and upserted | all | GA | supported | [searchattributes](samples/searchattributes) |
| Workflow info, replay state, and the previous run's result or failure | all | GA | supported | [Workflow info](README.md#workflow-info) |
| Dynamic workflows | all | GA | supported | [Dynamic workflows](README.md#dynamic-workflows), [dynamic-workflows](samples/dynamic-workflows) |
| Choosing which errors fail the workflow and which fail the workflow task | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Failure policy](README.md#failure-policy) |
| Deadlock detection | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Deadlock detection](README.md#deadlock-detection) |
| Stack trace query | all | GA | partial | `temporal-sdk-zig-d51.6.15`. Missing: the SDK answering `__stack_trace` with where each coroutine is blocked; a workflow can register a query of that name (it isn't reserved) and answer it from its own bookkeeping. |
| Enhanced stack trace query | TypeScript, Python | GA, deferred | partial | `temporal-sdk-zig-d51.6.9`. Missing: the SDK answering `__enhanced_stack_trace` with each coroutine's location and the source around it; a workflow can register a query of that name (it isn't reserved) and answer it from its own bookkeeping. |
| Workflow metadata query and handler descriptions | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.6.9`. Missing: the SDK answering `__temporal_workflow_metadata` from its registered handlers, and handler descriptions; a workflow can register that query itself. |
| Event groups: tying a workflow's commands and their history events to a named group (experimental) | Go, TypeScript | pre-release | missing | `temporal-sdk-zig-d51.13.7`. Commands carry no event group markers, and workflow options have no field for them. |
| Priority and fairness keys on workflows, activities, and children (Task Queue Priority and Fairness GA, Replay 2026) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [Starting workflows](README.md#starting-workflows) |

### Activities

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Typed activity definitions and registration | all | GA | supported | [Definition](README.md#definition-1), [greetings](samples/greetings) |
| Activity context and info | all | GA | supported | [Context](README.md#context) |
| Heartbeats with details, read back on retry | all | GA | supported | [Heartbeating and cancellation](README.md#heartbeating-and-cancellation) |
| Activity cancellation, with the reason | all | GA | supported | [Heartbeating and cancellation](README.md#heartbeating-and-cancellation) |
| Worker shutdown notice to running activities | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Worker shutdown](README.md#worker-shutdown) |
| Asynchronous completion | all | GA | supported | [Asynchronous completion](README.md#asynchronous-completion), [expense](samples/expense) |
| Application failures: non-retryable, details, next retry delay, category (benign failures) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Failures](README.md#failures) |
| Dynamic activities | all | GA | supported | [Dynamic activities](README.md#dynamic-activities), [dynamic](samples/dynamic) |
| Activity pause, unpause, and reset (Activity Operations, Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.5.17`. Missing: a heartbeat that reports the activity paused or reset, and typed client calls that pause, unpause, or reset (today only raw WorkflowService calls through `(try client.currentConnection()).call`); a cancel already gives the reason `paused` or `reset`. |

### Workers

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Run and stop a worker, with graceful shutdown | all | GA | supported | [Running and stopping](README.md#running-and-stopping), [Shutdown](README.md#shutdown) |
| Concurrency limits, poller counts, and fixed-size slots | all | GA | supported | [Options](README.md#options) |
| Activity rate limits, per worker and per task queue | all | GA | supported | [Options](README.md#options) |
| Workflow cache size and sticky queue timeout | all | GA | supported | [Options](README.md#options) |
| Eager activity execution | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Options](README.md#options) |
| Several workers in one process, on one client | all | GA | supported | [Running and stopping](README.md#running-and-stopping), [worker-specific-task-queues](samples/worker-specific-task-queues) |
| Worker Versioning with Worker Deployments (GA March 2026) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Versioning](README.md#versioning), [worker-versioning](samples/worker-versioning) |
| Build-ID-based versioning (deprecated) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.5.37`. Missing: typed client calls to manage and query build-ID compatibility sets (UpdateWorkerBuildIdCompatibility, GetWorkerBuildIdCompatibility) and task reachability, which today are only raw WorkflowService calls through `(try client.currentConnection()).call`. The worker side exists: `worker.Options.use_build_id_versioning` and `build_id`, and the `versioning_intent` field of `workflow.ActivityOptions`; see [Versioning](README.md#versioning). |
| Resource-based slot tuner (Public Preview) | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | missing | `temporal-sdk-zig-d51.9.3`. `worker.Options` has no option for it; only the SDK's internal bridge to sdk-core passes one. |
| Custom slot suppliers | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | missing | `temporal-sdk-zig-d51.9.3`. `worker.Options` has no option for it; only the SDK's internal bridge to sdk-core passes one. |
| Poller autoscaling (GA August 2026) | Go, Java, Python, TypeScript, .NET, Ruby | GA | missing | `temporal-sdk-zig-d51.9.4`. `worker.Options` has no option for it; only the SDK's internal bridge to sdk-core passes one. |
| Worker heartbeats to the server (Public Preview) | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | supported | [Proxy, keep-alive, and worker heartbeats](README.md#proxy-keep-alive-and-worker-heartbeats) |
| Client and worker plugins | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.9.7`. Missing: registering plugins on a client or worker and reporting their names to the server; `worker.Options` has no option for it, and only the SDK's internal bridge to sdk-core passes plugin names. A function that adjusts `client.Options` and `worker.Options` before use is plain code. |
| Serverless workers on AWS Lambda and Cloud Run (Public Preview) | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.13.5`. Missing: helpers that start a worker for an invocation, set defaults for it, and drain it before the invocation ends; a program can run `Worker.run` and call `Worker.stop` itself. |

### Data conversion

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Default payload converter: null, binary, JSON, protobuf | all | GA | supported | [Payloads](README.md#payloads) |
| Custom payload conversion | all | GA | supported | [Payloads](README.md#payloads) |
| Raw payloads passed through unconverted | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Payloads](README.md#payloads) |
| Payload codecs for compression and encryption | all | GA | supported | [Payload codecs](README.md#payload-codecs), [encryption](samples/encryption), [snappycompress](samples/snappycompress) |
| Codec server for the Web UI and CLI | Go, Java, Python, TypeScript, .NET | GA | supported | [Payload codecs](README.md#payload-codecs), [codec-server](samples/codec-server) |
| Remote codec client: a payload codec that sends payloads to a codec server's `/encode` and `/decode` | Go, Java | GA, deferred | partial | `temporal-sdk-zig-d51.4.4`. Missing: a ready-made codec that calls a codec server; a `converter.PayloadCodec` can make those HTTP calls itself. |
| Serialization context for converters and codecs | Java, Python, .NET, TypeScript | GA | partial | `temporal-sdk-zig-d51.4.8`. Missing: context for the payload converter; codecs get it. |
| Failures with cause chains and kinds | all | GA | supported | [Failures](README.md#failures-1) |
| Encoded failure attributes (message and stack trace through the codecs) | all | GA | supported | [Failures](README.md#failures-1) |
| Custom failure converter (errors to failures and back) | Go, Java, Python, TypeScript, .NET, Ruby | GA | partial | `temporal-sdk-zig-d51.4.21`. Missing: a pluggable failure converter; the SDK's own conversion is fixed, and `client.Options.encode_failure_attributes` is its only setting. |
| External payload storage (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.4.9`. Missing: storage drivers and a payload size threshold; a `converter.PayloadCodec` can store payloads elsewhere and pass references instead. |

### Testing

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Test environment on a local dev server | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Workflow environment](README.md#workflow-environment) |
| Time-skipping test environment | Java, Python, TypeScript, .NET | GA | supported | [Time skipping](README.md#time-skipping) |
| Activity test doubles in workflow tests | all | GA | supported | [Workflow environment](README.md#workflow-environment) |
| Activity test environment | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Activity environment](README.md#activity-environment) |
| Test environment on an existing server or client | Java, Python, TypeScript, .NET, Ruby | GA | partial | `temporal-sdk-zig-d51.11.18`. Missing: a `testing.WorkflowEnvironment` built on a server or client the test already has (`WorkflowEnvironment.Server` is only `dev` or `time_skipping`); a test can connect a `client.Client` to any server and run a `worker.Worker` on it itself. |
| Heartbeats and cancellation in activity tests | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Activity environment](README.md#activity-environment) |
| Stand-in Nexus handlers in workflow tests | Go, Java | GA | supported | [Serving operations](README.md#serving-operations) |
| Workflow replayer | all | GA | supported | [Replaying histories](README.md#replaying-histories), [multi-history-replay](samples/multi-history-replay) |

### Observability

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Workflow and activity loggers with context, quiet on replay | all | GA | supported | [Logging](README.md#logging), [logadapter](samples/logadapter) |
| Metrics: Prometheus, OpenTelemetry, custom handlers | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Metrics](README.md#metrics), [metrics](samples/metrics) |
| Global tags on every metric | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Metrics](README.md#metrics) |
| Client, workflow, and activity interceptors | all | GA | supported | [Interceptors](README.md#interceptors), [logger-interceptor](samples/logger-interceptor) |
| Nexus interceptors | Go, Java, Python, TypeScript, .NET | GA | partial | `temporal-sdk-zig-d51.12.3`. Missing: Nexus interceptors on the handling worker and on the calling workflow; a service's handler functions can only be wrapped by hand. |
| Context propagation through headers | all | GA | supported | [Context propagation](README.md#context-propagation), [ctxpropagation](samples/ctxpropagation) |
| Context propagation over Nexus calls | Go, Java | GA | partial | `temporal-sdk-zig-d51.12.10`. Missing: propagators applied to Nexus calls; a caller sets `NexusOperationOptions.headers` by hand and a handler reads them with `ctx.info().header`. |
| OpenTelemetry tracing | Go, Java, Python, TypeScript, .NET, Ruby | GA, deferred | partial | `temporal-sdk-zig-d51.12.5`. Missing: a tracing interceptor and span export; the client and worker interceptors and context propagation it would use exist. |

### Nexus

Nexus is GA in Go, Java, and Python, and since August 2026 in TypeScript and .NET.

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Nexus service contracts and synchronous handlers | Go, Java, Python, TypeScript, .NET | GA | supported | [Services and handlers](README.md#services-and-handlers), [nexus](samples/nexus) |
| Workflow-run operations | Go, Java, Python, TypeScript, .NET | GA | supported | [Services and handlers](README.md#services-and-handlers), [nexus](samples/nexus) |
| Calling Nexus operations from workflows | Go, Java, Python, TypeScript, .NET | GA | supported | [Calling operations from workflows](README.md#calling-operations-from-workflows), [nexus](samples/nexus) |
| Nexus operation cancellation types | Go, Java, Python, TypeScript, .NET | GA | supported | [Calling operations from workflows](README.md#calling-operations-from-workflows), [nexus-cancelation](samples/nexus-cancelation) |
| Nexus endpoints and handlers in tests | Go, Java, Python, TypeScript, .NET | GA | supported | [Serving operations](README.md#serving-operations) |
| Standalone Nexus operations from a client (Pre-release, August 2026) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.5.12`. Missing: typed calls to start, describe, and cancel an operation, and their README.md section; today only raw WorkflowService calls through `(try client.currentConnection()).call` reach them. |

### Others

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Workflow streams (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.13.2`. Missing: the stream library; the signals, updates, and queries it is built on exist. |
| AI agent framework integrations (OpenAI Agents, Vercel AI SDK, and others) | Python, TypeScript, Go | pre-release | not applicable | Each wraps an agent framework of its own language; Zig has no such framework to integrate. |

## Features only one SDK offers

These aren't parity requirements. Each entry was checked against the other six official SDKs; a feature a second SDK offers moves to the feature table (the enhanced stack trace query, the remote codec client, workflow-local variables, and event groups did).

| Feature | SDK | Zig |
|---------|-----|-----|
| Sessions: activities pinned to one worker host | Go | Has them: [Sessions](README.md#sessions), [Sessions](README.md#sessions-1) under Workers |
| Static determinism checker (workflowcheck) | Go | Planned (`temporal-sdk-zig-d51.13.6`); the analyzers for other languages are community projects, not part of an official SDK |

The Zig SDK also has a feature no official SDK offers: a [workflow memory limit](README.md#workflow-memory-limit) per cached run.

Sources for the release stages: [Worker Versioning GA](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new) (March 2026, with Upgrade on Continue-as-New in Public Preview), [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements) (Task Queue Priority and Fairness GA; Workflow Streams, External Storage, and worker heartbeats in Public Preview; Serverless Workers in Pre-release), [Durable Digest, August 2026](https://temporal.io/blog/durable-digest-august-2026) (Nexus GA in TypeScript and .NET, Poller Autoscaling GA, Standalone Nexus Operations in Pre-release, Activity Operations and Serverless Workers in Public Preview), [Durable Digest, September 2026](https://temporal.io/blog/durable-digest-september-2026) (Standalone Activities GA), [resource-based auto-tuning in Public Preview](https://temporal.io/changelog/resource-based-worker-auto-tuning-now-in-public-preview), [Eager Workflow Start in Public Preview](https://temporal.io/blog/durable-digest-october-2025), and [Environment Configuration GA](https://temporal.io/changelog/environment-config-ga). Features with no announcement of their own (most of the client, workflow, activity, and testing rows) are GA in every SDK that offers them, per Temporal's [SDK documentation](https://docs.temporal.io/develop); summaries, current details, and the metadata query are marked experimental in the SDKs' APIs.

## Samples

Each Zig sample lives in [samples](samples) and is validated against a dev server, with its tests, by `zig build samples-test`.

| Zig sample | samples-go sample |
|------------|-------------------|
| [helloworld](samples/helloworld) | helloworld |
| [greetings](samples/greetings) | greetings |
| [branch](samples/branch) | branch |
| [splitmerge-future](samples/splitmerge-future) | splitmerge-future |
| [splitmerge-select](samples/splitmerge-select) | splitmerge-selector |
| [choice-exclusive](samples/choice-exclusive) | choice-exclusive |
| [choice-multi](samples/choice-multi) | choice-multi |
| [io-group](samples/io-group) | goroutine |
| [timer](samples/timer) | timer |
| [start-delay](samples/start-delay) | start-delay |
| [retryactivity](samples/retryactivity) | retryactivity |
| [cron](samples/cron) | cron |
| [greetingslocal](samples/greetingslocal) | greetingslocal |
| [cancellation](samples/cancellation) | cancellation |
| [child-workflow](samples/child-workflow) | child-workflow |
| [query](samples/query) | query |
| [memo](samples/memo) | memo |
| [await-signals](samples/await-signals) | await-signals |
| [update](samples/update) | update |
| [early-return](samples/early-return) | early-return |
| [reqrespquery](samples/reqrespquery) | reqrespquery |
| [reqrespupdate](samples/reqrespupdate) | reqrespupdate |
| [safe_message_handler](samples/safe_message_handler) | safe_message_handler |
| [shoppingcart](samples/shoppingcart) | shoppingcart |
| [updatabletimer](samples/updatabletimer) | updatabletimer |
| [sleep-for-days](samples/sleep-for-days) | sleep-for-days |
| [child-workflow-continue-as-new](samples/child-workflow-continue-as-new) | child-workflow-continue-as-new |
| [saga](samples/saga) | saga |
| [dsl](samples/dsl) | dsl |
| [reqrespactivity](samples/reqrespactivity) | reqrespactivity |
| [dynamic](samples/dynamic) | dynamic |
| [worker-specific-task-queues](samples/worker-specific-task-queues) | worker-specific-task-queues |
| [synchronous-proxy](samples/synchronous-proxy) | synchronous-proxy |
| [batch-sliding-window](samples/batch-sliding-window) | batch-sliding-window |
| [expense](samples/expense) | expense |
| [polling](samples/polling) | polling |
| [dynamic-workflows](samples/dynamic-workflows) | dynamic-workflows |
| [mutex](samples/mutex) | mutex |
| [searchattributes](samples/searchattributes) | searchattributes, typed-searchattributes |
| [multi-history-replay](samples/multi-history-replay) | multi-history-replay |
| [recovery](samples/recovery) | recovery |
| [schedule](samples/schedule) | schedule |
| [standalone-activity](samples/standalone-activity) | standalone-activity |
| [eager-workflow-start](samples/eager-workflow-start) | eager-workflow-start |
| [snappycompress](samples/snappycompress) | snappycompress |
| [codec-server](samples/codec-server) | codec-server |
| [pso](samples/pso) | pso |
| [logadapter](samples/logadapter) | slogadapter, zapadapter |
| [ctxpropagation](samples/ctxpropagation) | ctxpropagation |
| [workflow-security-interceptor](samples/workflow-security-interceptor) | workflow-security-interceptor |
| [metrics](samples/metrics) | metrics |
| [logger-interceptor](samples/logger-interceptor) | logger-interceptor |
| [encryption](samples/encryption) | encryption |
| [worker-versioning](samples/worker-versioning) | worker-versioning |
| [nexus](samples/nexus) | nexus |
| [nexus-cancelation](samples/nexus-cancelation) | nexus-cancelation |
| [external-env-conf](samples/external-env-conf) | external-env-conf |

## Samples with no Zig equivalent

None are recorded yet.
