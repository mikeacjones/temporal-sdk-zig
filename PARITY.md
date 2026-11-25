# Parity

This file records how the SDK's feature coverage is measured, which features of Temporal's SDKs the Zig SDK has, and which samples demonstrate them. It is the one file in the repository that names other SDKs.

## How parity is measured

This SDK is not a port of another SDK. It measures its feature coverage against the features Temporal's official SDKs offer: [Go](https://github.com/temporalio/sdk-go), [Java](https://github.com/temporalio/sdk-java), [Python](https://github.com/temporalio/sdk-python), [TypeScript](https://github.com/temporalio/sdk-typescript), [.NET](https://github.com/temporalio/sdk-dotnet), [PHP](https://github.com/temporalio/sdk-php), and [Ruby](https://github.com/temporalio/sdk-ruby). Each feature is expressed the Zig way, with its own names and types, not copied from one of them.

- A feature that more than one official SDK offers is a parity requirement, and has a row in the [feature table](#feature-table). The rows come from a sweep of every topic in each SDK's developer guide (`https://docs.temporal.io/develop/<sdk>`, November 2026), together with the SDKs' public APIs where a feature has no page of its own (such as the failure converter). A feature only one SDK offers is not; [Features only one SDK offers](#features-only-one-sdk-offers) lists them, and the ones the Zig SDK has anyway.
- Each feature has a release stage: GA when it is generally available in more than one official SDK, and pre-release (Temporal's Pre-release and Public Preview stages) otherwise. Stages come from Temporal's documentation, changelog, and the SDKs' release notes, as of November 2026; a row says where the SDKs' stages differ.
- The latest survey, of 2026-11-23, read the latest release of each official SDK: Go 1.51.0 (with its contrib releases of 2026-11-16: opentelemetry v0.9.0, opentelemetry-v2 v0.2.0, opentracing v0.4.0, and datadog v0.7.0), Java 1.41.1, Python 1.36.1, TypeScript 1.26.0, .NET 1.22.0, PHP 2.20, and Ruby 1.11.0, together with Temporal's changelog and blog through 2026-11-23. The next survey starts from the releases published after these.
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
| Connect with TLS, mutual TLS, and gRPC headers | all | GA | supported | [Connecting](README.md#connecting), [helloworldmtls](samples/helloworldmtls) |
| API key authentication | all | GA | supported | [Connecting](README.md#connecting), [helloworld-apiKey](samples/helloworld-apiKey) |
| Client RPC retry policy and a client-wide RPC timeout | all | GA | supported | [Connecting](README.md#connecting) |
| RPC timeout and gRPC metadata on a single call | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Connecting](README.md#connecting) |
| Environment configuration: profiles from a TOML file and environment variables | Go, Python, TypeScript, .NET, Ruby, Java | GA | supported | [Connecting](README.md#connecting), [external-env-conf](samples/external-env-conf) |
| Environment configuration extensions: extension settings in a profile's ext tables (experimental) | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | missing | `temporal-sdk-zig-d51.5.47`. `client.loadOptions` reads profiles through sdk-core and ignores their `ext` tables, so an extension can't reach its settings. |
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
| Reset a workflow | Go, Java | GA | supported | [Workflow reset and execution options](README.md#workflow-reset-and-execution-options) |
| Namespace management | Go, Java, TypeScript | GA | supported | [Namespaces and raw service access](README.md#namespaces-and-raw-service-access) |
| Raw gRPC service calls (workflow, operator, and test services) | all | GA | supported | [Namespaces and raw service access](README.md#namespaces-and-raw-service-access) |
| Cloud Operations API client | Go, Java, Python, TypeScript, .NET | pre-release | partial | `unfiled`. Typed calls to every CloudService method go through `Client.callService(.cloud, ...)` on a client connected to the Cloud Operations API with an API key ([Namespaces and raw service access](README.md#namespaces-and-raw-service-access)). Missing: a run against Temporal Cloud. |

### Workflows

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Typed workflow definitions and registration | all | GA | supported | [Definition](README.md#definition), [helloworld](samples/helloworld) |
| Workflow initialization that sees the start arguments before handlers run | Java, Python, .NET, PHP, Ruby | GA | supported | [Definition](README.md#definition) (a function workflow reads its arguments, then registers its handlers) |
| Determinism enforcement at run time (sandbox, illegal-call checks) | Python, TypeScript, .NET, Ruby | GA | supported | [Determinism](README.md#determinism) |
| Deterministic random numbers | Java, Python, TypeScript, .NET, Ruby, PHP | GA | supported | [Determinism](README.md#determinism) |
| Deterministic UUIDs | Java, Python, TypeScript, .NET, PHP | GA | supported | [Determinism](README.md#determinism) |
| Activities from workflows: timeouts, retry policy, cancellation types | all | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [retryactivity](samples/retryactivity) |
| Local activities | all | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [greetingslocal](samples/greetingslocal) |
| Local activity arguments recorded in the marker | TypeScript, .NET | GA | supported | [Activities and local activities](README.md#activities-and-local-activities) |
| Activities by name, without a typed definition | all | GA | supported | [Dynamic activities](README.md#dynamic-activities), [dynamic](samples/dynamic) |
| Durable timers, conditions, and conditions with a timeout | all | GA | supported | [Timers and conditions](README.md#timers-and-conditions), [timer](samples/timer) |
| Summaries and details on workflow starts, activities, timers, and child workflows | all | pre-release | supported | [Activities and local activities](README.md#activities-and-local-activities), [Timers and conditions](README.md#timers-and-conditions), [Child workflows](README.md#child-workflows) |
| Current details shown in the UI | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.7.14`. Missing: a workflow API that sets current details. |
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
| Signal-with-start from a workflow | Go, Python, TypeScript, .NET | pre-release | supported | [Signal-with-start from a workflow](README.md#signal-with-start-from-a-workflow) |
| Continue-as-new, with the server's suggestion and history size | all | GA | supported | [Continue-as-new](README.md#continue-as-new), [child-workflow-continue-as-new](samples/child-workflow-continue-as-new) |
| Continue-as-new by workflow name | all | GA | supported | [Continue-as-new](README.md#continue-as-new) |
| Upgrade on continue-as-new (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.7.35`. Missing: a `ContinueAsNewOptions` field that chooses the new run's initial versioning behavior; continuing as a definition whose behavior is `auto_upgrade` moves the new run to the current version only after its first workflow task, which runs on the old version. |
| Cancellation, with cleanup that runs after the cancel | all | GA | supported | [Cancellation](README.md#cancellation), [cancellation](samples/cancellation) |
| Shielding work from cancellation (cancellation scopes, non-cancellable sections) | Java, TypeScript, Python, .NET | GA | supported | [Cancellation](README.md#cancellation) |
| Patching: patched and deprecated patches | Python, TypeScript, .NET, Ruby | GA | supported | [Versioning and patching](README.md#versioning-and-patching) |
| Version numbers in code (get version) | Go, Java, PHP | GA | supported | [Versioning and patching](README.md#versioning-and-patching) |
| Side effects | Go, Java, PHP | GA | supported | [Side effects, memo, and search attributes](README.md#side-effects-memo-and-search-attributes) |
| Mutable side effects | Go, Java | GA | supported | [Side effects, memo, and search attributes](README.md#side-effects-memo-and-search-attributes) |
| Workflow-local variables: a value scoped to one run that any of its code can reach | Go, Java | GA | supported | [Workflow-local variables](README.md#workflow-local-variables) |
| Memo, read and upserted | all | GA | supported | [memo](samples/memo) |
| Search attributes, typed and upserted | all | GA | supported | [searchattributes](samples/searchattributes) |
| Workflow info, replay state, and the previous run's result or failure | all | GA | supported | [Workflow info](README.md#workflow-info) |
| Dynamic workflows | all | GA | supported | [Dynamic workflows](README.md#dynamic-workflows), [dynamic-workflows](samples/dynamic-workflows) |
| Choosing which errors fail the workflow and which fail the workflow task | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Failure policy](README.md#failure-policy) |
| Deadlock detection | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Deadlock detection](README.md#deadlock-detection) |
| Stack trace query | all | GA | supported | [Stack trace query](README.md#stack-trace-query) |
| Enhanced stack trace query | TypeScript, Python | GA | supported | [Stack trace and metadata queries](README.md#stack-trace-and-metadata-queries) |
| Workflow metadata query and handler descriptions | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | supported | [Stack trace and metadata queries](README.md#stack-trace-and-metadata-queries) |
| Event groups: tying a workflow's commands and their history events to a named group (experimental) | Go, TypeScript | pre-release | missing | `temporal-sdk-zig-d51.13.7`. Commands carry no event group markers, and workflow options have no field for them. |
| Priority and fairness keys on workflows, activities, and children (Task Queue Priority and Fairness GA, Replay 2026) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Activities and local activities](README.md#activities-and-local-activities), [Starting workflows](README.md#starting-workflows), [Child workflows](README.md#child-workflows) |
| Versioning overrides on child workflows, and one-time overrides (experimental) | Go, Java, Python, TypeScript, .NET, Ruby | pre-release | partial | `temporal-sdk-zig-d51.7.48`. Client starts and `updateWorkflowExecutionOptions` take a `deployment.VersioningOverride` (`pinned` or `auto_upgrade`). Missing: a one-time override, and an override on `workflow.ChildWorkflowOptions`. |

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
| Poller autoscaling (GA August 2026) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Poller behavior](README.md#poller-behavior) |
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
| Payload codecs on header fields, chosen by the field's producer | Go, Java, Python, TypeScript | GA | supported | [Context propagation](README.md#context-propagation) |
| Codec server for the Web UI and CLI | Go, Java, Python, TypeScript, .NET | GA | supported | [Payload codecs](README.md#payload-codecs), [codec-server](samples/codec-server) |
| Remote codec client: a payload codec that sends payloads to a codec server's `/encode` and `/decode` | Go, Java | GA | supported | [Remote codec client](README.md#remote-codec-client) |
| Serialization context for converters and codecs | Go, Java, Python, TypeScript, .NET, PHP | GA | supported | [Payloads](README.md#payloads), [Payload codecs](README.md#payload-codecs) |
| Failures with cause chains and kinds | all | GA | supported | [Failures](README.md#failures-1) |
| Encoded failure attributes (message and stack trace through the codecs) | all | GA | supported | [Failures](README.md#failures-1) |
| Custom failure converter (errors to failures and back) | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Failures](README.md#failures-1) |
| Payload size limits: a worker fails a task whose payloads are too large, and clients warn above a size | all | GA | supported | [Payload size limits](README.md#payload-size-limits) |
| External payload storage (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.4.9`. Missing: storage drivers and a payload size threshold; a `converter.PayloadCodec` can store payloads elsewhere and pass references instead. |

### Testing

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Test environment on a local dev server | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Workflow environment](README.md#workflow-environment) |
| Time-skipping test environment | Java, Python, TypeScript, .NET | GA | supported | [Time skipping](README.md#time-skipping) |
| Activity test doubles in workflow tests | all | GA | supported | [Workflow environment](README.md#workflow-environment) |
| Activity test environment | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Activity environment](README.md#activity-environment) |
| Test environment on an existing server or client | Java, Python, TypeScript, .NET, Ruby | GA | supported | [Workflow environment](README.md#workflow-environment) |
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
| Nexus interceptors | Go, Java, Python, TypeScript, .NET | GA | supported | [Interceptors](README.md#interceptors) |
| Context propagation through headers | all | GA | supported | [Context propagation](README.md#context-propagation), [ctxpropagation](samples/ctxpropagation) |
| Context propagation over Nexus calls | Go, Java | GA | supported | [Context propagation](README.md#context-propagation), [nexus-context-propagation](samples/nexus-context-propagation) |
| OpenTelemetry tracing | Go, Java, Python, TypeScript, .NET, Ruby | GA | supported | [Tracing](README.md#tracing), [opentelemetry](samples/opentelemetry) |

### Nexus

Nexus is GA in Go, Java, and Python, and since August 2026 in TypeScript and .NET.

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Nexus service contracts and synchronous handlers | Go, Java, Python, TypeScript, .NET | GA | supported | [Services and handlers](README.md#services-and-handlers), [nexus](samples/nexus) |
| Workflow-run operations | Go, Java, Python, TypeScript, .NET | GA | supported | [Services and handlers](README.md#services-and-handlers), [nexus](samples/nexus) |
| Calling Nexus operations from workflows | Go, Java, Python, TypeScript, .NET | GA | supported | [Calling operations from workflows](README.md#calling-operations-from-workflows), [nexus](samples/nexus) |
| Nexus operation cancellation types | Go, Java, Python, TypeScript, .NET | GA | supported | [Calling operations from workflows](README.md#calling-operations-from-workflows), [nexus-cancelation](samples/nexus-cancelation) |
| Nexus endpoints and handlers in tests | Go, Java, Python, TypeScript, .NET | GA | supported | [Serving operations](README.md#serving-operations) |
| Nexus serialization context: endpoint, service, and operation in the converter context (Nexus Per Endpoint Encryption, Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | missing | `temporal-sdk-zig-d51.10.9`. `converter.Context` has no Nexus fields, and the context a Nexus caller propagates to the workflows, updates and activities its handler starts is not used. |
| Temporal operation handlers: operations that start workflows or standalone activities and signal workflows through a Temporal client (Pre-release) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.10.10`. `nexus.WorkflowStart` backs an operation with a workflow run, and `nexus.Context.client()` reaches the worker's client. Missing: operations backed by a standalone activity, and signals that reuse the operation's request ID, with links. |
| Standalone Nexus operations from a client (Public Preview, November 2026) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.5.12`. Missing: typed calls to start, describe, and cancel an operation, and their README.md section; today only raw WorkflowService calls through `(try client.currentConnection()).call` reach them. |

### Others

| Feature | SDKs | Stage | Zig | Where |
|---------|------|-------|-----|-------|
| Workflow streams (Public Preview) | Go, Java, Python, TypeScript, .NET | pre-release | partial | `temporal-sdk-zig-d51.13.2`. Missing: the stream library; the signals, updates, and queries it is built on exist. |
| AI agent framework integrations (OpenAI Agents, Vercel AI SDK, and others) | Go, Java, Python, TypeScript, .NET | GA | not applicable | Each wraps an agent framework of its own language; Zig has no such framework to integrate. |

## Features only one SDK offers

These aren't parity requirements. Each entry was checked against the other six official SDKs; a feature a second SDK offers moves to the feature table (the enhanced stack trace query, the remote codec client, workflow-local variables, and event groups did).

| Feature | SDK | Zig |
|---------|-----|-----|
| Sessions: activities pinned to one worker host | Go | Has them: [Sessions](README.md#sessions), [Sessions](README.md#sessions-1) under Workers, [fileprocessing](samples/fileprocessing) |
| Static determinism checker (workflowcheck) | Go | Planned (`temporal-sdk-zig-d51.13.6`); the analyzers for other languages are community projects, not part of an official SDK |

The Zig SDK also has a feature no official SDK offers: a [workflow memory limit](README.md#workflow-memory-limit) per cached run.

Sources for the release stages: [Worker Versioning GA](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new) (March 2026, with Upgrade on Continue-as-New in Public Preview), [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements) (Task Queue Priority and Fairness GA; Workflow Streams, External Storage, and worker heartbeats in Public Preview; Serverless Workers in Pre-release), [Durable Digest, August 2026](https://temporal.io/blog/durable-digest-august-2026) (Nexus GA in TypeScript and .NET, Poller Autoscaling GA, Standalone Nexus Operations in Pre-release, Activity Operations and Serverless Workers in Public Preview), [Durable Digest, September 2026](https://temporal.io/blog/durable-digest-september-2026) (Standalone Activities GA), [resource-based auto-tuning in Public Preview](https://temporal.io/changelog/resource-based-worker-auto-tuning-now-in-public-preview), [Eager Workflow Start in Public Preview](https://temporal.io/blog/durable-digest-october-2025), [Environment Configuration GA](https://temporal.io/changelog/environment-config-ga), [Standalone Nexus Operations](https://temporal.io/blog/standalone-nexus-operations) (Public Preview from November 1, 2026), [AI framework integrations](https://temporal.io/blog/ai-framework-integrations) (GA, October 2026), [Durable Digest: October highlights](https://temporal.io/blog/durable-digest-october-highlights) (Nexus Invoked Standalone Activities in Pre-release), and the SDKs' November 2026 release notes (Nexus Per Endpoint Encryption in Public Preview). Features with no announcement of their own (most of the client, workflow, activity, and testing rows) are GA in every SDK that offers them, per Temporal's [SDK documentation](https://docs.temporal.io/develop); summaries, current details, and the metadata query are marked experimental in the SDKs' APIs.

## Samples

Each Zig sample lives in [samples](samples) and is validated against a dev server, with its tests, by `zig build samples-test`. The table has one row for every top-level directory of [samples-go](https://github.com/temporalio/samples-go) as of 2026-11-19 (commit 79eadc2), except `.github` and `.vscode`, sorted by directory name. **Status** is one of:

- `ported`: the Zig sample linked in **Zig sample** demonstrates the same feature and passes `zig build samples-test`.
- `planned`: no Zig sample exists yet; the bead that adds it is named, or `unfiled` where none is filed.
- `not applicable`: the sample has no Zig equivalent; the reason is recorded under [Samples with no Zig equivalent](#samples-with-no-zig-equivalent).

| samples-go sample | Zig sample | Status |
|-------------------|------------|--------|
| await-signals | [await-signals](samples/await-signals) | ported |
| batch-sliding-window | [batch-sliding-window](samples/batch-sliding-window) | ported |
| branch | [branch](samples/branch) | ported |
| cancellation | [cancellation](samples/cancellation) | ported |
| child-workflow | [child-workflow](samples/child-workflow) | ported |
| child-workflow-continue-as-new | [child-workflow-continue-as-new](samples/child-workflow-continue-as-new) | ported |
| choice-exclusive | [choice-exclusive](samples/choice-exclusive) | ported |
| choice-multi | [choice-multi](samples/choice-multi) | ported |
| codec-server | [codec-server](samples/codec-server) | ported |
| cron | [cron](samples/cron) | ported |
| ctxpropagation | [ctxpropagation](samples/ctxpropagation) | ported |
| datadog | [datadog](samples/datadog) | ported |
| dsl | [dsl](samples/dsl) | ported |
| dynamic | [dynamic](samples/dynamic) | ported |
| dynamic-workflows | [dynamic-workflows](samples/dynamic-workflows) | ported |
| dynamicconfig | | not applicable: see [Samples with no Zig equivalent](#samples-with-no-zig-equivalent) |
| dynamicmtls | [dynamicmtls](samples/dynamicmtls) | ported |
| eager-workflow-start | [eager-workflow-start](samples/eager-workflow-start) | ported |
| early-return | [early-return](samples/early-return) | ported |
| encryption | [encryption](samples/encryption) | ported |
| expense | [expense](samples/expense) | ported |
| external-env-conf | [external-env-conf](samples/external-env-conf) | ported |
| external-storage | | planned: `temporal-sdk-zig-d51.16.75` |
| fileprocessing | [fileprocessing](samples/fileprocessing) | ported |
| gcp | | planned: `temporal-sdk-zig-d51.16.80` |
| googleadk | | not applicable: see [Samples with no Zig equivalent](#samples-with-no-zig-equivalent) |
| goroutine | [io-group](samples/io-group) | ported |
| greetings | [greetings](samples/greetings) | ported |
| greetingslocal | [greetingslocal](samples/greetingslocal) | ported |
| grpc-proxy | | not applicable: see [Samples with no Zig equivalent](#samples-with-no-zig-equivalent) |
| helloworld | [helloworld](samples/helloworld) | ported |
| helloworld-apiKey | [helloworld-apiKey](samples/helloworld-apiKey) | ported |
| helloworldmtls | [helloworldmtls](samples/helloworldmtls) | ported |
| lambda-worker | | planned: `temporal-sdk-zig-d51.16.79` |
| logger-interceptor | [logger-interceptor](samples/logger-interceptor) | ported |
| memo | [memo](samples/memo) | ported |
| metrics | [metrics](samples/metrics) | ported |
| multi-history-replay | [multi-history-replay](samples/multi-history-replay) | ported |
| mutex | [mutex](samples/mutex) | ported |
| nexus | [nexus](samples/nexus) | ported |
| nexus-cancelation | [nexus-cancelation](samples/nexus-cancelation) | ported |
| nexus-context-propagation | [nexus-context-propagation](samples/nexus-context-propagation) | ported |
| nexus-messaging | [nexus-messaging](samples/nexus-messaging) | ported |
| nexus-messaging-temporal-operation | | planned: `temporal-sdk-zig-d51.16.68` |
| nexus-multiple-arguments | [nexus-multiple-arguments](samples/nexus-multiple-arguments) | ported |
| nexus-serialization-context | | planned: `temporal-sdk-zig-d51.16.107` |
| nexus-standalone-activity | | planned: `temporal-sdk-zig-d51.16.70` |
| nexus-standalone-operations | | planned: `temporal-sdk-zig-d51.16.71` |
| opentelemetry | [opentelemetry](samples/opentelemetry) | ported |
| opentelemetry-v2 | | planned: `temporal-sdk-zig-d51.16.77` |
| pickfirst | | not applicable: see [Samples with no Zig equivalent](#samples-with-no-zig-equivalent) |
| polling | [polling](samples/polling) | ported |
| pso | [pso](samples/pso) | ported |
| query | [query](samples/query) | ported |
| recovery | [recovery](samples/recovery) | ported |
| reqrespactivity | [reqrespactivity](samples/reqrespactivity) | ported |
| reqrespquery | [reqrespquery](samples/reqrespquery) | ported |
| reqrespupdate | [reqrespupdate](samples/reqrespupdate) | ported |
| retryactivity | [retryactivity](samples/retryactivity) | ported |
| safe_message_handler | [safe_message_handler](samples/safe_message_handler) | ported |
| saga | [saga](samples/saga) | ported |
| schedule | [schedule](samples/schedule) | ported |
| searchattributes | [searchattributes](samples/searchattributes) | ported |
| serverjwtauth | [serverjwtauth](samples/serverjwtauth) | ported |
| session-failure | [session-failure](samples/session-failure) | ported |
| shoppingcart | [shoppingcart](samples/shoppingcart) | ported |
| sleep-for-days | [sleep-for-days](samples/sleep-for-days) | ported |
| slogadapter | [logadapter](samples/logadapter) | ported |
| snappycompress | [snappycompress](samples/snappycompress) | ported |
| splitmerge-future | [splitmerge-future](samples/splitmerge-future) | ported |
| splitmerge-selector | [splitmerge-select](samples/splitmerge-select) | ported |
| standalone-activity | [standalone-activity](samples/standalone-activity) | ported |
| start-delay | [start-delay](samples/start-delay) | ported |
| synchronous-proxy | [synchronous-proxy](samples/synchronous-proxy) | ported |
| temporal-fixtures | [temporal-fixtures](samples/temporal-fixtures) | ported |
| timer | [timer](samples/timer) | ported |
| typed-searchattributes | | not applicable: see [Samples with no Zig equivalent](#samples-with-no-zig-equivalent) |
| updatabletimer | [updatabletimer](samples/updatabletimer) | ported |
| update | [update](samples/update) | ported |
| worker-specific-task-queues | [worker-specific-task-queues](samples/worker-specific-task-queues) | ported |
| worker-versioning | [worker-versioning](samples/worker-versioning) | ported |
| workflow-security-interceptor | [workflow-security-interceptor](samples/workflow-security-interceptor) | ported |
| workflowstreams | | planned: `temporal-sdk-zig-d51.16.74` |
| zapadapter | [logadapter](samples/logadapter) | ported |

## Samples with no Zig equivalent

| samples-go sample | Reason |
|-------------------|--------|
| dynamicconfig | A server configuration file, not a sample. |
| googleadk | Runs Google's Go ADK agent library; there is no Zig ADK. |
| grpc-proxy | Its proxy must serve gRPC, which needs an HTTP/2 server; Zig's standard library has none and the Rust core provides only the client side of the connection. [codec-server](samples/codec-server) shows payload encoding kept outside the worker. Revisit when a Zig gRPC server exists. |
| pickfirst | Exercises grpc-go's pick_first load-balancing policy; the Rust core owns the gRPC connection. Revisit if the core exposes load-balancing options. |
| typed-searchattributes | Holds only a pointer to searchattributes, whose Zig sample covers typed search attributes. |
