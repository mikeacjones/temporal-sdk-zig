# Temporal Zig SDK (unofficial) [![CI](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml) [![Samples](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml)

> [!WARNING]
> This is an unofficial, community-built SDK. Temporal Technologies does not maintain, support, or endorse it. It is in early development, its APIs will change without notice, and it is not ready for production use. For supported SDKs, see [Temporal's SDK documentation](https://docs.temporal.io/develop).

[Temporal](https://github.com/temporalio/temporal) is a distributed, scalable, durable, and highly available orchestration engine used to execute asynchronous long-running business logic in a scalable and resilient way.

The Temporal Zig SDK is a framework for authoring workflows and activities in Zig. It is built on Temporal's [Rust Core SDK](https://github.com/temporalio/sdk-core), the same core the TypeScript, Python, .NET, and Ruby SDKs use, It is a standalone, Zig-first SDK: it aims for feature parity with Temporal's other SDKs, with every feature designed the Zig way.

## Status

Workflows can run activities and local activities (sequentially, in parallel, or chosen at run time), start child workflows, wait on timers, race activities against timers, run concurrent coroutines on the workflow's deterministic `std.Io`, await conditions, record side effects, answer typed queries, handle signals and updates (with validators) in their own coroutines, including signals named at run time and a catch-all signal handler, signal and cancel other workflows, read their parent and root workflow, wait for all handlers to finish, continue as new, and read and upsert their memo and typed search attributes. Workers can run dynamic (catch-all) workflows and activities, several workers can share a process and stop on one signal, and callers can wait until a worker is running. Cancellation reaches workflows, activities, and children as `std.Io` cancellation, honoring each activity's or child's cancellation type. Activities can share state, heartbeat, observe cancellation, resume from their last heartbeat on retry, complete asynchronously, fail with a benign category or a custom next retry delay, and use the worker's client. Clients can start workflows by definition or by type name, with a start delay, a cron schedule, a memo, typed search attributes, or eager start, wait for results (following continue-as-new), signal, signal-with-start, update (waiting for acceptance or completion), update-with-start, query, describe, cancel, and terminate workflows; list and count workflows with visibility queries; read a run's history page by page; complete, fail, and heartbeat activities by task token or ID; create, describe, update, trigger, backfill, pause, list, and delete schedules; and run, list, and count standalone activities. Tests run on a dev server or the time-skipping test server, and can signal, query, and update the workflows they start; the replayer replays recorded histories, batches of them, or runs fetched through a client. Fifty-four [samples](samples) pass against a dev server with their tests (see [Parity](#parity)). Every test run through the test environment also replays the workflow's own history to check determinism.

Not implemented yet: custom data converters and payload codecs, interceptors and context propagation, metrics and logger adapters, TLS and API-key connections and environment config profiles, worker versioning and patching, and Nexus.

## Install

You need Zig 0.17.1. Add a release to your project's `build.zig.zon`:

```bash
zig fetch --save=temporal https://github.com/mikeacjones/temporal-sdk-zig/releases/download/v<version>/temporal-sdk-zig-<version>.tar.gz
```

and import the module in `build.zig`:

```zig
const temporal = b.dependency("temporal", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("temporal", temporal.module("temporal"));
```

A release needs no Rust toolchain: its build links a prebuilt core library for your target, which Zig fetches on first use. Prebuilt libraries exist for `aarch64-macos`, `x86_64-macos`, `aarch64-linux-gnu`, `x86_64-linux-gnu`, `aarch64-linux-musl`, and `x86_64-linux-musl`; for any other target, the build stops and says so. [docs/releasing.md](docs/releasing.md) has the support policy, including the glibc the gnu libraries need. No release has been published yet; the [releases page](https://github.com/mikeacjones/temporal-sdk-zig/releases) lists them once there are.

To build from source instead, for another target or for unreleased changes, clone the SDK with its submodule next to your project and depend on it by path. The build then compiles the Rust core with cargo, so it needs a Rust toolchain installed through [rustup](https://rustup.rs), which installs the pinned Rust version on first use. You don't need `protoc`.

```bash
git clone --recursive https://github.com/mikeacjones/temporal-sdk-zig
```

```zig
.dependencies = .{
    .temporal = .{ .path = "../temporal-sdk-zig" },
},
```

## Versioning

Releases follow [SemVer 2.0](https://semver.org/spec/v2.0.0.html) and are tagged `v<version>`. Until the SDK reaches parity, releases are 0.x pre-releases starting at `0.1.0-alpha.1`, and a minor version may break the API. Each release supports one Zig version and one sdk-core release (`CORE_VERSION`). [CHANGELOG.md](CHANGELOG.md) lists the changes in each release, and [docs/releasing.md](docs/releasing.md) describes the policy and how releases are made.

## Getting started

See [samples](samples) for working examples, and [Temporal's documentation](https://docs.temporal.io) for the concepts.

The API is designed for Zig:

- Functions that allocate or do I/O take an explicit `std.mem.Allocator` and `std.Io`.
- Options are struct literals with defaults, not functional options or context derivation.
- Workflows, signals, queries, updates, and activities have comptime-typed definitions shared by the client and the worker, so calls check argument and result types at compile time.
- Workflow code runs on a deterministic `std.Io`: `io.sleep` is a durable timer, `io.now` is workflow time, and `std.Io` futures, `Select`, `Mutex`, and `Queue` work inside workflows unchanged.

## Parity

This SDK is not a port of another SDK, but it measures its feature coverage against the [Go SDK](https://github.com/temporalio/sdk-go), Temporal's most complete feature set, and its [samples](https://github.com/temporalio/samples-go). Every samples-go sample gets a Zig sample that demonstrates the same feature in idiomatic Zig, with its own names, payload shapes, and tests. `zig build samples-test` runs every sample against a dev server, and the Samples badge shows whether they pass.

Where the Go SDK relies on machinery Temporal's Rust core doesn't have, this SDK behaves like the other core-based SDKs (.NET, Python, TypeScript, Ruby): a side effect is a local activity, version checks use patches, and tests run against the dev server and the time-skipping test server instead of an in-memory mock.

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

## Development

### Prerequisites

- [Zig](https://ziglang.org/download/) 0.17.1
- [rustup](https://rustup.rs). The Rust version is pinned in [deps/rust-toolchain.toml](deps/rust-toolchain.toml) and installed automatically.
- git, with submodules: `git submodule update --init --depth 1`

### Building and testing

```bash
zig build test           # unit tests; builds the Rust core with cargo on first run
zig build fmt            # check formatting
zig build samples-test   # build the samples and validate them against a dev server
```

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

- `src/` contains the SDK: `client`, `worker`, `workflow`, `activity`, `nexus`, `converter`, and `testing` mirror the Go SDK packages. `bridge.zig` wraps the sdk-core C API, and `proto/` holds the generated protobuf types.
- `deps/` holds the sdk-core submodule and the cargo workspace that builds its C bridge.
- `samples/` contains Zig ports of samples-go.
- `tools/` contains build tools such as the samples runner.
- `scripts/` contains maintenance scripts.

## Contributing

See [AGENTS.md](AGENTS.md) for how the project is built, tested, and reviewed. Work is tracked with [beads](https://github.com/gastownhall/beads) (`bd ready` lists open work).

## License

MIT. See [LICENSE](LICENSE). The vendored [sdk-core](https://github.com/temporalio/sdk-core) is also MIT licensed.
