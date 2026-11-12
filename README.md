# Temporal Zig SDK (unofficial) [![CI](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml) [![Samples](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml)

> [!WARNING]
> This is an unofficial, community-built SDK. Temporal Technologies does not maintain, support, or endorse it. It is in early development, its APIs will change without notice, and it is not ready for production use. For supported SDKs, see [Temporal's SDK documentation](https://docs.temporal.io/develop).

[Temporal](https://github.com/temporalio/temporal) is a distributed, scalable, durable, and highly available orchestration engine used to execute asynchronous long-running business logic in a scalable and resilient way.

The Temporal Zig SDK is a framework for authoring workflows and activities in Zig. It is built on Temporal's [Rust Core SDK](https://github.com/temporalio/sdk-core), the same core the TypeScript, Python, .NET, and Ruby SDKs use, It is a standalone, Zig-first SDK: it aims for feature parity with Temporal's other SDKs, with every feature designed the Zig way.

## Status

Workflows can run activities and local activities (sequentially, in parallel, or chosen at run time), start child workflows, wait on timers, race activities against timers, run concurrent coroutines on the workflow's deterministic `std.Io`, await conditions, answer typed queries, handle signals and updates (with validators) in their own coroutines, wait for all handlers to finish, continue as new, and read and upsert their memo. Cancellation reaches workflows, activities, and children as `std.Io` cancellation, honoring each activity's or child's cancellation type. Activities can share state, heartbeat, observe cancellation, and resume from their last heartbeat on retry. Clients can start workflows with a start delay, a cron schedule, or a memo, wait for results (following continue-as-new), signal, signal-with-start, update (waiting for acceptance or completion), update-with-start, query, describe, cancel, and terminate workflows. Tests run on a dev server or the time-skipping test server, and can signal, query, and update the workflows they start. Thirty-two [samples](samples) pass against a dev server with their tests (see [Parity](#parity)). Every test run through the test environment also replays the workflow's own history to check determinism.

Signaling other workflows from a workflow, Nexus, search attributes, schedules, and most client and worker options are not implemented yet.

## Add the SDK to your project

You need Zig 0.17.1 and a Rust toolchain installed through [rustup](https://rustup.rs). The build compiles the Rust core with cargo and installs the pinned Rust version on first use. You don't need `protoc`.

The Rust core is a git submodule, and `zig fetch` doesn't fetch submodules, so clone the SDK next to your project:

```bash
git clone --recursive https://github.com/mikeacjones/temporal-sdk-zig
```

Then add it to your project's `build.zig.zon`:

```zig
.dependencies = .{
    .temporal = .{ .path = "../temporal-sdk-zig" },
},
```

and import the module in `build.zig`:

```zig
const temporal = b.dependency("temporal", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("temporal", temporal.module("temporal"));
```

Fetching by URL with prebuilt core libraries, so you don't need Rust, is planned.

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

A clean build of the Rust core takes a minute or two. Later builds reuse it from `.zig-cache/cargo-target`. To skip cargo, pass a prebuilt core library with `-Dcore-lib=<path>`.

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
