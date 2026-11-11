# Temporal Zig SDK (unofficial) [![CI](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/ci.yml) [![Samples](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml/badge.svg?event=push)](https://github.com/mikeacjones/temporal-sdk-zig/actions/workflows/samples.yml)

> [!WARNING]
> This is an unofficial, community-built SDK. Temporal Technologies does not maintain, support, or endorse it. It is in early development, its APIs will change without notice, and it is not ready for production use. For supported SDKs, see [Temporal's SDK documentation](https://docs.temporal.io/develop).

[Temporal](https://github.com/temporalio/temporal) is a distributed, scalable, durable, and highly available orchestration engine used to execute asynchronous long-running business logic in a scalable and resilient way.

The Temporal Zig SDK is a framework for authoring workflows and activities in Zig. It is built on Temporal's [Rust Core SDK](https://github.com/temporalio/sdk-core), the same core the TypeScript, Python, .NET, and Ruby SDKs use, and aims for feature parity with the [Go SDK](https://github.com/temporalio/sdk-go).

## Status

Workflows can run activities (sequentially, in parallel, or chosen at run time), wait on timers, race activities against timers, run concurrent coroutines on the workflow's deterministic `std.Io`, and await conditions. Activities can share state, heartbeat, and resume from their last heartbeat on retry. Clients can start workflows with a start delay or a cron schedule, wait for results, and terminate workflows. Twelve samples-go ports pass against a dev server with their ported tests: helloworld, greetings, branch, splitmerge-future, splitmerge-selector, choice-exclusive, choice-multi, goroutine, timer, start-delay, retryactivity, and cron. Every test run through the test environment also replays the workflow's own history to check determinism.

Signals, queries, updates, child workflows, local activities, Nexus, and most client and worker options are not implemented yet.

Parity is measured by porting every sample in [samples-go](https://github.com/temporalio/samples-go) to Zig and running it against a Temporal dev server. The Samples badge shows whether the ported samples pass.

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

See [samples](samples) for Zig ports of the Go samples as they land, and [Temporal's documentation](https://docs.temporal.io) for the concepts.

The API follows the Go SDK's feature set in Zig's style:

- Functions that allocate or do I/O take an explicit `std.mem.Allocator` and `std.Io`.
- Options are struct literals with defaults, not functional options or context derivation.
- Workflows, signals, queries, updates, and activities have comptime-typed definitions shared by the client and the worker, so calls check argument and result types at compile time.
- Workflow code runs on a deterministic `std.Io`: `io.sleep` is a durable timer, `io.now` is workflow time, and `std.Io` futures, `Select`, `Mutex`, and `Queue` work inside workflows unchanged.

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
