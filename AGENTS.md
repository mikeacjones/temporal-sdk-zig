# Contributor guidance for `temporal-sdk-zig`

This repository is an unofficial Temporal SDK for Zig, built on Temporal's Rust Core SDK and aiming for feature parity with the Go SDK. Use this document as the quick reference for coding-agent work and pull requests. Task tracking with beads is described in `CLAUDE.md`.

## Requirements for coding agents

- Use the Zig version in `build.zig.zon` (`minimum_zig_version`, currently 0.17.1).
- Check standard library APIs against the installed source, not memory. Zig's `std` changes between releases (`std.Io`, the build system, and reflection all changed recently). Find it with `zig env` (`std_dir`).
- Use `zig build` steps for checks and tests:
  - `zig build test` runs the unit tests and builds the Rust core with cargo when needed.
  - `zig build test -Doptimize=ReleaseSafe` is required for changes to the workflow runtime, fibers, or the bridge. Fiber code has failed only under optimization before.
  - `zig build fmt` checks formatting.
  - `zig build samples-test` builds the samples and validates them against a dev server.
  - `scripts/check-core-version.sh` checks that the sdk-core pin, the cargo workspace in `deps/`, and the proto list agree.
- For focused tests, use `-Dtest-filter` (repeatable): `zig build test -Dtest-filter="fibers interleave"`.
- Use `std.testing.allocator` in tests so leaks fail them. Avoid fixed sleeps in tests: wait on the condition itself.
- Keep comments sparse. Comments explain non-obvious reasons, invariants, and ownership, not what the code already says.
- Error messages must be actionable: include the operation, option, input, or state that helps the caller diagnose the problem.
- Don't add abstractions for one-off helpers. Follow the style of the file you are changing.
- Behavior changes need tests. A user-facing feature also needs its samples (see below) or documentation.
- Don't hand-edit generated files. Regenerate them:
  - `src/proto/` and `src/proto.zig`: `zig build gen-proto`
  - `deps/protos.zig`: `scripts/list-protos.sh`
  - `src/bridge_abi.txt`: `scripts/update-abi-snapshot.sh`, after reviewing the header change
- Keep `deps/Cargo.toml`'s `[workspace.*]` tables identical to `deps/sdk-core/Cargo.toml`, and `deps/rust-toolchain.toml` equal to sdk-core's. The check script enforces both.

## Building and testing

```bash
zig build test
zig build test -Doptimize=ReleaseSafe
zig build fmt
zig build samples-test
scripts/check-core-version.sh
```

A clean build of the Rust core takes a minute or two; later builds reuse `.zig-cache/cargo-target`. Pass `-Dcore-lib=<path>` to link a prebuilt core library instead, and `-Dprotoc=<path>` to override the pinned `protoc` that cargo uses.

Moving to a new sdk-core release follows [docs/core-bump.md](docs/core-bump.md).

## Samples are the parity bar

Every sample in [temporalio/samples-go](https://github.com/temporalio/samples-go) gets a Zig port under `samples/<same-name>/` (or a Zig-named directory, see below), validated against a dev server by `zig build samples-test`. Samples land from the simplest to the most complex as their SDK features land.

- Keep the Go sample's workflow, activity, signal, query, and update names and payload shapes, so a Go starter can drive the Zig worker and the other way round.
- Port the Go sample's `*_test.go` tests where it has them.
- A finished sample stays in the suite. A change that breaks a passing sample is a regression, even if the unit tests pass.
- A sample named after a Go-only concept is renamed for its Zig equivalent (samples-go `goroutine` is `io-group`). The renamed sample keeps the Go sample's workflow, activity, and task queue names, and `samples/README.md` maps it to the Go sample.
- A sample with no Zig equivalent is listed in `samples/README.md` with the reason.

## Zig SDK review and implementation focus

- Workflow code must stay deterministic. Anything reachable from workflow execution must not use threads, wall-clock time, randomness, unordered hash map iteration, or I/O outside the workflow `std.Io`.
- For workflow runtime, command, patch, update, local activity, or Nexus workflow changes, add replay coverage with the workflow replayer to protect existing histories.
- The Go SDK defines the feature set; the core-based SDKs define the behavior. Behavior that crosses the core boundary follows sdk-core's rules (activation job ordering, patch semantics, local activity backoff): compare against the .NET SDK, which drives the same C bridge, and the Python and TypeScript SDKs. Where Go relies on machinery the core lacks (SideEffect, GetVersion ranges, its mocked test environment), do what the core-based SDKs do and document the difference from Go.
- Prefer idiomatic Zig over literal ports: explicit allocators and `std.Io`, struct-literal options, comptime-typed definitions, error sets with failure details read from the future or handle.
- Document ownership, lifetime, and thread-safety at every interface boundary. Every `TemporalCoreByteArray` the core returns must be freed with `temporal_core_byte_array_free`. Core callbacks run on the core's threads, not the caller's.
- When the C header changes, the ABI snapshot test fails. Update every wrapper that builds or reads a changed struct before refreshing the snapshot.
- Zig panics can't be caught. SDK code returns errors instead of panicking on bad input from users, the server, or the core.
- Workflow fibers support aarch64 and System V x86_64 only. Windows x64 is a compile error until its register set is implemented.

## Pull request expectations

- Keep changes focused and include tests for behavior changes.
- Run `zig build fmt`, `zig build test`, and `zig build test -Doptimize=ReleaseSafe`.
- Run `zig build samples-test` when the change touches the client, worker, workflow runtime, activities, Nexus, payload conversion, or interceptors.
- Commit messages: a short imperative subject, and a body that explains why.

## Where things are

- `src/temporal.zig` is the root module.
- `src/client.zig`, `src/worker.zig`, `src/workflow.zig`, `src/activity.zig`, `src/nexus.zig`, `src/converter.zig`, and `src/testing.zig` are the public namespaces, mirroring the Go SDK packages.
- `src/workflow/Scheduler.zig` is the deterministic fiber scheduler that workflow code runs on.
- `src/bridge.zig` wraps the sdk-core C API; `src/bridge_abi.txt` is its struct layout snapshot.
- `src/proto/` and `src/proto.zig` are the generated protobuf types (`temporal.proto`).
- `deps/sdk-core/` is the pinned sdk-core submodule. `deps/Cargo.toml` and `deps/Cargo.lock` build its C bridge.
- `samples/` holds the samples-go ports, and `tools/samples_runner.zig` validates them.
- `scripts/` holds maintenance scripts, each with usage in its header.
- `docs/` holds maintainer documentation.
- `.github/workflows/` runs CI (`ci.yml`) and the samples suite (`samples.yml`).

## Notes

- `zig fetch` doesn't fetch git submodules, so a project that depends on this SDK by git URL can't build the Rust core from source yet. Use a recursive clone and a path dependency until prebuilt core libraries ship.
- The generated protobuf code uses zig-protobuf with unknown-field preservation on. Encode ProtoJSON with `.emit_oneof_field_name = false` to get standard ProtoJSON.
