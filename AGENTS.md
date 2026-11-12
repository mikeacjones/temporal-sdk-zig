# Contributor guidance for `temporal-sdk-zig`

This repository is an unofficial Temporal SDK for Zig, built on Temporal's Rust Core SDK. It is a standalone, Zig-first SDK: it aims for feature parity with Temporal's other SDKs, with every feature expressed the Zig way rather than copied from another language. README.md explains how parity is measured; nothing else in the repository refers to other SDKs' APIs or samples. Use this document as the quick reference for coding-agent work and pull requests. Task tracking with beads is described in `CLAUDE.md`.

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

Each feature is demonstrated by a sample under `samples/`, validated against a dev server by `zig build samples-test`. README.md lists the parity samples; they land from the simplest to the most complex as their SDK features land.

- A sample is a directory `samples/<name>/` with `worker.zig`, `starter.zig`, and `sample.zon`, which lists the starter output to expect, the sample's test files, and other samples it imports. build.zig finds samples by their `sample.zon`.
- Samples are idiomatic Zig: Zig names for workflows, activities, and payload fields (snake_case), Zig types for durations and times, and the Zig equivalent of each concept (for example `std.Io.Group` for concurrent workflow coroutines).
- Each sample has tests covering its behavior, run in `samples-test`.
- Comments and docs describe what the code does in Zig terms, without referring to other SDKs.
- A finished sample stays in the suite. A change that breaks a passing sample is a regression, even if the unit tests pass.
- A parity sample with no Zig equivalent is listed in README.md with the reason.

## Zig SDK review and implementation focus

- Workflow code must stay deterministic. Anything reachable from workflow execution must not use threads, wall-clock time, randomness, unordered hash map iteration, or I/O outside the workflow `std.Io`.
- For workflow runtime, command, patch, update, local activity, or Nexus workflow changes, add replay coverage with the workflow replayer to protect existing histories.
- Behavior that crosses the core boundary follows sdk-core's rules (activation job ordering, patch semantics, local activity backoff). When in doubt, compare against the .NET SDK, which drives the same C bridge, and the Python and TypeScript SDKs.
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
- `src/client.zig`, `src/worker.zig`, `src/workflow.zig`, `src/activity.zig`, `src/nexus.zig`, `src/converter.zig`, and `src/testing.zig` are the public namespaces.
- `src/workflow/Scheduler.zig` is the deterministic fiber scheduler that workflow code runs on.
- `src/bridge.zig` wraps the sdk-core C API; `src/bridge_abi.txt` is its struct layout snapshot.
- `src/proto/` and `src/proto.zig` are the generated protobuf types (`temporal.proto`).
- `deps/sdk-core/` is the pinned sdk-core submodule. `deps/Cargo.toml` and `deps/Cargo.lock` build its C bridge.
- `samples/` holds the samples, and `tools/samples_runner.zig` validates them.
- `scripts/` holds maintenance scripts, each with usage in its header.
- `docs/` holds maintainer documentation.
- `.github/workflows/` runs CI (`ci.yml`) and the samples suite (`samples.yml`).

## Notes

- `zig fetch` doesn't fetch git submodules, so a project that depends on this SDK by git URL can't build the Rust core from source yet. Use a recursive clone and a path dependency until prebuilt core libraries ship.
- The generated protobuf code uses zig-protobuf with unknown-field preservation on. Encode ProtoJSON with `.emit_oneof_field_name = false` to get standard ProtoJSON.
