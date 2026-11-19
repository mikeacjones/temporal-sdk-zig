# Contributor guidance for `temporal-sdk-zig`

This repository is an unofficial Temporal SDK for Zig, built on Temporal's Rust Core SDK. It is a standalone, Zig-first SDK: it aims for feature parity with Temporal's other SDKs, with every feature expressed the Zig way rather than copied from another language. PARITY.md explains how parity is measured, maps every feature that more than one of Temporal's official SDKs offers to this SDK's status (supported, partial, missing, or not applicable) with its release stage, and lists the samples; it is the only file that names other SDKs. Use this document as the quick reference for coding-agent work and pull requests. Task tracking with beads is described in `CLAUDE.md`.

## Requirements for coding agents

- Use the Zig version in `build.zig.zon` (`minimum_zig_version`, currently 0.17.1).
- Check standard library APIs against the installed source, not memory. Zig's `std` changes between releases (`std.Io`, the build system, and reflection all changed recently). Find it with `zig env` (`std_dir`).
- Use `zig build` steps for checks and tests:
  - `zig build test` runs the unit tests and builds the Rust core with cargo when needed.
  - `zig build test -Doptimize=ReleaseSafe` is required for changes to the workflow runtime, fibers, or the bridge. Fiber code has failed only under optimization before.
  - `zig build fmt` checks formatting.
  - `zig build samples-test` builds the samples and validates them against a dev server.
  - `zig build readme-check` checks that README.md's code blocks equal their regions in `examples/readme/` (`zig build test` runs that comparison too) and runs `scripts/parity-check-test.sh` and `scripts/parity-check.sh`, and `zig build readme` rewrites them.
  - `scripts/check-core-version.sh` checks that the sdk-core pin, the cargo workspace in `deps/`, and the proto list agree.
  - `.review/reports.sh [--base <ref>] [--quick] [--all]` writes the review reports for a change into `.review/out/`: the diff, the checks above, and the scanners' output. Reviewers read `INDEX.txt` there before the code. `.review/install.sh` installs the scanners.
  - `scripts/parity-check.sh` checks PARITY.md's feature table: known statuses and stages, README.md anchors and sample directories that exist for supported rows, a bead ID or `unfiled` for partial and missing rows, and the `GA, deferred` stage (a GA feature the owner deferred past the first release) only on a partial or missing row with a bead ID. README.md anchors come from `scripts/github-anchors.awk`, which reads only plain top-level `#` headings and fails the check on any README.md construct that could create or hide a heading in a way it doesn't model (keep README.md's headings, fences, and comments plain and at column 0). It prints the GA features still partial or missing that the first release needs, and the deferred ones under their own heading. `zig build readme-check` runs it and its tests (`scripts/parity-check-test.sh`), and CI's check job runs `zig build readme-check`; run it locally after changing PARITY.md or README.md's headings (or run `sh scripts/parity-check-test.sh` and `sh scripts/parity-check.sh` directly).
  - `scripts/changelog-check.sh [<base>]` checks the changelog fragments, and the commits since `<base>` for a fragment or trailer; `scripts/changelog-test.sh` tests the changelog scripts.
  - See [Correctness commands](#correctness-commands) for the checks and reports available in a checkout.
- For focused tests, use `-Dtest-filter` (repeatable): `zig build test -Dtest-filter="fibers interleave"`.
- Use `std.testing.allocator` in tests so leaks fail them. `zig build test` also runs the `zig build test-allocators` scan of the tree, so a test that allocates through another allocator fails it. Avoid fixed sleeps in tests: wait on the condition itself.
- Keep comments sparse. Comments explain non-obvious reasons, invariants, and ownership, not what the code already says.
- Error messages must be actionable: include the operation, option, input, or state that helps the caller diagnose the problem.
- Don't add abstractions for one-off helpers. Follow the style of the file you are changing.
- Behavior changes need tests. A user-facing feature also needs its README.md section and its samples (see below).
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
zig build readme-check
scripts/check-core-version.sh
scripts/changelog-test.sh
scripts/changelog-check.sh origin/main
```

A clean build of the Rust core takes a minute or two; later builds reuse `.zig-cache/cargo-target`. Pass `-Dcore-lib=<path>` to link a prebuilt core library instead (the header still comes from the submodule), `-Dcore-bundle=<dir>` to link an extracted core bundle from `scripts/package-core-bundle.sh` (library and header), and `-Dprotoc=<path>` to override the pinned `protoc` that cargo uses. Without the submodule and either option, build.zig links the bundle that build.zig.zon pins as `core-<CORE_VERSION>-<target>`, and fails naming the target if there is none. `.github/workflows/core-bundles.yml` builds and publishes the bundles.

Moving to a new sdk-core release follows [docs/core-bump.md](docs/core-bump.md), and releases follow [docs/releasing.md](docs/releasing.md).

## Correctness commands

`addCheckSteps` in build.zig adds these commands in a checkout. Programs, fixtures, and allow-lists live under `tools/checks/`. Exit code 0 means the tool ran successfully, 1 means a check found a problem or the tool could not run, and 2 means the arguments were invalid. Run times are measurements, not limits.

### `zig build alloc-failures`

Calls SDK functions that take an allocator with each of their allocations failing in turn: fail index 0, 1, 2, and so on, until a call ends with no induced failure. Every call that saw a failure must return `error.OutOfMemory`. A success, another error, or a leak fails the case, as does a case that sees fewer than 2 induced failures or reaches the cap of 20,000. Each problem prints one line naming the case and the fail index. A function that allocates into a caller's arena gets a `std.testing.FailingAllocator` between it and a fresh scratch arena, so every allocation fails once; a function that frees what it allocates runs through `std.testing.checkAllAllocationFailures`. The cases are in `src/alloc_failure.zig`, its top comment lists the functions covered, and the helper is `tools/checks/alloc_failures.zig`. A case is named by the function's path from `src` with dots, such as `converter.Header.fromEntries`. `-Dtest-filter=<text>` runs every case whose name contains the text: `-Dtest-filter=converter.toPayload` also runs `converter.toPayloads`, while `-Dtest-filter=converter.Header.fromEntries` runs exactly one.

`zig build test` compiles the cases, in its own optimize mode, but does not run them, so a case that stops compiling fails `zig build test`. It runs only the fixtures under `tools/checks/fixtures/alloc_failures/`, which check the helper itself.

To add a case, follow the top comment of `src/alloc_failure.zig`: a `test "<case>"` that builds its inputs with `std.testing.allocator` and calls `alloc_failures.run("<case>", .arena, <wrapper>, .{inputs...})`, where the wrapper takes the allocator first and fixes the comptime arguments. A finding that is not fixed with its case goes in `tools/checks/alloc_failures.zon` with its `case`, `bead`, and `reason`; a listed case that still fails passes quietly. An entry whose case no longer fails, or whose case no test names, fails the command.

- When to run: When changing a function a case calls, or adding a case.
- Run time: about 7.2 s for the whole command with all 32 cases (41 tests as the runner counts them) on the factory's Apple-silicon Mac, measured 2026-11-19: about 4 s compiling the test root and 2 s running the cases (first run about 64 s, most of it cargo building the Rust core).

### `zig build api`

Lists the public names reachable from `@import("temporal")` in byte order, with aliases under each path. The command takes no arguments. It lists names only, so signature changes do not show up. It includes the members of public error sets and omits the errors a function can return. It lists `temporal.proto` without expanding it. The list is for the machine running the build, regardless of `-Dtarget`. A public declaration that evaluates to `@compileError` prevents the lister from compiling.

- When to run: When reviewing a change to public declarations or re-exports.
- Run time: about 0.2 s on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 2.0 s).

### `zig build api-diff`

Compares the public names from `-Dbase=<ref>` with the working tree, including uncommitted changes. Pass `-Dhead=<ref>` to compare with a second commit. It prints `+ name` for additions and `- name` for removals, sorted by name, and exits successfully even when the lists differ. It requires a local Git repository. The same limits as `zig build api` apply, including names only and reflection for the host regardless of `-Dtarget`. Committed sources are compiled with the checkout's dependencies and core header, so a ref across an sdk-core bump may fail to compile.

- When to run: When reviewing which public names a branch adds or removes.
- Run time: about 0.3 s on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 6.7 s).

### `zig build dupes`

Reports repeated runs in hand-written Zig under `src`, `tools`, `examples`, `samples`, and `conformance` as `file:start-end` line ranges, longest first. It compares both token kind and exact text, including identifiers and literals, and ignores comments and whitespace between tokens. It leaves out `src/proto/`, `src/proto.zig`, `samples/dispatch.zig`, and `tools/checks/fixtures/`. Directory walks also skip hidden directories, `zig-out`, and `zig-pkg`. The default minimum is 100 tokens. Set it with `-Ddupes-min-tokens=<n>`, or pass relative file and directory paths after `--` to limit the scan. Finding duplicates does not fail the command.

- When to run: When reviewing new code for copies of existing code.
- Run time: about 0.8 s on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 5.3 s).

### `zig build fuzz`

Runs the `failure_attributes`, `history_json`, `nexus_links`, `payload`, `protojson`, `search_attributes`, and `time_parse` targets with Zig's fuzzer. A panic or failed safety check, a leak, an error outside the declared set, or a failed round trip counts as a crash. The default box is 30 seconds per target. Set `-Dfuzz-seconds=<n>` to change it. Each target runs in two nested builds. The first builds the target and replays the corpus, and the second fuzzes until its deadline. A second run in the same checkout waits on `.zig-cache/fuzz.lock`. Interrupting the command or killing its wrapper stops the nested build and fuzzer. Zig keeps the inputs it learns under `.zig-cache/f/`.

Crash inputs are saved under `zig-out/fuzz/<target>/crash-<8 hex digits>`. Copy the saved input into `src/fuzz/<target>/` and run `zig build test` to replay it. `tools/checks/fuzz.zon` lists the open bugs with their targets and the reason for each guard. `zig build test` fails for an entry without a guard.

- When to run: When changing code a target calls, or adding a target.
- Run time: about 4.1 min for the 7 targets at the default 30 s each (7 × 30 s of fuzzing plus builds and start-up) on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 5.1 min, with an empty local cache).

### `zig build lint`

Runs zlinter over hand-written Zig under `src`, `tools`, `examples`, `samples`, and `conformance`, excluding generated code and check fixtures. The rules are the `addRule` calls in the `lint` block of `addCheckSteps` in `build.zig`. Findings fail the step. Initialize `deps/sdk-core` with `git submodule update --init`, even if you pass `-Dcore-bundle` or `-Dcore-lib`. zlinter configures the build a second time with `zig build --print-configuration-path` and omits the outer command's `-D` options. Without the submodule or a default bundle pin for the host, that configuration stops with `no prebuilt core`. [docs/zlinter.md](docs/zlinter.md) records the rule evaluation.

- When to run: When changing hand-written Zig code or the lint rules.
- Run time: about 2.4 s on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 24 s with packages already fetched).

### `zig build test-allocators`

Scans test bodies in hand-written Zig under `src`, `tools`, `examples`, `samples`, and `conformance` for allocator names such as `page_allocator` and `FixedBufferAllocator`. Helpers defined outside test blocks are outside the scan's scope. The ZON allow-list at `tools/checks/test_allocators.zon` has one entry per exempt test with a root-relative `file`, a `test_name` (the test's string or identifier, empty for an unnamed test), and a `reason`. Entries with no matching use fail the check. `zig build test` runs this scan too, as its `check test-allocators tree` step.

- When to run: When adding or changing tests or reviewing an allow-list entry; `zig build test` runs it every time.
- Run time: about 0.4 s on the factory's Apple-silicon Mac, measured 2026-11-18 (first run about 5.8 s).

## Conformance

`addConformanceSteps` in build.zig adds three commands that run Temporal's cross-SDK features suite against this SDK. The suite is `temporalio/features` at the commit build.zig.zon pins as the lazy dependency `temporal_features`. Each command runs a nested `zig build <step> -Dconformance-suite=true`, and only the nested build fetches the suite. The first run needs network, for that fetch and for the dev server's download. The suite's own Go, TypeScript, and other harnesses are not used: the Zig versions of its features live in `conformance/features/`, and no Go toolchain is needed. The harness is `conformance/harness/` and the runner `conformance/runner/`. Run times are measurements, not limits: they were measured on 2026-11-18 on the factory's Mac (Apple M4 Pro, 14 cores, 64 GB, macOS 27.2.1), each command in its own fresh `git clone --recursive` with no `.zig-cache`, while other builds ran on the machine. The first runs include building the Rust core with cargo (about 70 s). Machine-wide caches were already warm: Zig's global package cache held the suite and the other dependencies, so nothing was downloaded; the dev server's Temporal CLI binary was already in `$TMPDIR`; and cargo's registry and the Rust toolchain were installed.

### `zig build conformance`

Starts a dev server on a free local port it picks itself (sdk-core downloads the Temporal CLI on the first run and caches it in the temporary directory), runs each written feature in a child process with a 240 s limit, and prints a table of every suite feature with the counts on stdout. A feature whose suite `config.json` lists run variants gets a dev server of its own for each variant. Logs go to standard error. `-Dfeature=<pattern>` (repeatable, `*` matches any run of characters) runs only the matching features, as in `zig build conformance -Dfeature='signal/*'`; a pattern that matches no suite feature fails the run.

The report is written to `zig-out/conformance/report.json` (under `--prefix` when one is given), and the previous one is removed first, also when the run fails. It is JSON with the fields `suite_commit`, `sdk_version`, `filter` (the `-Dfeature` patterns), `readme_only_directories` (the number of suite directories that hold only a README.md, which are not features), and `features`: one entry per suite feature (84), sorted by path in byte order, each with `path`, `status`, and, for `fail` and `not_applicable`, `message`. The statuses:

- `pass`: the Zig feature ran and passed.
- `fail`: the Zig feature failed, timed out, or could not run; `message` says why.
- `unwritten`: the suite has the feature and `conformance/features/` doesn't.
- `not_applicable`: `conformance/not_applicable.zon` lists it; `message` is the reason.
- `not_run`: written but excluded by `-Dfeature`. It appears only with `-Dfeature`.

The table on stdout ends with the count of each status and the report's path.

- When to run: After writing or changing a feature, or a change to the SDK that a written feature covers.
- Run time: about 42 s on a repeat run, almost all of it running the features (first run about 103 s).

### `zig build conformance-check`

Runs `zig build conformance-map`'s check, then the whole suite as `zig build conformance` does (rewriting the same report), then compares the report with `conformance/parity_map.zon` and PARITY.md's feature table. The suite's table and progress go to standard error, so stdout holds only the finding lines and the counts line, and `zig build conformance-check > findings.tsv` captures them. Each line's fields are separated by one tab:

```
failing<TAB><row><TAB><feature><TAB><status>
uncovered<TAB><row>
unknown-row<TAB><row text><TAB><feature>
counts<TAB>failing=<n><TAB>uncovered=<n><TAB>unknown-row=<n>
```

- `failing`: a feature mapped to a PARITY.md row whose status is `supported` did not pass; `<status>` is its report status, such as `fail` or `unwritten`.
- `uncovered`: a `supported` row that no feature maps to. These lines are information and don't fail the command.
- `unknown-row`: a parity_map.zon entry names a row text PARITY.md's feature table lacks.
- `counts`: always the last line; `failing` counts lines, not rows.

Supported rows come first in table order, then the `unknown-row` lines by feature path. Rows that are partial, missing, or not applicable print nothing. The command exits non-zero when there is a `failing` or `unknown-row` line, and also, with nothing on stdout, when the map check fails, when the suite run fails, or when the report or PARITY.md's feature table cannot be read. A feature that fails doesn't fail the suite run: it reaches the comparison as a `fail` status. It takes no `-Dfeature`, and fails at once if given one. On main it exits non-zero while supported rows map to unwritten features, which is expected: each `failing ... unwritten` line is a feature still to write.

- When to run: When changing PARITY.md's statuses, parity_map.zon, or before claiming a row as supported.
- Run time: about 37 s on a repeat run (first run about 114 s).

### `zig build conformance-map`

Checks `conformance/parity_map.zon` against the suite's features and `conformance/not_applicable.zon`, without starting a server or writing a report. It reports a duplicate entry, an entry for a feature the suite lacks, a not-applicable feature mapped to a row, a `.none` with an empty reason, and a suite feature with no entry, and on success prints one line to standard error: `./conformance/parity_map.zon: 84 entries, 77 mapped to 31 PARITY.md rows, 7 none`. It doesn't read PARITY.md, and `-Dfeature` has no effect.

- When to run: After editing parity_map.zon or moving to a new suite commit.
- Run time: about 10 s on a repeat run (first run about 82 s).

### The parity map and PARITY.md

`conformance/parity_map.zon` has one entry per suite feature, sorted by path in byte order: `.{ .feature = "signal/basic", .parity = .{ .row = "<the Feature cell of a PARITY.md row>" } }`, or `.parity = .{ .none = "<one sentence on why no row covers it>" }`. The row text is the Feature cell as written, trimmed, with `\|` read as `|`, and matched byte for byte; several features may name one row. PARITY.md's Feature texts must therefore stay unique. `zig build test` checks this offline, without the suite: it fails when PARITY.md's feature table has a duplicate Feature text, and when a parity_map.zon entry names a row the table lacks (test `conformance parity map names only rows PARITY.md's feature table has`). Renaming a row means updating its entries in parity_map.zon in the same commit. `zig build fmt` checks the formatting of the `.zon` files under `conformance/`, and `zig fmt conformance/` rewrites them.

### Adding a feature

1. Read the suite feature's README.md and its other SDKs' code in the fetched suite, which a run extracts under `zig-pkg/`.
2. Write `conformance/features/<area>/<name>/feature.zig`, where `<area>/<name>` is the suite path, with `pub const feature = harness.define(.{ .workflows = ..., .activities = ..., .execute = execute });`. `.execute` is required; `.workflows`, `.activities`, `.nexus_services`, `.worker_options`, and `.configureClient` are optional. build.zig finds the file by its path, and the runner rejects a feature the suite doesn't have.
3. Check that its entry in `conformance/parity_map.zon` names the right row, or change it from `.none` to `.row` when the feature now covers one.
4. Run `zig build conformance -Dfeature=<area>/<name>`, then `zig build conformance-check`.
5. When it passes and declares workflows, record its histories with `zig build conformance -Dfeature=<area>/<name> -Drecord-histories` and commit them (see Stored histories below), or `zig build test` fails for the missing history.

When the feature fails because the SDK lacks something, keep the feature as written and file a bead for the gap. Don't add it to not_applicable.zon or work around the gap in the feature: the report keeps it as `fail` with its message, and `zig build conformance-check` lists it as `failing` while its row is supported. With no history to record, it also gets an entry in `conformance/replay/exclusions.zon` with that bead ID, as Stored histories below says.

### Stored histories

The workflow histories of the written conformance features live in `conformance/histories/<feature path>/<temporal.version>/[<variant>/]`, one `<n>-<workflow type>.json` per closed run in start order, with a `runs.json` that gives the namespace, task queue and each run's workflow ID. `zig build conformance -Drecord-histories` records them for every passing feature, and `-Dfeature=<pattern>` limits that to the matching features. A history's directory is named by `temporal.version` (`src/temporal.zig`) as it is when the history is recorded, not by `build.zig.zon`'s `.version`, and the recorder writes only into the current version's directory. The directory of every version other than the current `temporal.version` is frozen: nothing in it is re-recorded, edited or removed. That includes the 0.0.0 set: release 0.1.0-alpha.1 reports itself as 0.0.0, so the 0.0.0 set is what that release records. While `temporal.version` is itself 0.0.0, that directory is also the current one and follows the rule for the current version's directory. The current version's directory takes the history of each newly written feature, recorded with `-Dfeature=<its path>` so that no other history changes; a history already in it is re-recorded only on purpose, with the reason in the commit message. The commit that changes `temporal.version` for a release records the new version's set with `zig build conformance -Drecord-histories` in the same commit, beside the directories of the earlier versions. `zig build test` replays every stored file, of every version, against its feature's workflows (`conformance/replay/replay.zig`), and fails for a written feature that declares workflows and has no history for the current version. A replay failure on a frozen file is an SDK bug, not a reason to re-record: file it, and list the feature in `conformance/replay/exclusions.zon` with that bead ID and the reason. A written feature that fails because of an SDK gap, and so has no history to record, is listed there the same way. An entry exempts its feature from replay and from the missing-history rule, and the test prints it on every run; the commit that fixes the defect, or makes the feature pass and records its history, removes the entry.

### `zig build tsan`

Runs the unit tests under Thread Sanitizer, which reports data races, lock-order inversions and similar threading errors in SDK code as the tests run. It takes `-Doptimize`, `-Dtarget`, `-Dtest-filter` and the core options as `zig build test` does, except that the tests run through a host program that cannot hand them to an emulator (`-fqemu` and the like), so a target must run natively. sdk-core is not instrumented, so the sanitizer does not see a race inside the core, or between SDK code and the core. For the same reason it reports a false race when SDK code hands an object to a core thread through a callback; `handToCore` and `takeFromCore` in `src/bridge/tsan.zig` give it the ordering for every such callback in `src/bridge/` (connections, calls, the gRPC override, worker polls and completions, and the ephemeral server), and any new handoff of that kind gets the same annotation. Some of these are reported only in ReleaseSafe. The scheduler tells the sanitizer about fiber switches (the `tsan` namespace in `src/workflow/Scheduler.zig`). The test "an activity's stack trace holds only its own error's frames, not those of earlier tasks on its thread" in `src/failure_test.zig` is skipped under the sanitizer, because its 64 threads spinning on a barrier took more than 10 minutes.

A report is a `WARNING: ThreadSanitizer: ...` block on stderr with the stacks of both accesses and of the threads' creation. The test process then exits 66 even when every test passed, and the command fails. Fix a real race; annotate a handoff through the core. A real race that is not fixed in the change at hand is filed as a bug and listed in `tools/checks/tsan.zon` with the `site` (a function as the report prints it, such as `worker.Poller.run`), the `bead`, the `reason` and an optional `kind` (`race` unless given), which suppresses it. The command cannot tell that an entry matched nothing, so remove an entry when its bead closes. A malformed entry fails the command, naming the entry. A crash of the sanitized test binary can leave the command hanging, as it can `zig build test`, until bug d51.11.16 is fixed.

- When to run: When changing worker threads, bridge callbacks, the scheduler, or state shared between threads.
- Run time: about 5 minutes in Debug (298 to 315 s over eight runs with the test binary already built, about 4 minutes of it the tests) and about 8 minutes in ReleaseSafe (484 s, including its build), on the factory's Apple-silicon Mac, measured 2026-11-19 with the core already built.
- Linux: Run on 2026-11-19 in the factory's Ubuntu 24.04 container (aarch64), where it fails: tens of thousands of reports, each with a frame of the core in one of the two stacks. On Linux the sanitizer's runtime is linked into the test binary and intercepts the core's `malloc`, `free` and `memcpy`, while the core's own synchronization stays invisible to it, so the sanitizer cannot tell a real race there from the core's own ordering. These reports are not listed in `tools/checks/tsan.zon`. Until that is settled, run the command on macOS.

## README.md is the API documentation

- README.md documents the public API area by area. A commit that changes the shape of the public API (a public function, type, option, error, or default in `src/`'s public namespaces) or adds a capability updates the matching README.md section in the same commit.
- A planned capability has a stub in README.md, marked `<!-- stub: <bead id> -->`. The commit that lands the capability replaces the stub with a full section: explanation, example, and notes. A newly planned capability gets a stub when its bead is created.
- Never edit README.md's code in place. Examples live in `examples/readme/` between `// example: <id>` and `// end-example`: change the region, then run `zig build readme`. `zig build test` compiles and runs the examples, and `zig build readme-check` fails when README.md and the regions differ.
- Sample status goes in PARITY.md, not README.md: a new sample adds a row there, and a sample with no Zig equivalent is listed there with the reason.
- A commit that lands a feature, or part of one, updates its row in PARITY.md's feature table in the same commit: a supported row links the README.md section or the sample that shows it.
- README.md names no other SDK. PARITY.md is the only file that names other SDKs.

## Samples are the parity bar

Each feature is demonstrated by a sample under `samples/`, validated against a dev server by `zig build samples-test`. PARITY.md lists the parity samples; they land from the simplest to the most complex as their SDK features land.

- A sample is a directory `samples/<name>/` with `worker.zig`, `starter.zig`, and `sample.zon`, which lists the starter output to expect, the sample's test files, other samples it imports, and any extra programs to build (such as a server the sample talks to). build.zig finds samples by their `sample.zon`. A sample that needs TLS, client certificates, or JWT authorization sets `.docker_server = .{ .require_client_cert = true, .jwt = true }` in `sample.zon`: samples-test then runs it against its own `temporalio/server` container and passes the connection settings in environment variables (see `DockerServer` in `tools/samples_runner.zig`), and reports it as skipped where Docker isn't available. A sample that needs extra namespaces or a Nexus endpoint declares them in `sample.zon` with `.namespaces = .{"my-target-namespace"}` and `.nexus_endpoints = .{.{ .name = "nexus-endpoint", .namespace = "my-target-namespace", .task_queue = "nexus-handler" }}`: samples-test starts the shared dev server with those namespaces and creates the endpoints before the sample's programs run (see `Manifest` in `tools/samples_runner.zig`); a sample with `docker_server` can't declare them. Names on the shared dev server (task queues, endpoints, and workflow IDs in a shared namespace) must be the sample's own, because samples run concurrently. samples-test runs most samples from one program, `samples/dispatch.zig`, generated by `zig build samples-index`; rerun it after adding or removing a sample (CI checks it).
- Samples are idiomatic Zig: Zig names for workflows, activities, and payload fields (snake_case), Zig types for durations and times, and the Zig equivalent of each concept (for example `std.Io.Group` for concurrent workflow coroutines).
- Each sample has tests covering its behavior, run in `samples-test`.
- Comments and docs describe what the code does in Zig terms, without referring to other SDKs.
- A finished sample stays in the suite. A change that breaks a passing sample is a regression, even if the unit tests pass.
- A parity sample with no Zig equivalent is listed in PARITY.md with the reason.

## Zig SDK review and implementation focus

- Workflow code must stay deterministic. Anything reachable from workflow execution must not use threads, wall-clock time, randomness, unordered hash map iteration, or I/O outside the workflow `std.Io`.
- For workflow runtime, command, patch, update, local activity, or Nexus workflow changes, add replay coverage with the workflow replayer to protect existing histories.
- Behavior that crosses the core boundary follows sdk-core's rules (activation job ordering, patch semantics, local activity backoff). When in doubt, compare against the official SDKs that drive the same core, which PARITY.md names.
- Prefer idiomatic Zig over literal ports: explicit allocators and `std.Io`, struct-literal options, comptime-typed definitions, error sets with failure details read from the future or handle. See [allocator passing](docs/standards.md#allocator-passing).
- Document ownership, lifetime, and thread-safety at every interface boundary. Every `TemporalCoreByteArray` the core returns must be freed with `temporal_core_byte_array_free`. Core callbacks run on the core's threads, not the caller's. See the [ownership standards](docs/standards.md#ownership).
- When the C header changes, the ABI snapshot test fails. Update every wrapper that builds or reads a changed struct before refreshing the snapshot.
- Zig panics can't be caught. SDK code returns errors instead of panicking on bad input from users, the server, or the core.
- Workflow fibers support aarch64 and System V x86_64 only. Windows x64 is a compile error until its register set is implemented.

## Pull request expectations

- Keep changes focused and include tests for behavior changes.
- Update README.md when the public API changes.
- Run `zig build fmt`, `zig build test`, and `zig build test -Doptimize=ReleaseSafe`.
- Run `zig build samples-test` when the change touches the client, worker, workflow runtime, activities, Nexus, payload conversion, or interceptors.
- A user-visible change adds a fragment `changelog/<category>/<name>.md` (changelog/README.md has the rules); otherwise its commit message ends with a `Changelog: none (<reason>)` trailer. A breaking change (docs/releasing.md) adds a `breaking-changes/` fragment, a `Breaking-Change:` trailer, and a subject starting with 💥.
- Commit messages: a short imperative subject, and a body that explains why. CONTRIBUTING.md has the full commit and pull request description style, and `.github/PULL_REQUEST_TEMPLATE.md` carries the description's sections.

## Where things are

- `src/temporal.zig` is the root module.
- `src/client.zig`, `src/worker.zig`, `src/workflow.zig`, `src/activity.zig`, `src/nexus.zig`, `src/converter.zig`, and `src/testing.zig` are the public namespaces.
- `src/workflow/Scheduler.zig` is the deterministic fiber scheduler that workflow code runs on.
- `src/bridge.zig` wraps the sdk-core C API; `src/bridge_abi.txt` is its struct layout snapshot.
- `src/proto/` and `src/proto.zig` are the generated protobuf types (`temporal.proto`).
- `deps/sdk-core/` is the pinned sdk-core submodule. `deps/Cargo.toml` and `deps/Cargo.lock` build its C bridge.
- `samples/` holds the samples, and `tools/samples_runner.zig` validates them. PARITY.md lists them, explains how parity is measured, and has the feature table; `scripts/parity-check.sh` checks the table.
- `examples/readme/` holds the compiled examples that README.md's code blocks copy, and `tools/readme_examples.zig` checks or rewrites the copies.
- `tools/checks/` holds the correctness commands and their fixtures.
- `conformance/` holds the features-suite conformance run: `features/<area>/<name>/feature.zig` are the Zig features, `harness/` and `runner/` run them, `parity_map.zon` maps each suite feature to a PARITY.md row, `not_applicable.zon` lists the features with no Zig equivalent, `histories/` holds the stored workflow histories, and `replay/` replays them in `zig build test`.
- `scripts/` holds maintenance scripts, each with usage in its header.
- `.review/` holds the review tooling: `reports.sh` runs the checks and the scanners (gitleaks, osv-scanner, cargo-deny, shellcheck, actionlint, typos, zlint) on a change, `install.sh` installs them, and the `.toml` files are their settings. It isn't part of the SDK and isn't in the release tarball.
- `docs/` holds maintainer documentation.
- `CONTRIBUTING.md` is the guide for issues and pull requests; `.github/PULL_REQUEST_TEMPLATE.md` and `.github/ISSUE_TEMPLATE/` hold the pull request and issue templates.
- `.github/workflows/` runs CI (`ci.yml`) and the samples suite (`samples.yml`), and, when dispatched by hand, builds and publishes the core bundles (`core-bundles.yml`) and releases the SDK (`release.yml`).
- `tools/release-smoke/` is a project that depends on a release tarball, built by `scripts/smoke-release.sh`.
- `CHANGELOG.md` is assembled from the `changelog/` fragments when a release is prepared, not edited by ordinary changes; `docs/upgrading.md` holds the before and after code for breaking changes. [docs/releasing.md](docs/releasing.md) has the version and support policy and the release sequence.

## Notes

- `zig fetch` doesn't fetch git submodules, so a project that depends on this SDK by git URL gets no sdk-core and can't build the core. Users fetch a release tarball instead, whose build links the core bundle `build.zig.zon` pins; building from source takes a recursive clone and a path dependency.
- The generated protobuf code uses zig-protobuf with unknown-field preservation on. Write and read ProtoJSON with `temporal.protojson` (`src/protojson.zig`), not the generated `jsonEncode` and `jsonDecode`: it writes standard ProtoJSON, accepts short enum names, and base64-decodes the values of bytes maps.
