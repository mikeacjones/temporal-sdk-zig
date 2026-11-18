# zlinter evaluation

Evaluated zlinter commit `e1f92a1159a36fa301730596f742d0f66e60908d` from `0.17.x` with Zig 0.17.1 on 2026-11-18. The SDK baseline was `origin/main` at `7d58fb081c95976724fa118ffc7e778530a47657`. The kept rules were also clean on the bead's base, `53ef08b`, which adds the API and test allocator tools. [zlinter source](https://github.com/KurtWagner/zlinter/tree/e1f92a1159a36fa301730596f742d0f66e60908d)

## Rules

The evaluation build registered all 30 `BuiltinLintRule` members with their default settings. The scan covered `src`, `tools`, `examples`, and `samples`, excluding `src/proto/`, `src/proto.zig`, `samples/dispatch.zig`, and `tools/checks/fixtures/`. This is 337 hand-written Zig files on the baseline. zlinter also recognizes ZON files in the included directories. Counts include warnings and errors. Paths and line numbers below refer to the baseline. For zero counts, the table gives examples of what would trigger the rule.

To reproduce the full survey, replace the `addRule` calls in a scratch build with an `inline for` over `std.enums.values(zlinter.BuiltinLintRule)`, passing `.{}` as each rule's configuration. Run `zig build lint -- --format json --exit-zero` and count `.results[].messages[].ruleId` in the JSON on stdout. For the baseline measurement, the resulting executable ran from a copy of `origin/main` with its own build graph and the same paths passed via `--build-include` and `--build-exclude`.

| Rule | Findings on main | Examples | Decision | Reason |
| --- | ---: | --- | --- | --- |
| `field_naming` | 129 | `src/activity.zig:786` short country enum tags, `src/bridge/Worker.zig:43` long timeout field, `src/bridge/completion.zig:34` snake-case callback field | Off | The length and casing defaults conflict with descriptive option names, protocol values, and callback fields. |
| `field_ordering` | 91 | `src/activity.zig:127` `CancelReason`, `src/bridge/EphemeralServer.zig:23` `Executable`, `samples/expense/expense_system.zig:25` `State` | Off | Declaration order can express state progression or determine implicit enum values. |
| `declaration_naming` | 3,110 | `src/activity.zig:120` short local `a`, `src/client.zig:3172` long public type name, `examples/readme/activities.zig:15` `Greet` | Off | The default length and category heuristics flag ordinary locals and idiomatic typed definitions. |
| `function_naming` | 13 | `src/converter.zig:341` `unconvertibleType`, `src/nexus.zig:106` `operationNamed`, `src/bridge/Worker.zig:266` `poll_fn` | Off | The defaults prescribe casing for optional type lookups and callback arguments that conflicts with existing usage. |
| `file_naming` | 0 | `Worker.zig` containing only namespace declarations, `worker.zig` with root struct fields | Keep | File casing distinguishes namespaces from file-level structs and agrees with the existing layout. |
| `import_ordering` | 233 | `src/activity.zig:63` `converter`, `src/bridge/Connection.zig:9` `c`, `src/client.zig:17` `bridge` | Off | Import groups communicate dependencies. Global alphabetical order would churn many files. |
| `no_unused` | 12 | `src/interceptor_test.zig:755` unused `Handler`, `samples/cron/workflow.zig:4` and `samples/saga/saga.zig:6` unused `std` | Off for now | Removing dead private declarations and imports is useful. Review references and clean up the existing findings first. |
| `no_deprecated` | 694 | `src/activity.zig:643` `allocPrint`, `src/bridge/Connection.zig:151` `indexOfScalar`, `src/client.zig:223` `bufPrint` | Off for now | A good migration check. Bulk updates for Zig 0.17.1 belong in a separate change. |
| `no_empty_block` | 41 | `src/client.zig:5410` no-op test workflow, `src/client_tls_test.zig:61` successful Docker probe, `examples/readme/converter.zig:223` no-op sample activity | Off | Intentional stubs and ignored success branches are common. The error-handling rules target the risk more directly. |
| `no_inferred_error_unions` | 317 | `src/activity.zig:691` test activity, `examples/readme/activities.zig:19` example activity, `samples/await-signals/starter.zig:8` sample `main` | Off for now | Explicit error sets help at public SDK boundaries. Review or narrow the rule before applying it across samples and test code. |
| `no_orelse_unreachable` | 2 | `src/workflow/Scheduler.zig:245` and `:252` assertions that a fiber is current | Off | These assertions spell out a scheduler invariant. Requiring `.?` would be a style change. |
| `require_labeled_continue` | 25 | `src/client.zig:2448` history operation filter, `samples/nexus/starter.zig:31` link filter, `samples/sleep-for-days/starter.zig:68` missing attributes | Off | Short inner-loop filters already have clear targets. Apply labels in review when the target is hard to see. |
| `no_unsafe_undefined` | 14 | `src/nexus.zig:1529` returns `undefined`, `src/testing.zig:3055` undefined slice, `samples/expense/expense_system.zig:190` undefined enum | Off for now | Review the initialization and return-value invariants at each site before enforcing it. |
| `no_literal_only_bool_expression` | 0 | `if (1 == 1)`, `if (false)` | Keep | Catches accidentally unconditional or dead branches. The rule allows `while (true)`. |
| `no_hidden_allocations` | 0 | direct `std.heap.page_allocator.alloc(...)`, direct `std.heap.c_allocator.create(...)` | Keep | Catches calls to recognized heap allocators that bypass a caller's allocator. The default detector covers direct calls to a small set of allocators and excludes test bodies. |
| `switch_case_ordering` | 0 | `else` before a named tag, `else` before an explicit integer case | Keep | A trailing fallback makes explicit cases easier to scan. |
| `max_positional_args` | 48 | `src/bridge/Connection.zig:540` eight arguments, `src/bridge/Worker.zig:299` six, `src/client.zig:2237` seven | Off | A fixed threshold counts the receiver, allocator, I/O context, and comptime arguments. Some flagged signatures are public. |
| `no_comment_out_code` | 0 | `// const removed = 2;`, `// return error.OutOfMemory;` | Keep | Catches stale code left in comments. The heuristic allows backticked snippets. |
| `no_todo` | 0 | `// TODO: Finish the fallback.`, `// todo: Add a timeout.` | Keep | Keeps TODOs tied to an issue. Defaults allow `#123` or a URL. |
| `no_literal_args` | 86 | `src/activity.zig:176` `.init(false)`, `examples/readme/client.zig:301` final `false` argument to `stopChain`, `samples/searchattributes/searchattributes.zig:76` `.set(true)` | Off | Calls like atomic initialization and Boolean setters make the literal's meaning clear. |
| `no_swallow_error` | 130 | `src/bridge/Connection.zig:657` empty shutdown catch, `src/client.zig:443` `catch unreachable`, `src/failure.zig:397–401` empty error branch | Off for now | Worth auditing for discarded failures, with deliberate best-effort cleanup and proven invariants reviewed separately. |
| `no_panic` | 11 | `src/bridge/completion.zig:137` OOM in a test callback, `src/client_rpc_options_test.zig:61` OOM in a helper, `src/workflow/WorkflowIo.zig:494` coroutine invariant | Off for now | Audit runtime panics against the SDK's error policy and account for test helpers and internal invariants. |
| `no_redundant_comptime` | 252 | `src/activity.zig:380` `comptime T: type`, `samples/recovery/lru.zig:12` `comptime V: type`, `samples/saga/saga.zig:44` `comptime Step: type` | Off | The explicit marker is a repository convention and documents the phase at the call boundary. |
| `require_exhaustive_enum_switch` | 2 | `src/client.zig:699` connect log wording, `src/client.zig:1090` first-use deadline | Off | Both switches deliberately group cases that share one policy. |
| `require_braces` | 101 | `src/activity.zig:434` compile-time signature check, `examples/readme/workers.zig:44` worker error log, `samples/await-signals/workflow.zig:83` signal timeout | Off | The multi-line heuristic flags intentionally unbraced single statements. |
| `require_doc_comment` | 1,191 | `src/activity.zig:73` public `Info`, `src/bridge/Connection.zig:147` public `next`, `examples/readme/activities.zig:15` example `Greet` | Off for now | Audit real public SDK documentation gaps before enabling a rule that also counts internal and example declarations. |
| `require_errdefer_dealloc` | 106 | `src/activity.zig:748` context-allocated writer, `examples/readme/workflows.zig:397` list on the workflow allocator, `samples/batch-sliding-window/starter.zig:16` argument iterator | Off | The rule misses ownership through arenas and can flag initializers that do not allocate. Review reported sites for actual leaks before using it as a gate. |
| `require_fmt` | 0 | extra spaces around an assignment, misindented block body | Off | `zig build fmt` already enforces Zig formatting. |
| `no_global_vars` | 110 | `src/bridge/Runtime.zig:73` callback forwarding slots, `examples/readme/testing.zig:152` coordination event, `samples/branch/workflow_test.zig:11` test counter | Off | The blanket ban flags required callback state and deliberate test coordination. |
| `build_zig_zon` | 1 | `tools/release-smoke/build.zig.zon:8` `.paths = .{""}`. The rule would also flag a manifest with no `.paths` field. | Off | The smoke project is a consumer fixture. The root package manifest is outside the scan and already lists `LICENSE`. |

## Overlap with zlint

`.review/install.sh` pins zlint v0.10.0. `.review/reports.sh` sends changed existing Zig file paths to `zlint -S -f json` by default and tracked Zig paths with `--all`. It excludes `src/proto/`, `src/proto.zig`, and `deps/sdk-core/`. Its scope can include `build.zig`, `samples/dispatch.zig`, and Zig fixtures. The repository supplies no zlint config, so the rule comparison uses v0.10.0's default severities.

I ran the review script's zlint invocation with its `--all` file list from the same `origin/main` snapshot. It reported 238 warnings across five rules. The three direct overlaps were `suppressed-errors` with `no_swallow_error` (114 matching file and line pairs), `unsafe-undefined` with `no_unsafe_undefined` (12), and `unused-decls` with `no_unused` (10). zlint's `unsafe-undefined` reported 97 warnings, including `samples/pso/swarm.zig:134`, outside zlinter's narrower patterns. zlinter's `no_unused` also caught `src/interceptor_test.zig:755`. zlint's `empty-file` checks whole files. zlinter's `no_empty_block` checks bodies. zlint's wider `case-convention` rule is disabled by default.

zlint also checks for `std.debug.print`, redundant catch-and-return, unresolved file imports, `try` in non-error contexts, and copies of fields that store capacity. Its `no-print` and `no-catch-return` produced the other 16 warnings. zlinter reads Zig's evaluated build configuration to resolve imports and declarations for checks such as `no_deprecated` and `no_hidden_allocations`. It can also check file-level struct naming, tracked TODOs, literal-only conditions, and ZON manifests. The kept set adds checks outside the three overlapping diagnostics.

## Run time

On the factory's Apple-silicon Mac on 2026-11-18, the full 30-rule executable took 31.44 s on the first scan of the baseline snapshot and 18.92 s on the second. The first scan included evaluating the baseline's build configuration. The evaluation build of the all-rule executable and scan ran in 37.16 s in the bead checkout with warm package and build-configuration caches.

With the six kept rules, `/usr/bin/time -p zig build lint --cache-dir .zig-cache/zlinter-eval/lint-final-cache --summary all` took 30.63 s with an empty local build cache and 2.45 s on the next run. The packages were already fetched and the global compiler cache was warm. The first time includes compiling the linter in ReleaseSafe and running the fixture. The warm run reused the fixture result and scanned the full tree again.

## Users of the package

The dependency is lazy. `build` calls `addCheckSteps` when `tools/checks` exists, and its zlinter block calls `b.lazyImport` only for the root project. The release package has no `tools/checks` directory. A path dependency on a full checkout reaches the guard and skips the import because the consumer is the root project.

I built two copies of `tools/release-smoke` from empty `zig-pkg` directories using a local bundle for the pinned core. For the release case I packed the SDK's published paths without `deps/sdk-core`, including the candidate `build.zig` and `build.zig.zon`, and ran `zig fetch --save=temporal "file://$PWD/../temporal.tar.gz"` in the consumer. For the path case, I set `.temporal.path` to the full checkout. In each consumer I ran:

```sh
zig build install -Dcore-bundle="$PWD/../core-bundle" --summary all
ls -1 zig-pkg
test -z "$(find zig-pkg -maxdepth 1 -name 'zlinter-*' -print)"
```

The build linked and installed `release-smoke` in both cases. The release consumer fetched the SDK package and protobuf. The path consumer fetched protobuf and the SDK's pinned host protoc. Both package directories lacked zlinter after the build.

In a root checkout, the first `zig build` of any step fetches zlinter once, like the lazy host protoc dependency. This includes CI checkouts and the factory's Linux checkout. `zig build -l` added `zlinter-*` to a local package directory that started with only protobuf and protoc. Only `zig build lint` compiles and runs the linter. The failing fixture runs as a dependency of `lint` because wiring it into `test` would compile zlinter for contributors who never lint. The fixture checks zlinter's findings exit code and the rule name in its diagnostic.

The scan and the fixture run from the checkout root, including when `zig build lint` is invoked from a subdirectory. To resolve imports, zlinter invokes `zig build --print-configuration-path` with default options. Initialize `deps/sdk-core` with `git submodule update --init` before linting. An outer `-Dcore-bundle` option applies only to the outer build. Lint uses the build configuration without compiling or linking sdk-core.

## Decision

Adopt six rules that ran clean on main and the bead base. Make each finding an error so `zig build lint` can serve as a gate. The fresh-checkout download is the trade-off for a full-tree scan and checks beyond the review script's zlint rules. The seven rules marked **Off for now** are cleanup candidates. Their counts are the baseline for that work.
