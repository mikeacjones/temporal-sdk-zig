# Parity

This file records how the SDK's feature coverage is measured and which samples demonstrate it. It is the one file in the repository that names the SDK and samples coverage is measured against.

## How parity is measured

This SDK is not a port of another SDK, but it measures its feature coverage against the [Go SDK](https://github.com/temporalio/sdk-go), Temporal's most complete feature set, and its [samples](https://github.com/temporalio/samples-go). Every samples-go sample gets a Zig sample that demonstrates the same feature in idiomatic Zig, with its own names, payload shapes, and tests. `zig build samples-test` runs every sample against a dev server, and the Samples badge shows whether they pass.

Where the Go SDK relies on machinery Temporal's Rust core doesn't have, this SDK behaves like the other core-based SDKs (.NET, Python, TypeScript, Ruby): a side effect is a local activity, version checks use patches, and tests run against the dev server and the time-skipping test server instead of an in-memory mock.

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

## Samples with no Zig equivalent

None are recorded yet.
