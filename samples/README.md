# Samples

Zig ports of [temporalio/samples-go](https://github.com/temporalio/samples-go). Each directory
has a worker, a starter, the Go sample's tests ported to Zig, and a `sample.zon` that tells
`zig build samples-test` how to validate it against a dev server.

Most samples keep the Go sample's directory name. A sample named after a Go-only concept is
named for its Zig equivalent instead; it still uses the Go sample's workflow, activity, and task
queue names, so the Go starter can drive the Zig worker.

## Renamed samples

| Zig sample | samples-go sample | Why |
|------------|-------------------|-----|
| [io-group](io-group) | [goroutine](https://github.com/temporalio/samples-go/tree/main/goroutine) | Workflow coroutines are `std.Io.Group` tasks on the workflow's `std.Io`, not goroutines. |

## Samples with no Zig equivalent

None yet.
