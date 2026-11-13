`workflow.Def` and `workflow.DefWithOptions` define a workflow from a function taking `*workflow.Context` or from a struct whose fields are the run's state; arguments and results are typed from the signature, and a bad signature is a compile error.
Workflow code runs on a deterministic `std.Io` from `ctx.io()`: `io.sleep` is a durable timer, `io.now` workflow time, `io.random` seeded from the run, and `io.async`, `std.Io.Select`, `std.Io.Group`, `std.Io.Mutex` and `std.Io.Queue` run coroutines one at a time in a fixed order.
`ctx.await` and `ctx.awaitTimeout` wait for a condition; canceling a coroutine cancels the timer, activity or child workflow it waits on.
`workflow.DynamicDef` defines a workflow that runs every type the worker has no `Def` for.
