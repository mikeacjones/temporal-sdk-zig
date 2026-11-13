`activity.Def` defines an activity from a function taking `*activity.Context`; `activity.defs` defines one per method of a struct whose state the activities share, registered with `Worker.registerActivities`.
`activity.Context` gives an activity its allocator, its worker's `std.Io`, `info()`, a logger, a client, headers, propagated values and metrics.
`activity.Context.heartbeat` and `heartbeatDetails` record progress for retries, and a cancel from the workflow, a timeout or worker shutdown reaches the activity as `error.Canceled` with `cancelReason()` saying why.
`activity.Context.fail` sets the failure's type, details, retryability, category and next retry delay; `completeAsync` leaves the activity to be completed through a client.
`activity.DynamicDef` defines an activity that runs every type the worker has no `Def` for.
