`ctx.logger()` returns a structured `log.Logger` carrying the workflow's or activity's context; records go to a `log.Sink` set with `client.Options.logger` or `worker.Options.logger`, which also receives sdk-core's logs.
`client.Options.metrics` exports the SDK's and sdk-core's metrics to Prometheus, an OpenTelemetry collector or a custom `metrics.Handler`, and code records its own through `ctx.metrics()` and `client.metrics()`.
`interceptor.Worker` and `interceptor.Client` intercept workflow, activity and client calls, installed with `worker.Options.interceptors`, `client.Options.worker_interceptors` and `client.Options.interceptors`.
`propagation.ContextPropagator` carries values such as a tenant or trace ID through workflow, activity and child workflow headers.
