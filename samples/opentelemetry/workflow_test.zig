//! Tests for the OpenTelemetry sample.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("opentelemetry.zig");

const testing = std.testing;

test "the workflow completes with the traced activity" {
    // The environment turns the OpenTelemetry SDK off, so nothing is exported: the default
    // exporter writes to stdout, which is the test runner's channel to the build runner. Spans
    // still start and carry their contexts from the client to the activity's custom-span.
    var environ: std.process.Environ.Map = .init(testing.allocator);
    defer environ.deinit();
    try environ.put("OTEL_SDK_DISABLED", "true");
    const tracing = try sample.setup.Tracing.create(testing.allocator, testing.io, &environ, .{ .scope_name = "temporal-sdk-zig/samples/opentelemetry/test" });
    defer tracing.destroy();
    const interceptors = [_]temporal.interceptor.Client{tracing.interceptor.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{tracing.interceptor.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{tracing.interceptor.propagator()};

    const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .interceptors = &interceptors,
        .worker_interceptors = &worker_interceptors,
        .propagators = &propagators,
    });
    defer env.deinit();
    var activities: sample.Activities = .{ .gpa = testing.allocator, .tracer = tracing.tracer };
    try env.registerWorkflow(sample.Workflow);
    try env.registerActivityWith(sample.Activity, &activities);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try env.executeWorkflow(sample.Workflow, .{"Temporal"}, arena.allocator(), null);
}

test "the stdout exporter appends to a stdout that already holds the program's log" {
    // `worker > worker.log 2>&1`: stdout and stderr share one file and one offset. The log
    // lines written before the spans are exported, and after, must all stay in the file.
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(testing.io, "out.log", .{ .read = true });
    defer file.close(testing.io);

    const saved = std.c.dup(1);
    try testing.expect(saved >= 0);
    defer _ = std.c.close(saved);
    try testing.expect(std.c.dup2(file.handle, 1) >= 0);
    var restored = false;
    defer if (!restored) {
        _ = std.c.dup2(saved, 1);
    };

    const before = "info: Started workflow WorkflowID=otel_workflowID\n";
    _ = std.c.write(1, before, before.len);
    var environ: std.process.Environ.Map = .init(testing.allocator);
    defer environ.deinit();
    const tracing = try sample.setup.Tracing.create(testing.allocator, testing.io, &environ, .{ .scope_name = "temporal-sdk-zig/samples/opentelemetry/test" });
    var span = try tracing.tracer.startSpan(testing.allocator, "exported-span", .{ .root = true });
    span.end(null);
    span.deinit();
    tracing.destroy();
    const after = "info: Workflow completed.\n";
    _ = std.c.write(1, after, after.len);

    try testing.expect(std.c.dup2(saved, 1) >= 0);
    restored = true;
    const contents = try tmp.dir.readFileAlloc(testing.io, "out.log", testing.allocator, .limited(1024 * 1024));
    defer testing.allocator.free(contents);
    try testing.expectStringStartsWith(contents, before);
    try testing.expect(std.mem.indexOf(u8, contents, "\"name\":\"exported-span\"") != null);
    try testing.expectStringEndsWith(contents, after);
}
