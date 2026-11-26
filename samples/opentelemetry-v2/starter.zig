//! Runs the first two parts' workflows with the name "Temporal" and prints their results.
//! Then starts the third part's workflow and sends it update `greet` with "Temporal" from
//! inside an OpenTelemetry span of the starter's own, `send-update`: its context, put under
//! `temporal.tracing.span_key` in the call's `.context`, is the parent of the client's
//! `UpdateWorkflow:greet` span, so the update's spans on the worker join its trace.

const std = @import("std");
const temporal = @import("temporal");
const temporal_opentelemetry = @import("temporal_opentelemetry");
const sample = @import("opentelemetry-v2.zig");

const Client = temporal.client.Client;
const update = sample.client_update_propagation;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // Destroyed after the client, so the client's spans are exported before the program ends.
    const tracing = sample.setup.Tracing.create(gpa, io, init.environ_map, .{
        .service_name = "temporal-otel-v2-client",
        .scope_name = "temporal-sdk-zig/samples/opentelemetry-v2/starter",
    }) catch |err| {
        std.log.err("Unable to create a trace provider: {t}", .{err});
        std.process.exit(1);
    };
    defer tracing.destroy();
    const interceptors = [_]temporal.interceptor.Client{tracing.interceptor.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{tracing.interceptor.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{tracing.interceptor.propagator()};

    // The client is a heavyweight object that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(arena);
    options.interceptors = &interceptors;
    options.worker_interceptors = &worker_interceptors;
    options.propagators = &propagators;
    var client = Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    try executeGreeting(&client, arena, sample.automatic_instrumentation, "opentelemetry-v2-automatic-instrumentation");
    try executeGreeting(&client, arena, sample.workflow_activity_propagation, "opentelemetry-v2-workflow-activity-propagation");

    var run = client.executeWorkflow(update.Workflow, .{
        .id = "opentelemetry-v2-client-update-propagation",
        .task_queue = update.task_queue,
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    const update_result = sendUpdate(&client, tracing, arena, run.id, run.run_id, "Temporal") catch |err| {
        std.log.err("Unable to send Workflow Update: {t}", .{err});
        std.process.exit(1);
    };
    const workflow_result = run.get(arena) catch |err| {
        std.log.err("Unable to get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Update result: {s}", .{update_result});
    std.log.info("Workflow result: {s}", .{workflow_result});
}

/// Runs `part.Workflow` on `part.task_queue` with the name "Temporal" and prints its result.
fn executeGreeting(client: *Client, arena: std.mem.Allocator, comptime part: type, id: []const u8) !void {
    var run = client.executeWorkflow(part.Workflow, .{ .id = id, .task_queue = part.task_queue }, .{"Temporal"}) catch |err| {
        std.log.err("Unable to execute workflow on {s}: {t}", .{ part.task_queue, err });
        std.process.exit(1);
    };
    defer run.deinit();
    const result = run.get(arena) catch |err| {
        std.log.err("Unable to get the result of workflow {s}: {t}", .{ id, err });
        std.process.exit(1);
    };
    std.log.info("{s} result: {s}", .{ part.task_queue, result });
}

/// Sends update `greet` with `name` inside the root span `send-update` and returns its result.
fn sendUpdate(client: *Client, tracing: *sample.setup.Tracing, arena: std.mem.Allocator, workflow_id: []const u8, run_id: []const u8, name: []const u8) ![]const u8 {
    var span = try tracing.tracer.startSpan(tracing.gpa, "send-update", .{ .root = true });
    defer {
        span.end(null);
        span.deinit();
    }

    var context: temporal.propagation.Values = .init(arena);
    try context.put(temporal.tracing.span_key, try temporal_opentelemetry.fromSpanContext(arena, span.span_context));

    var handle = try client.updateWorkflow(update.Greet, workflow_id, run_id, .{name}, .{
        .wait_for_stage = .completed,
        .context = &context,
    });
    defer handle.deinit();
    return handle.get(arena) catch |err| {
        if (handle.failure()) |f| std.log.err("Workflow Update failed: {s}", .{f.message});
        return err;
    };
}
