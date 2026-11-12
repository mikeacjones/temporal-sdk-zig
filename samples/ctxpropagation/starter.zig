const std = @import("std");
const temporal = @import("temporal");
const ctxpropagation = @import("ctxpropagation.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var tracer: ctxpropagation.Tracer = .{ .service_name = "ctx-propagation-sample" };
    const interceptors = [_]temporal.interceptor.Client{tracer.client()};
    const propagators = [_]temporal.propagation.ContextPropagator{ctxpropagation.propagator};

    // The client is a heavyweight object that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(arena);
    options.interceptors = &interceptors;
    options.propagators = &propagators;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // The propagator writes this value into the start request's header.
    var values: temporal.propagation.Values = .init(arena);
    try values.put(ctxpropagation.propagate_key, .{ .key = "test", .value = "tested" });

    var run = client.executeWorkflow(ctxpropagation.CtxPropWorkflow, .{
        .id = try std.fmt.allocPrint(arena, "ctx-propagation_{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "ctx-propagation",
        .context = &values,
    }, .{}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();

    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Wait for the workflow, so the run ends with the sample. The worker logs the value it
    // saw in the workflow and in the activity.
    run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow completed. Check worker logs.", .{});
}
