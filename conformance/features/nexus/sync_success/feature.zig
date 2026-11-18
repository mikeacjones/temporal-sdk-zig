const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const nexus = temporal.nexus;
const workflow = temporal.workflow;
const SayHello = nexus.OperationDef("say-hello", []const u8, []const u8);
const Service = nexus.ServiceDef("test-service", .{SayHello});
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(SayHello, sayHello)});
const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .nexus_services = .{Handler},
    .execute = execute,
});

fn sayHello(ctx: *nexus.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn runWorkflow(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    return ctx.executeNexusOperation(Service, SayHello, .{
        .endpoint = endpoint,
        .schedule_to_close_timeout = .fromSeconds(60),
    }, "world").get();
}

fn execute(ctx: *harness.Context) !void {
    const endpoint = ctx.nexus_endpoint orelse return ctx.fail("missing Nexus endpoint for {s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{endpoint});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("Hello, world!", result);
    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectEventsInOrder(recorded, &.{ .EVENT_TYPE_NEXUS_OPERATION_SCHEDULED, .EVENT_TYPE_NEXUS_OPERATION_COMPLETED });
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_NEXUS_OPERATION_STARTED);
}
