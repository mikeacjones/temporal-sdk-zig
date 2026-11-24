//! A workflow starts three synchronous Nexus operations in one workflow task, then awaits
//! them in order and joins their results. The history has three scheduled events, all from
//! the same workflow task, three completed events, and no started events.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const nexus = temporal.nexus;
const workflow = temporal.workflow;
const SayHello = nexus.OperationDef("say-hello", []const u8, []const u8);
const Service = nexus.ServiceDef("test-service", .{SayHello});
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(SayHello, sayHello)});
const Workflow = workflow.Def("Workflow", runWorkflow);

const names = [_][]const u8{ "one", "two", "three" };

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .nexus_services = .{Handler},
    .execute = execute,
});

fn sayHello(ctx: *nexus.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

fn runWorkflow(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
    var operations: [names.len]workflow.NexusOperationFuture([]const u8) = undefined;
    for (&operations, names) |*operation, name| {
        operation.* = ctx.executeNexusOperation(Service, SayHello, .{
            .endpoint = endpoint,
            .schedule_to_close_timeout = .fromSeconds(60),
        }, name);
    }
    var results: [names.len][]const u8 = undefined;
    for (&results, operations) |*result, operation| result.* = try operation.get();
    return std.mem.join(ctx.allocator(), " ", &results);
}

fn execute(ctx: *harness.Context) !void {
    const endpoint = ctx.nexus_endpoint orelse return ctx.fail("missing Nexus endpoint for {s}", .{ctx.task_queue});
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{endpoint});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        if (run.failure()) |failure| return ctx.fail("workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings("Hello, one! Hello, two! Hello, three!", result);

    const recorded = try ctx.fetchHistory(run.id, run.run_id);
    try ctx.expectNoEvent(recorded, .EVENT_TYPE_NEXUS_OPERATION_STARTED);
    var completed: usize = 0;
    var scheduled: usize = 0;
    var scheduled_by: i64 = 0;
    for (recorded.events.items) |event| {
        const attributes = event.attributes orelse continue;
        switch (attributes) {
            .nexus_operation_completed_event_attributes => completed += 1,
            .nexus_operation_scheduled_event_attributes => |a| {
                if (scheduled == 0) scheduled_by = a.workflow_task_completed_event_id;
                try ctx.expect(
                    a.workflow_task_completed_event_id == scheduled_by,
                    "expected all operations to be scheduled by one workflow task, got tasks {d} and {d}",
                    .{ scheduled_by, a.workflow_task_completed_event_id },
                );
                scheduled += 1;
            },
            else => {},
        }
    }
    try ctx.expect(scheduled == names.len, "expected {d} scheduled operations, got {d}", .{ names.len, scheduled });
    try ctx.expect(completed == names.len, "expected {d} completed operations, got {d}", .{ names.len, completed });
}
