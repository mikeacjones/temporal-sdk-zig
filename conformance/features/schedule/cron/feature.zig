//! A workflow started with the cron schedule `@every 2s` sees that schedule in its info and
//! completes; listed by its workflow ID once a second, up to ten times, it shows at least two
//! completed runs and no run that is neither completed nor running. The workflow is
//! terminated when the feature ends, so no run stays open.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Io = std.Io;
const Workflow = workflow.Def("Workflow", run);
const cron_schedule = "@every 2s";
const attempts = 10;
const poll_interval: Io.Duration = .fromSeconds(1);

pub const feature = harness.define(.{ .workflows = .{Workflow}, .execute = execute });

fn run(ctx: *workflow.Context) ![]const u8 {
    if (!std.mem.eql(u8, ctx.info().cron_schedule, cron_schedule)) return error.InvalidCronSchedule;
    return "";
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.cron_schedule = cron_schedule;
    var started = try ctx.client.executeWorkflow(Workflow, options, .{});
    defer started.deinit();
    defer ctx.client.terminateWorkflow(started.id, "", "feature complete") catch |err|
        std.log.warn("terminating cron workflow {s} failed with error.{t}", .{ started.id, err });

    const query = try std.fmt.allocPrint(ctx.arena, "WorkflowId = '{s}'", .{started.id});
    var scratch: std.heap.ArenaAllocator = .init(ctx.gpa);
    defer scratch.deinit();
    var completed: usize = 0;
    for (0..attempts) |_| {
        try ctx.io.sleep(poll_interval, .awake);
        _ = scratch.reset(.retain_capacity);
        completed = 0;
        var executions = try ctx.client.listWorkflows(query, .{});
        defer executions.deinit();
        while (executions.next(scratch.allocator()) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            else => return ctx.fail("listing workflows with {s} failed with error.{t}", .{ query, err }),
        }) |execution| switch (execution.status) {
            .completed => completed += 1,
            .running => {},
            else => return ctx.fail("cron run {s} of workflow {s} is {t}, expected completed or running", .{ execution.run_id, started.id, execution.status }),
        };
        if (completed >= 2) return;
    }
    return ctx.fail("expected at least 2 completed runs of cron workflow {s} after {d} listings a second apart, got {d}", .{ started.id, attempts, completed });
}
