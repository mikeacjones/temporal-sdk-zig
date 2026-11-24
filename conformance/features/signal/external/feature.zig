//! A client outside any workflow signals a running workflow, which returns the signal's
//! value; a signal to a workflow that does not exist returns an error.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const ExternalSignal = workflow.SignalDef("external_signal_channel", fn ([]const u8) void);
const signal_data = "Signaled!";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const channel = try ctx.signalChannel(ExternalSignal);
    return channel.receive();
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.client.signalWorkflow(ExternalSignal, run.id, run.run_id, .{signal_data});
    try ctx.expectEqualStrings(signal_data, try run.get(ctx.arena));

    const missing_id = try std.fmt.allocPrint(ctx.arena, "{s}-missing", .{ctx.task_queue});
    if (ctx.client.signalWorkflow(ExternalSignal, missing_id, "", .{signal_data})) {
        return ctx.fail("signaling the workflow {s}, which does not exist, succeeded; expected error.NotFound", .{missing_id});
    } else |err| switch (err) {
        error.NotFound => {},
        else => return ctx.fail("signaling the workflow {s}, which does not exist, returned error.{s}; expected error.NotFound", .{ missing_id, @errorName(err) }),
    }
}
