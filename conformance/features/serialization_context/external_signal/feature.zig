const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Receiver = workflow.Def("Receiver", runReceiver);
const External = workflow.SignalDef("external", fn ([]const u8) void);

const signal_data = "signaled";
/// Bead of the SDK gap that breaks this feature: a signal to another workflow is encoded with
/// the sender's context, and the receiver decodes it with its own.
const gap = "temporal-sdk-zig-d51.4.32";

pub const feature = harness.define(.{
    .workflows = .{ Workflow, Receiver },
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

// The signal's payload belongs to the target: it is converted with the target's workflow ID.
fn runWorkflow(ctx: *workflow.Context, target_id: []const u8) ![]const u8 {
    try ctx.signalExternalWorkflow(External, target_id, "", .{signal_data}).get();
    return target_id;
}

const Inbox = struct {
    received: ?[]const u8 = null,

    fn receive(inbox: *Inbox, value: []const u8) void {
        inbox.received = value;
    }

    fn hasValue(inbox: *Inbox) bool {
        return inbox.received != null;
    }
};

fn runReceiver(ctx: *workflow.Context) ![]const u8 {
    const inbox = try ctx.allocator().create(Inbox);
    inbox.* = .{};
    try ctx.setSignalHandler(External, inbox, Inbox.receive);
    try ctx.await(inbox, Inbox.hasValue);
    return inbox.received.?;
}

fn execute(ctx: *harness.Context) !void {
    var receiver_options = ctx.startOptions();
    receiver_options.id = try std.fmt.allocPrint(ctx.arena, "{s}_receiver", .{ctx.task_queue});
    var receiver = try ctx.client.executeWorkflow(Receiver, receiver_options, .{});
    defer receiver.deinit();

    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{receiver.id});
    defer run.deinit();
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "sender workflow result", gap);
        if (run.failure()) |failure| return ctx.fail("sender workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(receiver.id, result);

    const expected = try sercontext.workflowSignature(ctx.arena, ctx.namespace, receiver.id);
    const sender_signature = try sercontext.workflowSignature(ctx.arena, ctx.namespace, run.id);
    if (std.mem.eql(u8, expected, sender_signature)) return ctx.fail("receiver and sender share the signature \"{s}\"", .{expected});

    // Checked before awaiting the receiver, which never finishes when it refuses the signal.
    const sent = try ctx.fetchRawHistory(run.id, run.run_id);
    const initiated = try sercontext.findAttributes(ctx, sent, .signal_external_workflow_execution_initiated_event_attributes);
    const signature = sercontext.firstSignature(initiated.input);
    if (signature == null or !std.mem.eql(u8, expected, signature.?)) return ctx.fail(
        "sender's SignalExternalWorkflowExecutionInitiated input: expected context signature \"{s}\", got \"{s}\" (SDK gap: {s})",
        .{ expected, signature orelse "(none)", gap },
    );

    const received = receiver.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "receiver workflow result", gap);
        if (receiver.failure()) |failure| return ctx.fail("receiver workflow failed: {s}", .{failure.message});
        return err;
    };
    try ctx.expectEqualStrings(signal_data, received);
    const receiver_history = try ctx.fetchRawHistory(receiver.id, receiver.run_id);
    const signaled = try sercontext.findAttributes(ctx, receiver_history, .workflow_execution_signaled_event_attributes);
    try sercontext.expectSignature(ctx, "receiver's WorkflowExecutionSignaled input", expected, sercontext.firstSignature(signaled.input));
}
