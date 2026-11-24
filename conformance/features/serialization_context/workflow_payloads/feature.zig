const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const sercontext = harness.sercontext;
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Append = workflow.SignalDef("append", fn ([]const u8) void);
const Prefixed = workflow.QueryDef("prefixed", fn ([]const u8) []const u8);
const Suffixed = workflow.UpdateDef("suffixed", fn ([]const u8) []const u8);

const memo_key = "ser-ctx-memo";
const workflow_input = "input";
const memo_value = "memo";
const query_arg = "query-";
const update_arg = "-update";
const signal_data = "signal";
const side_effect_value = "side";

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .configureClient = sercontext.configureClient,
    .execute = execute,
});

const State = struct {
    ctx: *workflow.Context,
    input: []const u8,
    signaled: ?[]const u8 = null,

    fn prefixed(state: *const State, prefix: []const u8) ![]const u8 {
        return std.fmt.allocPrint(state.ctx.allocator(), "{s}{s}", .{ prefix, state.input });
    }

    fn suffixed(state: *State, suffix: []const u8) ![]const u8 {
        return std.fmt.allocPrint(state.ctx.allocator(), "{s}{s}", .{ state.input, suffix });
    }

    fn append(state: *State, value: []const u8) void {
        state.signaled = value;
    }

    fn hasSignal(state: *State) bool {
        return state.signaled != null;
    }

    fn side(_: void) []const u8 {
        return side_effect_value;
    }
};

fn runWorkflow(ctx: *workflow.Context, input: []const u8) ![]const u8 {
    const state = try ctx.allocator().create(State);
    state.* = .{ .ctx = ctx, .input = input };
    try ctx.setQueryHandler(Prefixed, @as(*const State, state), State.prefixed);
    try ctx.setUpdateHandler(Suffixed, state, State.suffixed);
    try ctx.setSignalHandler(Append, state, State.append);
    const side = try ctx.sideEffect([]const u8, {}, State.side);
    try ctx.await(state, State.hasSignal);
    return std.fmt.allocPrint(ctx.allocator(), "{s}|{s}|{s}", .{ input, side, state.signaled.? });
}

fn execute(ctx: *harness.Context) !void {
    var options = ctx.startOptions();
    options.memo = try temporal.converter.toFields(ctx.arena, .{ .@"ser-ctx-memo" = memo_value });
    var run = try ctx.client.executeWorkflow(Workflow, options, .{workflow_input});
    defer run.deinit();

    const queried = ctx.client.queryWorkflow(Prefixed, ctx.arena, run.id, run.run_id, .{query_arg}) catch |err| {
        try sercontext.checkMismatch(ctx, "query prefixed", null);
        return err;
    };
    try ctx.expectEqualStrings(query_arg ++ workflow_input, queried);

    var update = ctx.client.updateWorkflow(Suffixed, run.id, run.run_id, .{update_arg}, .{}) catch |err| {
        try sercontext.checkMismatch(ctx, "update suffixed", null);
        return err;
    };
    defer update.deinit();
    const updated = update.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "update suffixed", null);
        return err;
    };
    try ctx.expectEqualStrings(workflow_input ++ update_arg, updated);

    try ctx.client.signalWorkflow(Append, run.id, run.run_id, .{signal_data});
    const result = run.get(ctx.arena) catch |err| {
        try sercontext.checkMismatch(ctx, "workflow result", null);
        return err;
    };
    try ctx.expectEqualStrings(workflow_input ++ "|" ++ side_effect_value ++ "|" ++ signal_data, result);

    const recorded = try ctx.fetchRawHistory(run.id, run.run_id);
    const expected = try sercontext.workflowSignature(ctx.arena, ctx.namespace, run.id);

    const started = try sercontext.findAttributes(ctx, recorded, .workflow_execution_started_event_attributes);
    try sercontext.expectSignature(ctx, "WorkflowExecutionStarted input", expected, sercontext.firstSignature(started.input));
    const memo_payload: ?temporal.converter.Payload = blk: {
        const memo = started.memo orelse break :blk null;
        for (memo.fields.items) |field| {
            if (std.mem.eql(u8, field.key, memo_key)) break :blk field.value;
        }
        break :blk null;
    };
    const memo = memo_payload orelse return ctx.fail("WorkflowExecutionStarted has no memo field {s}", .{memo_key});
    try sercontext.expectSignature(ctx, "WorkflowExecutionStarted memo " ++ memo_key, expected, sercontext.signatureOf(memo));

    const completed = try sercontext.findAttributes(ctx, recorded, .workflow_execution_completed_event_attributes);
    try sercontext.expectSignature(ctx, "WorkflowExecutionCompleted result", expected, sercontext.firstSignature(completed.result));

    const signaled = try sercontext.findAttributes(ctx, recorded, .workflow_execution_signaled_event_attributes);
    try sercontext.expectSignature(ctx, "WorkflowExecutionSignaled input", expected, sercontext.firstSignature(signaled.input));

    const accepted = try sercontext.findAttributes(ctx, recorded, .workflow_execution_update_accepted_event_attributes);
    const accepted_args = if (accepted.accepted_request) |request| if (request.input) |input| input.args else null else null;
    try sercontext.expectSignature(ctx, "WorkflowExecutionUpdateAccepted input", expected, sercontext.firstSignature(accepted_args));

    const update_completed = try sercontext.findAttributes(ctx, recorded, .workflow_execution_update_completed_event_attributes);
    const success: ?temporal.converter.Payloads = if (update_completed.outcome) |outcome| if (outcome.value) |value| switch (value) {
        .success => |payloads| payloads,
        .failure => null,
    } else null else null;
    try sercontext.expectSignature(ctx, "WorkflowExecutionUpdateCompleted outcome", expected, sercontext.firstSignature(success));

    // A side effect is recorded as core's local activity marker, not a SideEffect marker.
    const marker = try sercontext.findAttributes(ctx, recorded, .marker_recorded_event_attributes);
    const marker_result: ?temporal.converter.Payloads = for (marker.details.items) |detail| {
        if (std.mem.eql(u8, detail.key, "result")) break detail.value;
    } else null;
    try sercontext.expectSignature(ctx, "side effect marker result", expected, sercontext.firstSignature(marker_result));
}
