//! The SDK's client interceptors see each request the client sends (`ClientCall`), not a
//! typed update call, so the interceptor here decodes the update's first argument from the
//! UpdateWorkflowExecution request, adds to it and encodes it again, where Go changes
//! `in.Args[0]`.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const interceptor = temporal.interceptor;
const workflow = temporal.workflow;
const converter = temporal.converter;
const UpdateRequest = temporal.proto.temporal.api.workflowservice.v1.UpdateWorkflowExecutionRequest;

const Workflow = workflow.Def("Workflow", runWorkflow);
const Add = workflow.UpdateDef("add", fn (i64) i64);
const Shutdown = workflow.SignalDef("shutdown_signal", fn () void);
const update_arg = 5;
const add_to_update_arg = 5;

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .configureClient = configureClient,
    .execute = execute,
});

const AddInterceptor = struct {
    pub fn call(_: *AddInterceptor, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        if (input.request(UpdateRequest)) |request| {
            if (request.request) |*update| if (update.input) |*update_input| {
                if (std.mem.eql(u8, update_input.name, Add.update_name)) {
                    if (update_input.args) |*args| if (args.payloads.items.len > 0) {
                        const first = &args.payloads.items[0];
                        // A first argument that isn't an integer is passed through for the
                        // workflow to reject.
                        if (converter.fromPayload(i64, input.arena, first.*)) |value| {
                            first.* = try converter.toPayload(input.arena, value + add_to_update_arg);
                        } else |_| {}
                    };
                }
            };
        }
        return next.call(input);
    }
};

var add_interceptor: AddInterceptor = .{};
const interceptors = [_]interceptor.Client{.from(AddInterceptor, &add_interceptor)};

fn configureClient(options: *temporal.client.Options) void {
    options.interceptors = &interceptors;
}

const State = struct {
    counter: i64 = 0,
    shutdown: bool = false,

    fn add(state: *State, i: i64) i64 {
        state.counter += i;
        return state.counter;
    }

    fn stop(state: *State) void {
        state.shutdown = true;
    }

    fn stopped(state: *State) bool {
        return state.shutdown;
    }
};

fn runWorkflow(ctx: *workflow.Context) !i64 {
    const state = try ctx.allocator().create(State);
    state.* = .{};
    try ctx.setUpdateHandler(Add, state, State.add);
    try ctx.setSignalHandler(Shutdown, state, State.stop);
    try ctx.await(state, State.stopped);
    return state.counter;
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();

    var update = try ctx.client.updateWorkflow(Add, run.id, run.run_id, .{update_arg}, .{});
    defer update.deinit();
    const result = update.get(ctx.arena) catch |err| switch (err) {
        error.UpdateFailed => return ctx.fail("update failed: {s}", .{if (update.failure()) |f| f.message else "no failure details"}),
        else => return err,
    };
    const expected = update_arg + add_to_update_arg;
    try ctx.expect(result == expected, "expected the intercepted update to return {d}, got {d}", .{ expected, result });

    try ctx.client.signalWorkflow(Shutdown, run.id, run.run_id, .{});
    const run_result = try run.get(ctx.arena);
    try ctx.expect(run_result == expected, "expected the workflow to return {d}, got {d}", .{ expected, run_result });
}
