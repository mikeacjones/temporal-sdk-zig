//! Interceptors: code that runs around a workflow's, an activity's, or a client's calls, to
//! read or change their inputs, headers, and results, or to refuse them.
//!
//! Each interceptor is a runtime interface, a pointer and a table of optional methods. A
//! method takes the call's input and `next`, the rest of the chain: it may change the input,
//! call `next`, and change what comes back. A method left null passes the call through.
//! Build an interface from a struct with `from`, which fills in only the methods the struct
//! declares:
//!
//!     const Audit = struct {
//!         ctx: *temporal.workflow.Context,
//!         pub fn executeActivity(a: *Audit, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) temporal.workflow.ActivityFuture(temporal.converter.Payload) {
//!             a.ctx.logger().info("scheduling", .{ .ActivityType = input.activity_type });
//!             return next.executeActivity(input);
//!         }
//!     };
//!     const audit = try ctx.allocator().create(Audit);
//!     audit.* = .{ .ctx = ctx };
//!     return .{ .outbound = interceptor.WorkflowOutbound.from(Audit, audit) };
//!
//! Workers take `Worker` interceptors (`worker.Options.interceptors`, after the client's
//! `client.Options.worker_interceptors`). For each workflow run and each activity attempt, a
//! `Worker` interceptor hands out the run's interceptors: `WorkflowInbound` around what
//! reaches the workflow (its start, signals, queries, updates), `WorkflowOutbound` around what
//! the workflow sends (activities, children, signals, continue-as-new, its logger), and
//! `ActivityInbound` and `ActivityOutbound` likewise for an activity. The first interceptor in
//! a list is the outermost.
//!
//! Inputs are type-erased, since workflows, activities, and their handlers are typed at
//! compile time: names, `converter.EncodedValues` arguments, a `converter.Header`, and
//! options. Results are encoded payloads.
//!
//! Workflow interceptors run inside the workflow: they must follow the workflow rules (no
//! threads, clocks, randomness, or I/O outside `ctx.io()`), and they run again on replay, where
//! they must make the same calls. Query and update-validation interceptors run outside any
//! coroutine, like the handlers they wrap: they must not block or change state.

const std = @import("std");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const workflow = @import("workflow.zig");
const activity = @import("activity.zig");
const Allocator = std.mem.Allocator;
const Payload = converter.Payload;
const EncodedValues = converter.EncodedValues;
const Header = converter.Header;
const HeaderWriter = converter.HeaderWriter;

// ---------------------------------------------------------------- worker

/// Supplies the interceptors of each workflow run and activity attempt on a worker. Set with
/// `worker.Options.interceptors` or, for every worker on a client,
/// `client.Options.worker_interceptors`. Borrowed: it must outlive the workers.
pub const Worker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Called once per workflow run, when it starts on this worker (or is replayed), before
        /// the workflow function runs. Allocate per-run state with `ctx.allocator()`. Runs as
        /// part of the workflow.
        interceptWorkflow: ?*const fn (ptr: *anyopaque, ctx: *workflow.Context) error{OutOfMemory}!Workflow = null,
        /// Called once per activity attempt, before the activity runs, on the activity's
        /// thread. Allocate per-attempt state with `ctx.allocator()`.
        interceptActivity: ?*const fn (ptr: *anyopaque, ctx: *activity.Context) error{OutOfMemory}!Activity = null,
    };

    pub fn interceptWorkflow(w: Worker, ctx: *workflow.Context) error{OutOfMemory}!Workflow {
        const f = w.vtable.interceptWorkflow orelse return .{};
        return f(w.ptr, ctx);
    }

    pub fn interceptActivity(w: Worker, ctx: *activity.Context) error{OutOfMemory}!Activity {
        const f = w.vtable.interceptActivity orelse return .{};
        return f(w.ptr, ctx);
    }

    /// The `Worker` interceptor of `impl`, from whichever of `interceptWorkflow` and
    /// `interceptActivity` `T` declares, each taking `*T` first.
    pub fn from(comptime T: type, impl: *T) Worker {
        const Erased = struct {
            fn wf(ptr: *anyopaque, ctx: *workflow.Context) error{OutOfMemory}!Workflow {
                return T.interceptWorkflow(@as(*T, @ptrCast(@alignCast(ptr))), ctx);
            }
            fn act(ptr: *anyopaque, ctx: *activity.Context) error{OutOfMemory}!Activity {
                return T.interceptActivity(@as(*T, @ptrCast(@alignCast(ptr))), ctx);
            }
            const vtable: VTable = .{
                .interceptWorkflow = if (@hasDecl(T, "interceptWorkflow")) wf else null,
                .interceptActivity = if (@hasDecl(T, "interceptActivity")) act else null,
            };
        };
        return .{ .ptr = impl, .vtable = &Erased.vtable };
    }
};

/// A workflow run's interceptors; either may be null.
pub const Workflow = struct {
    inbound: ?WorkflowInbound = null,
    outbound: ?WorkflowOutbound = null,
};

/// An activity attempt's interceptors; either may be null.
pub const Activity = struct {
    inbound: ?ActivityInbound = null,
    outbound: ?ActivityOutbound = null,
};

// ---------------------------------------------------------------- workflow inbound

/// The workflow function's start. Arguments and header are arena memory of the run.
pub const ExecuteWorkflowInput = struct {
    workflow_type: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A signal, run inside its handler's coroutine. An interceptor that returns without calling
/// `next` drops the signal; an error fails the workflow as a handler's error would.
pub const HandleSignalInput = struct {
    signal_name: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A query, answered outside any coroutine. `arena` holds the answer until it is sent.
pub const HandleQueryInput = struct {
    query_type: []const u8,
    args: EncodedValues,
    header: Header,
    arena: Allocator,
};

/// An update: validated outside any coroutine (`validateUpdate`, skipped on replay), then run
/// in its handler's coroutine (`executeUpdate`). An error from either rejects the update.
pub const UpdateInput = struct {
    update_name: []const u8,
    update_id: []const u8,
    args: EncodedValues,
    header: Header,
};

/// Wraps what reaches a workflow run. See the namespace documentation.
pub const WorkflowInbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        executeWorkflow: ?*const fn (ptr: *anyopaque, input: ExecuteWorkflowInput, next: Next) anyerror!Payload = null,
        handleSignal: ?*const fn (ptr: *anyopaque, input: HandleSignalInput, next: Next) anyerror!void = null,
        handleQuery: ?*const fn (ptr: *anyopaque, input: HandleQueryInput, next: Next) anyerror!Payload = null,
        validateUpdate: ?*const fn (ptr: *anyopaque, input: UpdateInput, next: Next) anyerror!void = null,
        executeUpdate: ?*const fn (ptr: *anyopaque, input: UpdateInput, next: Next) anyerror!Payload = null,
    };

    /// The interceptors after this one, ending in the workflow itself.
    pub const Next = struct {
        chain: []const WorkflowInbound,
        root: WorkflowInbound,

        pub fn executeWorkflow(n: Next, input: ExecuteWorkflowInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "executeWorkflow", n, input);
        }
        pub fn handleSignal(n: Next, input: HandleSignalInput) anyerror!void {
            return dispatch(WorkflowInbound, "handleSignal", n, input);
        }
        pub fn handleQuery(n: Next, input: HandleQueryInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "handleQuery", n, input);
        }
        pub fn validateUpdate(n: Next, input: UpdateInput) anyerror!void {
            return dispatch(WorkflowInbound, "validateUpdate", n, input);
        }
        pub fn executeUpdate(n: Next, input: UpdateInput) anyerror!Payload {
            return dispatch(WorkflowInbound, "executeUpdate", n, input);
        }
    };

    /// The interceptor of `impl`, from the methods of `VTable` that `T` declares, each as
    /// `fn (*T, Input, Next) Result`.
    pub fn from(comptime T: type, impl: *T) WorkflowInbound {
        return build(WorkflowInbound, T, impl);
    }
};

// ---------------------------------------------------------------- workflow outbound

/// An activity the workflow schedules. `header` holds what the propagators wrote; change it
/// in place. Arguments are arena memory of the run.
pub const ActivityInput = struct {
    activity_type: []const u8,
    options: workflow.ActivityOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A local activity the workflow schedules; as `ActivityInput`.
pub const LocalActivityInput = struct {
    activity_type: []const u8,
    options: workflow.LocalActivityOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A child workflow the workflow starts; as `ActivityInput`.
pub const ChildWorkflowInput = struct {
    workflow_type: []const u8,
    options: workflow.ChildWorkflowOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A signal the workflow sends. `child` is set for `ChildWorkflowFuture.signal`, which
/// reaches only a child of this workflow (`run_id` is then empty).
pub const SignalExternalInput = struct {
    signal_name: []const u8,
    workflow_id: []const u8,
    run_id: []const u8,
    child: bool,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A cancellation request the workflow sends to another workflow.
pub const CancelExternalInput = struct {
    workflow_id: []const u8,
    run_id: []const u8,
};

/// The next run the workflow continues as; as `ActivityInput`.
pub const ContinueAsNewInput = struct {
    workflow_type: []const u8,
    options: workflow.ContinueAsNewOptions,
    args: EncodedValues,
    header: *HeaderWriter,
};

/// A side effect's value, encoded: null on replay, where the value comes from history.
pub const SideEffectInput = struct {
    value: ?Payload,
};

/// A request for the logger workflow or activity code uses.
pub const LoggerInput = struct {};

/// Wraps what a workflow run sends. See the namespace documentation.
///
/// To refuse an activity or child workflow, return `rejectActivity` or
/// `rejectChildWorkflow`: the workflow's future fails with the failure given, and nothing is
/// scheduled.
pub const WorkflowOutbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const ContinueAsNewError = error{ OutOfMemory, InvalidSearchAttribute };

    pub const VTable = struct {
        executeActivity: ?*const fn (ptr: *anyopaque, input: ActivityInput, next: Next) workflow.ActivityFuture(Payload) = null,
        executeLocalActivity: ?*const fn (ptr: *anyopaque, input: LocalActivityInput, next: Next) workflow.ActivityFuture(Payload) = null,
        executeChildWorkflow: ?*const fn (ptr: *anyopaque, input: ChildWorkflowInput, next: Next) workflow.ChildWorkflowFuture(Payload) = null,
        signalExternalWorkflow: ?*const fn (ptr: *anyopaque, input: SignalExternalInput, next: Next) workflow.SignalExternalFuture = null,
        cancelExternalWorkflow: ?*const fn (ptr: *anyopaque, input: CancelExternalInput, next: Next) workflow.CancelExternalFuture = null,
        /// Records the request that the workflow function's return of `error.ContinueAsNew`
        /// sends.
        continueAsNew: ?*const fn (ptr: *anyopaque, input: ContinueAsNewInput, next: Next) ContinueAsNewError!void = null,
        sideEffect: ?*const fn (ptr: *anyopaque, input: SideEffectInput, next: Next) workflow.ActivityFuture(Payload) = null,
        /// The logger `ctx.logger()` returns, called on every `ctx.logger()`. Add fields with
        /// `with` (cache the result: the run's arena grows with each call).
        logger: ?*const fn (ptr: *anyopaque, input: LoggerInput, next: Next) log.Logger = null,
    };

    /// The interceptors after this one, ending in the workflow's commands.
    pub const Next = struct {
        chain: []const WorkflowOutbound,
        root: WorkflowOutbound,

        pub fn executeActivity(n: Next, input: ActivityInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeActivity", n, input);
        }
        pub fn executeLocalActivity(n: Next, input: LocalActivityInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeLocalActivity", n, input);
        }
        pub fn executeChildWorkflow(n: Next, input: ChildWorkflowInput) workflow.ChildWorkflowFuture(Payload) {
            return dispatch(WorkflowOutbound, "executeChildWorkflow", n, input);
        }
        pub fn signalExternalWorkflow(n: Next, input: SignalExternalInput) workflow.SignalExternalFuture {
            return dispatch(WorkflowOutbound, "signalExternalWorkflow", n, input);
        }
        pub fn cancelExternalWorkflow(n: Next, input: CancelExternalInput) workflow.CancelExternalFuture {
            return dispatch(WorkflowOutbound, "cancelExternalWorkflow", n, input);
        }
        pub fn continueAsNew(n: Next, input: ContinueAsNewInput) ContinueAsNewError!void {
            return dispatch(WorkflowOutbound, "continueAsNew", n, input);
        }
        pub fn sideEffect(n: Next, input: SideEffectInput) workflow.ActivityFuture(Payload) {
            return dispatch(WorkflowOutbound, "sideEffect", n, input);
        }
        pub fn logger(n: Next, input: LoggerInput) log.Logger {
            return dispatch(WorkflowOutbound, "logger", n, input);
        }
    };

    /// The interceptor of `impl`, from the methods of `VTable` that `T` declares, each as
    /// `fn (*T, Input, Next) Result`.
    pub fn from(comptime T: type, impl: *T) WorkflowOutbound {
        return build(WorkflowOutbound, T, impl);
    }
};

/// A future for an activity that is never scheduled: `get` returns `error.ActivityFailed` and
/// `failure` describes `f`, for an outbound interceptor refusing `activity_type`.
pub fn rejectActivity(ctx: *workflow.Context, activity_type: []const u8, f: failure_mod.Application) workflow.ActivityFuture(Payload) {
    return .{ .ctx = ctx, .state = ctx.instance.rejectedActivity(activity_type, f) };
}

/// A future for a child workflow that is never started: `started` and `get` return
/// `error.ChildWorkflowFailed` and `failure` describes `f`, for an outbound interceptor
/// refusing `workflow_type`.
pub fn rejectChildWorkflow(ctx: *workflow.Context, workflow_type: []const u8, f: failure_mod.Application) workflow.ChildWorkflowFuture(Payload) {
    return .{ .ctx = ctx, .state = ctx.instance.rejectedChild(workflow_type, f) };
}

// ---------------------------------------------------------------- activities

/// An activity attempt. Arguments and header live as long as the activity's memory.
pub const ExecuteActivityInput = struct {
    activity_type: []const u8,
    args: EncodedValues,
    header: Header,
};

/// A heartbeat's encoded details. Heartbeats the worker drops (a local activity's) don't
/// reach interceptors.
pub const HeartbeatInput = struct {
    details: []const Payload,
};

/// Wraps an activity attempt. See the namespace documentation.
pub const ActivityInbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: ?*const fn (ptr: *anyopaque, input: ExecuteActivityInput, next: Next) anyerror!Payload = null,
    };

    pub const Next = struct {
        chain: []const ActivityInbound,
        root: ActivityInbound,

        pub fn execute(n: Next, input: ExecuteActivityInput) anyerror!Payload {
            return dispatch(ActivityInbound, "execute", n, input);
        }
    };

    pub fn from(comptime T: type, impl: *T) ActivityInbound {
        return build(ActivityInbound, T, impl);
    }
};

/// Wraps what an activity attempt sends. See the namespace documentation.
pub const ActivityOutbound = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// The logger `ctx.logger()` returns, called on every `ctx.logger()`.
        logger: ?*const fn (ptr: *anyopaque, input: LoggerInput, next: Next) log.Logger = null,
        heartbeat: ?*const fn (ptr: *anyopaque, input: HeartbeatInput, next: Next) void = null,
    };

    pub const Next = struct {
        chain: []const ActivityOutbound,
        root: ActivityOutbound,

        pub fn logger(n: Next, input: LoggerInput) log.Logger {
            return dispatch(ActivityOutbound, "logger", n, input);
        }
        pub fn heartbeat(n: Next, input: HeartbeatInput) void {
            return dispatch(ActivityOutbound, "heartbeat", n, input);
        }
    };

    pub fn from(comptime T: type, impl: *T) ActivityOutbound {
        return build(ActivityOutbound, T, impl);
    }
};

// ---------------------------------------------------------------- chains

fn MethodFn(comptime I: type, comptime method: []const u8) type {
    return @typeInfo(@typeInfo(@FieldType(I.VTable, method)).optional.child).pointer.child;
}

fn Return(comptime I: type, comptime method: []const u8) type {
    return @typeInfo(MethodFn(I, method)).@"fn".return_type.?;
}

/// Calls `method` on the first interceptor of `n.chain` that has it, handing it the rest of
/// the chain, or on `n.root` when none does.
fn dispatch(comptime I: type, comptime method: []const u8, n: I.Next, input: anytype) Return(I, method) {
    for (n.chain, 0..) |ic, i| {
        if (@field(ic.vtable, method)) |f| return f(ic.ptr, input, .{ .chain = n.chain[i + 1 ..], .root = n.root });
    }
    return @field(n.root.vtable, method).?(n.root.ptr, input, .{ .chain = &.{}, .root = n.root });
}

/// The interface `I` of `impl`, with a vtable entry for each method of `I.VTable` that `T`
/// declares and null for the rest.
fn build(comptime I: type, comptime T: type, impl: *T) I {
    const Erased = struct {
        const vtable: I.VTable = blk: {
            var vt: I.VTable = .{};
            for (@typeInfo(I.VTable).@"struct".field_names) |name| {
                if (@hasDecl(T, name)) @field(vt, name) = &Method(I, T, name).call;
            }
            break :blk vt;
        };
    };
    return .{ .ptr = impl, .vtable = &Erased.vtable };
}

fn Method(comptime I: type, comptime T: type, comptime name: []const u8) type {
    const info = @typeInfo(MethodFn(I, name)).@"fn";
    const Input = info.param_types[1].?;
    const R = info.return_type.?;
    return struct {
        fn call(ptr: *anyopaque, input: Input, next: I.Next) R {
            const self: *T = @ptrCast(@alignCast(ptr));
            return @field(T, name)(self, input, next);
        }
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "from fills in only the methods a type declares, and calls pass through the rest" {
    const Counting = struct {
        signals: usize = 0,
        pub fn handleSignal(c: *@This(), input: HandleSignalInput, next: WorkflowInbound.Next) anyerror!void {
            c.signals += 1;
            return next.handleSignal(input);
        }
    };
    const Root = struct {
        queries: usize = 0,
        signals: usize = 0,
        fn signal(ptr: *anyopaque, _: HandleSignalInput, _: WorkflowInbound.Next) anyerror!void {
            const r: *@This() = @ptrCast(@alignCast(ptr));
            r.signals += 1;
        }
        fn query(ptr: *anyopaque, input: HandleQueryInput, _: WorkflowInbound.Next) anyerror!Payload {
            const r: *@This() = @ptrCast(@alignCast(ptr));
            r.queries += 1;
            return converter.toPayload(input.arena, r.queries);
        }
    };

    var first: Counting = .{};
    var second: Counting = .{};
    const a = WorkflowInbound.from(Counting, &first);
    try testing.expect(a.vtable.handleSignal != null);
    try testing.expect(a.vtable.executeWorkflow == null);
    try testing.expect(a.vtable.handleQuery == null);
    try testing.expect(a.vtable.validateUpdate == null);
    try testing.expect(a.vtable.executeUpdate == null);

    var root: Root = .{};
    const root_vtable: WorkflowInbound.VTable = .{ .handleSignal = Root.signal, .handleQuery = Root.query };
    const chain = [_]WorkflowInbound{ a, WorkflowInbound.from(Counting, &second) };
    const next: WorkflowInbound.Next = .{ .chain = &chain, .root = .{ .ptr = &root, .vtable = &root_vtable } };

    try next.handleSignal(.{ .signal_name = "s", .args = .{ .payloads = &.{} }, .header = .{} });
    try testing.expectEqual(@as(usize, 1), first.signals);
    try testing.expectEqual(@as(usize, 1), second.signals);
    try testing.expectEqual(@as(usize, 1), root.signals);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const answer = try next.handleQuery(.{ .query_type = "q", .args = .{ .payloads = &.{} }, .header = .{}, .arena = arena.allocator() });
    try testing.expectEqualStrings("1", answer.data);

    const W = struct {
        pub fn interceptActivity(_: *@This(), _: *activity.Context) error{OutOfMemory}!Activity {
            return .{};
        }
    };
    var w: W = .{};
    const worker = Worker.from(W, &w);
    try testing.expect(worker.vtable.interceptWorkflow == null);
    try testing.expect(worker.vtable.interceptActivity != null);
}
