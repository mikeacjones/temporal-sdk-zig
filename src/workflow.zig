//! Workflows (Go `workflow` package parity).
//!
//! A workflow is a function `fn (ctx: *workflow.Context, args...) !Result`, named with `Def`:
//!
//!     fn helloWorld(ctx: *temporal.workflow.Context, name: []const u8) ![]const u8 {
//!         var greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
//!         return greeting.get();
//!     }
//!     pub const HelloWorld = temporal.workflow.Def("Workflow", helloWorld);
//!
//! Workflow code must be deterministic: it is replayed from history. Use `ctx.io()` for
//! concurrency, timers (`io.sleep`), time (`io.now`), and randomness; everything on it is
//! recorded or derived from history. Memory from `ctx.allocator()` lives as long as the
//! workflow run stays cached on the worker.

const std = @import("std");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const proto = @import("proto.zig");
const Instance = @import("workflow/Instance.zig");
const WorkflowIo = @import("workflow/WorkflowIo.zig");
const Allocator = std.mem.Allocator;
const Duration = std.Io.Duration;

pub const Info = struct {
    workflow_id: []const u8,
    run_id: []const u8,
    workflow_type: []const u8,
    task_queue: []const u8,
    namespace: []const u8,
    attempt: u32,
};

pub const RetryPolicy = struct {
    initial_interval: ?Duration = null,
    backoff_coefficient: f64 = 0,
    maximum_interval: ?Duration = null,
    /// 0 means unlimited.
    maximum_attempts: i32 = 0,
    non_retryable_error_types: []const []const u8 = &.{},
};

/// Go `workflow.ActivityOptions`. At least one of `start_to_close_timeout` and
/// `schedule_to_close_timeout` is required.
pub const ActivityOptions = struct {
    /// Empty runs the activity on the workflow's task queue.
    task_queue: []const u8 = "",
    /// Empty uses the activity's sequence number, as other SDKs do.
    activity_id: []const u8 = "",
    schedule_to_close_timeout: ?Duration = null,
    schedule_to_start_timeout: ?Duration = null,
    start_to_close_timeout: ?Duration = null,
    heartbeat_timeout: ?Duration = null,
    retry_policy: ?RetryPolicy = null,
    cancellation_type: enum { try_cancel, wait_cancellation_completed, abandon } = .try_cancel,
    disable_eager_execution: bool = false,
};

pub const Context = struct {
    instance: *Instance,
    /// Set by `fail` and reported instead of the returned error's name.
    pending_failure: ?failure_mod.Application = null,
    /// The last activity failure a future returned; used as the workflow failure's cause when
    /// the workflow returns error.ActivityFailed.
    last_failure: ?*const failure_mod.Failure = null,

    /// The deterministic Io for this workflow: concurrency, timers, time, randomness.
    pub fn io(ctx: *Context) std.Io {
        return ctx.instance.wio.io();
    }

    /// Memory that lives as long as the workflow run is cached.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.instance.arena();
    }

    pub fn info(ctx: *const Context) *const Info {
        return &ctx.instance.info;
    }

    /// A logger that is silent while the workflow replays.
    pub fn logger(ctx: *const Context) log.Logger {
        return ctx.instance.logger;
    }

    pub fn isReplaying(ctx: *const Context) bool {
        return ctx.instance.replaying;
    }

    /// Fails the workflow with a failure you describe. Return the error this gives back.
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{ApplicationFailure} {
        ctx.pending_failure = a;
        return error.ApplicationFailure;
    }

    /// Schedules an activity defined with `temporal.activity.Def` and returns a future for its
    /// result. `args` is a tuple of the activity's parameters after its context.
    pub fn executeActivity(ctx: *Context, comptime Activity: type, options: ActivityOptions, args: Activity.Args) ActivityFuture(Activity.Result) {
        return .{ .ctx = ctx, .state = ctx.instance.scheduleActivity(Activity.activity_name, options, args) };
    }
};

pub const ActivityError = error{ ActivityFailed, Canceled, OutOfMemory, InvalidPayload, UnknownEncoding, WrongArgumentCount };

/// The pending result of an activity. Go `workflow.Future`.
pub fn ActivityFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *Context,
        /// Null if scheduling failed (out of memory); `get` then returns that error.
        state: ?*Instance.ActivityState,

        /// Waits for the activity and returns its result. On `error.ActivityFailed`, read the
        /// failure with `failure`. If the workflow is canceled while waiting, the activity is
        /// canceled too and this returns `error.Canceled`.
        pub fn get(f: Self) ActivityError!T {
            const state = f.state orelse return error.OutOfMemory;
            try f.ctx.instance.awaitActivity(state);
            switch (state.outcome.?) {
                .completed => |maybe| {
                    if (T == void) return;
                    const p = maybe orelse return error.InvalidPayload;
                    return converter.fromPayload(T, f.ctx.allocator(), p);
                },
                .failed, .cancelled => |fl| {
                    f.ctx.last_failure = fl;
                    return error.ActivityFailed;
                },
            }
        }

        /// The activity's failure, after `get` returned `error.ActivityFailed`.
        pub fn failure(f: Self) ?*const failure_mod.Failure {
            const state = f.state orelse return null;
            const outcome = state.outcome orelse return null;
            return switch (outcome) {
                .completed => null,
                .failed, .cancelled => |fl| fl,
            };
        }

        /// Requests cancellation of the activity.
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelActivity(s);
        }
    };
}

/// What a worker registers: a name and a type-erased call that decodes arguments, runs the
/// workflow function, and encodes its result.
pub const Registered = struct {
    name: []const u8,
    invoke: *const fn (ctx: *Context, args: []const converter.Payload) anyerror!converter.Payload,
};

/// Names a workflow function. The result is a type that clients pass to `executeWorkflow` and
/// workers pass to `registerWorkflow`.
pub fn Def(comptime name: []const u8, comptime func: anytype) type {
    const F = @TypeOf(func);
    const fn_info = @typeInfo(F).@"fn";
    if (fn_info.param_types.len == 0 or fn_info.param_types[0] != *Context)
        @compileError("workflow " ++ name ++ " must take *temporal.workflow.Context as its first parameter");
    const Ret = fn_info.return_type.?;
    return struct {
        pub const workflow_name = name;
        pub const function = func;
        pub const Args = @import("activity.zig").ArgsOf(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const args = try converter.fromPayloads(Args, ctx.allocator(), payloads);
            const result = try @call(.auto, func, .{ctx} ++ args);
            return converter.toPayload(ctx.allocator(), result);
        }

        pub const registered: Registered = .{ .name = name, .invoke = invoke };
    };
}

test {
    _ = @import("workflow/Scheduler.zig");
    _ = WorkflowIo;
    _ = Instance;
}
