//! Workflows.
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
//!
//! Concurrency in a workflow uses `std.Io`, which `ctx.io()` implements deterministically:
//!
//! - Coroutines: `io.async(fn, args)`, or `std.Io.Group.async` for a set of them.
//! - Waiting for whichever result comes first: `std.Io.Select`.
//! - Channels between coroutines: `std.Io.Queue`; locks: `std.Io.Mutex`, `std.Io.Semaphore`.
//! - Timers: `io.sleep`, or `io.async(std.Io.sleep, ...)` for a timer to race or cancel.
//! - Waiting for workflow state to change: `ctx.await`, `ctx.awaitTimeout`.
//! - Time: `ctx.now()`.
//!
//! Canceling a future cancels what it is waiting on: a pending `io.sleep` cancels its timer and
//! a pending `ActivityFuture.get` requests the activity's cancellation.

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
    /// The cron schedule the workflow was started with, or empty.
    cron_schedule: []const u8 = "",
    /// The first run in this chain of runs (continue-as-new, cron, retries).
    first_run_id: []const u8 = "",
    /// The run this one continued from, or empty.
    continued_run_id: []const u8 = "",
    execution_timeout: ?Duration = null,
    run_timeout: ?Duration = null,
    task_timeout: ?Duration = null,
};

pub const RetryPolicy = struct {
    initial_interval: ?Duration = null,
    backoff_coefficient: f64 = 0,
    maximum_interval: ?Duration = null,
    /// 0 means unlimited.
    maximum_attempts: i32 = 0,
    non_retryable_error_types: []const []const u8 = &.{},
};

/// How an activity is scheduled. At least one of `start_to_close_timeout` and
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

    /// Workflow time: the time of the current workflow task, the same on replay. Equal to
    /// `ctx.io().now(...)`.
    pub fn now(ctx: *const Context) std.Io.Timestamp {
        return .{ .nanoseconds = ctx.instance.now_ns };
    }

    /// Blocks until `condition(state)` returns true.
    ///
    /// `state` is usually a pointer to the variables the condition reads; other coroutines,
    /// activity results, and timers change them. The condition is re-checked every time the
    /// workflow's coroutines have all blocked, so it must be cheap and free of side effects.
    pub fn await(ctx: *Context, state: anytype, comptime condition: fn (@TypeOf(state)) bool) error{ Canceled, OutOfMemory }!void {
        _ = try ctx.awaitInner(state, condition, null);
    }

    /// Like `await`, but gives up after `timeout` (a durable timer) and returns false if the
    /// condition still doesn't hold.
    pub fn awaitTimeout(ctx: *Context, timeout: Duration, state: anytype, comptime condition: fn (@TypeOf(state)) bool) error{ Canceled, OutOfMemory }!bool {
        return ctx.awaitInner(state, condition, timeout);
    }

    fn awaitInner(ctx: *Context, state: anytype, comptime condition: fn (@TypeOf(state)) bool, timeout: ?Duration) error{ Canceled, OutOfMemory }!bool {
        const S = @TypeOf(state);
        const Erased = struct {
            fn check(p: *const anyopaque) bool {
                const s: *const S = @ptrCast(@alignCast(p));
                return condition(s.*);
            }
        };
        return ctx.instance.awaitCondition(Erased.check, @ptrCast(&state), timeout);
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

    /// Schedules an activity by its type name, for activities this program has no `Def` for
    /// (for example ones implemented in another language). `args` is a tuple of argument
    /// values; `Result` is the type to decode the result as.
    pub fn executeActivityByName(ctx: *Context, comptime Result: type, activity_type: []const u8, options: ActivityOptions, args: anytype) ActivityFuture(Result) {
        return .{ .ctx = ctx, .state = ctx.instance.scheduleActivity(activity_type, options, args) };
    }

    /// The workflow's memo: the memo it started with, with this run's `upsertMemo` calls
    /// applied, sorted by name. The slice lives in the run's arena; a later upsert leaves a
    /// slice returned earlier unchanged.
    pub fn memo(ctx: *const Context) []const converter.Field {
        return ctx.instance.memo;
    }

    /// The memo value called `name` decoded as `T` into `ctx.allocator()`, or null if the
    /// memo has no such key.
    pub fn memoValue(ctx: *Context, comptime T: type, name: []const u8) error{ OutOfMemory, InvalidPayload, UnknownEncoding }!?T {
        return converter.fieldValue(T, ctx.allocator(), ctx.instance.memo, name) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }

    /// Adds or replaces memo keys: `values` is a struct literal, one field per key, encoded
    /// like `converter.toFields`. A null value removes the key, and so does an empty slice,
    /// which the server treats the same way. Records the change in history and updates
    /// `memo()` right away, the same way on replay.
    ///
    ///     try ctx.upsertMemo(.{ .status = "shipped", .reviewer = @as(?[]const u8, null) });
    pub fn upsertMemo(ctx: *Context, values: anytype) error{OutOfMemory}!void {
        return ctx.instance.upsertMemo(try converter.toFields(ctx.allocator(), values));
    }

    /// Whether the previous run of this cron (or retried) workflow completed with a result.
    ///
    pub fn hasLastCompletionResult(ctx: *const Context) bool {
        return ctx.instance.last_completion_result != null;
    }

    /// The result of the previous run of this cron (or retried) workflow, decoded as `T`.
    ///
    pub fn lastCompletionResult(ctx: *Context, comptime T: type) error{ NoLastCompletionResult, OutOfMemory, InvalidPayload, UnknownEncoding }!T {
        const p = ctx.instance.last_completion_result orelse return error.NoLastCompletionResult;
        return converter.fromPayload(T, ctx.allocator(), p) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }
};

pub const ActivityError = error{ ActivityFailed, Canceled, OutOfMemory, InvalidPayload, UnknownEncoding, WrongArgumentCount };

/// The pending result of an activity.
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

        /// Whether the activity has resolved, so `get` won't block.
        pub fn isReady(f: Self) bool {
            const state = f.state orelse return true;
            return state.outcome != null;
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
