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
//!
//! Queries read a workflow's state from outside. Name one with `QueryDef` and answer it with
//! `ctx.setQueryHandler`; the handler runs synchronously and must not change state or block:
//!
//!     pub const Greeting = temporal.workflow.QueryDef("greeting", fn () []const u8);
//!
//!     const State = struct {
//!         greeting: []const u8 = "",
//!         fn current(s: *State) []const u8 {
//!             return s.greeting;
//!         }
//!     };
//!     const state = try ctx.allocator().create(State); // outlives the workflow function
//!     state.* = .{};
//!     try ctx.setQueryHandler(Greeting, state, State.current);

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

/// What canceling an activity's future does: `try_cancel` requests the cancellation and
/// reports it at once, `wait_cancellation_completed` requests it and waits for the activity to
/// end, and `abandon` reports it at once without telling the activity.
pub const ActivityCancellationType = enum { try_cancel, wait_cancellation_completed, abandon };

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
    cancellation_type: ActivityCancellationType = .try_cancel,
    disable_eager_execution: bool = false,
};

/// How a local activity is scheduled. A local activity runs on the worker running the
/// workflow, without a task queue round trip; its result is recorded in the workflow's
/// history as a marker. At least one of `start_to_close_timeout` and
/// `schedule_to_close_timeout` is required: without either, the future's `get` returns
/// `error.InvalidOptions`.
pub const LocalActivityOptions = struct {
    /// Empty uses the activity's sequence number, which changes when a retry waits on a
    /// workflow timer.
    activity_id: []const u8 = "",
    /// How long the workflow waits for the activity, retries included. Null means the
    /// workflow's execution timeout, if any.
    schedule_to_close_timeout: ?Duration = null,
    /// How long the activity may wait for a free local activity slot on the worker. Not
    /// retried.
    schedule_to_start_timeout: ?Duration = null,
    /// How long one attempt may run. Retried.
    start_to_close_timeout: ?Duration = null,
    /// Null retries indefinitely with the default backoff.
    retry_policy: ?RetryPolicy = null,
    /// Retries whose backoff is longer than this wait on a durable workflow timer instead of
    /// on the worker, so the workflow task doesn't stay open. Null means one minute.
    local_retry_threshold: ?Duration = null,
    cancellation_type: ActivityCancellationType = .try_cancel,
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

    /// Schedules a local activity: one the worker running this workflow executes itself, for
    /// short activities where a task queue round trip costs more than the work. The activity
    /// must be registered on this worker. Returns the same future as `executeActivity`.
    /// A retry whose backoff exceeds `local_retry_threshold` waits on a workflow timer;
    /// canceling the future then cancels the retry at once, whatever the cancellation type.
    pub fn executeLocalActivity(ctx: *Context, comptime Activity: type, options: LocalActivityOptions, args: Activity.Args) ActivityFuture(Activity.Result) {
        return .{ .ctx = ctx, .state = ctx.instance.scheduleLocalActivity(Activity.activity_name, options, args) };
    }

    /// Schedules a local activity by its type name, decoding its result as `Result`. `args`
    /// is a tuple of argument values.
    pub fn executeLocalActivityByName(ctx: *Context, comptime Result: type, activity_type: []const u8, options: LocalActivityOptions, args: anytype) ActivityFuture(Result) {
        return .{ .ctx = ctx, .state = ctx.instance.scheduleLocalActivity(activity_type, options, args) };
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

    pub const SetQueryHandlerError = error{
        OutOfMemory,
        /// `context` points into the calling coroutine's stack (logged with the query type).
        QueryContextOnStack,
    };

    /// Answers queries of type `Query` (a `QueryDef`) with `handler(context, args...)`, which
    /// returns `Query.Result` or an error union of it; an error fails the query with the
    /// error's name. Registering the same query type again replaces its handler.
    ///
    /// Handlers run synchronously on the worker thread, outside any coroutine, possibly after
    /// the workflow function has returned. They must only read workflow state: not change it,
    /// not block, and not call into the workflow context. Arguments and the result use
    /// memory that lives until the answer is sent.
    ///
    /// `context` is usually a pointer to the state the handler reads. It must outlive the run,
    /// so allocate it with `ctx.allocator()` (or point to a global); a pointer into the calling
    /// coroutine's stack returns `error.QueryContextOnStack`.
    ///
    ///     const state = try ctx.allocator().create(State);
    ///     try ctx.setQueryHandler(GetStatus, state, State.status);
    pub fn setQueryHandler(ctx: *Context, comptime Query: type, context: anytype, comptime handler: anytype) SetQueryHandlerError!void {
        const C = @TypeOf(context);
        comptime checkQueryHandler(Query, C, @TypeOf(handler));
        const Erased = struct {
            fn call(p: *const anyopaque, out: Allocator, payloads: []const converter.Payload) anyerror!converter.Payload {
                const c: *const C = @ptrCast(@alignCast(p));
                const args = try converter.fromPayloads(Query.Args, out, payloads);
                const result = @call(.auto, handler, .{c.*} ++ args);
                const value = if (@typeInfo(@TypeOf(result)) == .error_union) try result else result;
                return converter.toPayload(out, value);
            }
        };
        const addr: ?usize = switch (@typeInfo(C)) {
            .pointer => |p| switch (p.size) {
                .slice => if (context.len > 0) @intFromPtr(context.ptr) else null,
                else => @intFromPtr(context),
            },
            else => null,
        };
        const stored = try ctx.allocator().create(C);
        stored.* = context;
        return ctx.instance.setQueryHandler(Query.query_name, .{ .context = @ptrCast(stored), .call = Erased.call }, addr);
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

/// `InvalidOptions`: the options were rejected before scheduling (the worker logs why).
pub const ActivityError = error{ ActivityFailed, Canceled, OutOfMemory, InvalidOptions, InvalidPayload, UnknownEncoding, WrongArgumentCount };

/// The pending result of an activity.
pub fn ActivityFuture(comptime T: type) type {
    return struct {
        const Self = @This();

        ctx: *Context,
        /// The scheduling error, if scheduling failed; `get` then returns it.
        state: Instance.ScheduleError!*Instance.ActivityState,

        /// Waits for the activity and returns its result. On `error.ActivityFailed`, read the
        /// failure with `failure`. If the calling coroutine is canceled while waiting (the
        /// workflow is canceled, or an enclosing future, `Select`, or `Group` is), the
        /// activity's cancellation is requested and this returns `error.Canceled`: at once
        /// for `try_cancel` and `abandon`, and once the activity has ended for
        /// `wait_cancellation_completed`. If that activity completes or fails instead of
        /// acknowledging the cancel, this returns its result or `error.ActivityFailed`, and
        /// the next cancelable wait returns `error.Canceled`.
        pub fn get(f: Self) ActivityError!T {
            const state = try f.state;
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
            const state = f.state catch return null;
            const outcome = state.outcome orelse return null;
            return switch (outcome) {
                .completed => null,
                .failed, .cancelled => |fl| fl,
            };
        }

        /// Whether the activity has resolved, so `get` won't block.
        pub fn isReady(f: Self) bool {
            const state = f.state catch return true;
            return state.outcome != null;
        }

        /// Requests cancellation of the activity.
        pub fn cancel(f: Self) void {
            if (f.state) |s| f.ctx.instance.cancelActivity(s) else |_| {}
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

/// Names a query and its signature: `F` is a function type whose parameters are the query's
/// arguments and whose return type is its result. Workflows answer it with
/// `Context.setQueryHandler`; clients send it by the same type.
///
///     pub const Progress = temporal.workflow.QueryDef("progress", fn (step: u32) []const u8);
pub fn QueryDef(comptime name: []const u8, comptime F: type) type {
    if (@typeInfo(F) != .@"fn")
        @compileError("QueryDef(\"" ++ name ++ "\", F): F must be a function type such as `fn (u32) []const u8`, not " ++ @typeName(F));
    const Ret = @typeInfo(F).@"fn".return_type orelse
        @compileError("QueryDef(\"" ++ name ++ "\", F): F must have a concrete return type");
    return struct {
        pub const query_name = name;
        /// The query's arguments, as a tuple.
        pub const Args = std.meta.ArgsTuple(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };
    };
}

/// Compile error unless `H` is `fn (C, Query.Args...) Query.Result` or returns an error union
/// of `Query.Result`.
fn checkQueryHandler(comptime Query: type, comptime C: type, comptime H: type) void {
    const arg_types = @typeInfo(Query.Args).@"struct".field_types;
    var want: []const u8 = "fn (" ++ @typeName(C);
    for (arg_types) |T| want = want ++ ", " ++ @typeName(T);
    want = want ++ ") " ++ @typeName(Query.Result) ++ " (or an error union of it)";
    const bad = "query handler for \"" ++ Query.query_name ++ "\" must be " ++ want ++ ", not " ++ @typeName(H);
    const info = switch (@typeInfo(H)) {
        .@"fn" => |f| f,
        else => @compileError(bad),
    };
    if (info.param_types.len != 1 + arg_types.len) @compileError(bad);
    if (info.param_types[0] != C) @compileError(bad);
    for (arg_types, info.param_types[1..]) |want_t, got| if (got != want_t) @compileError(bad);
    const Ret = info.return_type orelse @compileError(bad);
    const payload = switch (@typeInfo(Ret)) {
        .error_union => |eu| eu.payload,
        else => Ret,
    };
    if (payload != Query.Result) @compileError(bad);
}

test {
    _ = @import("workflow/Scheduler.zig");
    _ = WorkflowIo;
    _ = Instance;
}
