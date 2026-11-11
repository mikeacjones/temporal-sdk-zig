//! Activities.
//!
//! An activity is a function `fn (ctx: *activity.Context, args...) !Result`, named with
//! `Def` so workflows and workers refer to it by the same type:
//!
//!     pub fn greet(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
//!         ctx.logger().info("Activity", .{ .name = name });
//!         return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
//!     }
//!     pub const Greet = temporal.activity.Def("Activity", greet);
//!
//! Activities run on the worker's real `std.Io` and may do any I/O. Memory from
//! `ctx.allocator()` lives until the activity's result has been reported.
//!
//! A running activity is canceled when its workflow cancels it, its timeout passes, or the
//! worker shuts down. The cancel arrives as `std.Io` cancellation: the activity's next wait
//! on `ctx.io()` returns `error.Canceled`, and `ctx.cancelReason()` says why. Returning
//! `error.Canceled` reports the activity canceled; returning anything else reports that
//! outcome as usual. A cancel from the workflow reaches the worker in the server's answer to
//! a heartbeat, so an activity that should notice it must call `ctx.heartbeat`. Heartbeats
//! are throttled to 80% of the heartbeat timeout (at most 60 seconds; 30 without one), which
//! bounds how late the cancel arrives. A local activity (`info().is_local`) gets its cancels
//! straight from the worker, without heartbeating, and its heartbeats are dropped.
//!
//! Activities that share state (a client, a connection pool, configuration) are methods taking the state first and the context second. The worker binds them to an
//! instance with `registerActivityWith`; workflows call them through the same `Def`:
//!
//!     pub const Activities = struct {
//!         greeting: []const u8,
//!         pub fn getGreeting(a: *Activities, ctx: *temporal.activity.Context) ![]const u8 {
//!             _ = ctx;
//!             return a.greeting;
//!         }
//!     };
//!     pub const GetGreeting = temporal.activity.Def("GetGreeting", Activities.getGreeting);
//!     // worker: try w.registerActivityWith(GetGreeting, &activities);

const std = @import("std");
const converter = @import("converter.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const Allocator = std.mem.Allocator;

pub const Info = struct {
    activity_id: []const u8,
    activity_type: []const u8,
    task_queue: []const u8,
    workflow_id: []const u8,
    workflow_run_id: []const u8,
    workflow_type: []const u8,
    workflow_namespace: []const u8,
    attempt: u32,
    /// The activity runs as a local activity of its workflow, on this worker.
    is_local: bool = false,
};

/// Why an activity was canceled.
pub const CancelReason = enum(u8) {
    /// The server no longer knows the activity (it already completed, timed out, or its
    /// workflow closed).
    not_found,
    /// The workflow requested cancellation.
    canceled,
    /// The activity passed its heartbeat or start-to-close timeout.
    timed_out,
    /// The worker is shutting down and its graceful shutdown period has passed.
    worker_shutdown,
    paused,
    reset,
};

/// An activity's context. The activity uses it from its own task; `cancelReason` is also
/// safe to call from other threads.
pub const Context = struct {
    arena: Allocator,
    io_: std.Io,
    info_: Info,
    logger_: log.Logger,
    /// Set by `fail` and reported instead of the returned error's name.
    pending_failure: ?failure_mod.Application = null,
    /// The previous attempt's last heartbeat details.
    heartbeat_details: []const converter.Payload = &.{},
    /// Where heartbeats go; null drops them (for example outside a worker).
    heartbeat_sink: ?HeartbeatSink = null,
    /// Written once by the worker when a cancel arrives, possibly from another thread.
    cancel_reason_: std.atomic.Value(u8) = .init(not_canceled),

    const not_canceled = std.math.maxInt(u8);

    /// Receives an activity's heartbeats: the worker sends them to the server, and the test
    /// environment records them.
    pub const HeartbeatSink = struct {
        gpa: Allocator,
        context: *anyopaque,
        record: *const fn (context: *anyopaque, details: []const converter.Payload) void,
    };

    /// Memory that lives until the activity's result has been reported.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.arena;
    }

    /// The worker's Io. Activities may block, sleep, and do I/O on it.
    pub fn io(ctx: *Context) std.Io {
        return ctx.io_;
    }

    /// Why the activity was canceled, or null if it hasn't been. Set before the cancel
    /// reaches the activity's `std.Io`, so after a wait returns `error.Canceled` because of a
    /// cancel, this is not null.
    pub fn cancelReason(ctx: *const Context) ?CancelReason {
        const r = ctx.cancel_reason_.load(.acquire);
        return if (r == not_canceled) null else @fromBackingInt(@intCast(r));
    }

    /// Records why the activity is being canceled, before its task is canceled. The first
    /// reason wins; returns whether this call set it. Called by the worker (any thread).
    pub fn setCancelReason(ctx: *Context, reason: CancelReason) bool {
        return ctx.cancel_reason_.cmpxchgStrong(not_canceled, @backingInt(reason), .acq_rel, .acquire) == null;
    }

    pub fn info(ctx: *const Context) *const Info {
        return &ctx.info_;
    }

    pub fn logger(ctx: *const Context) log.Logger {
        return ctx.logger_;
    }

    /// Fails the activity with a failure you describe. Return the error this gives back.
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{ApplicationFailure} {
        ctx.pending_failure = a;
        return error.ApplicationFailure;
    }

    /// Reports that the activity is alive, with progress `details` (any value the converter
    /// encodes, or `{}` for none). A later attempt reads the last details with
    /// `heartbeatDetails`. Heartbeats are throttled by the worker, so calling this often is
    /// cheap. To report several values, pass a struct.
    pub fn heartbeat(ctx: *Context, details: anytype) void {
        const sink = ctx.heartbeat_sink orelse return;
        var scratch: std.heap.ArenaAllocator = .init(sink.gpa);
        defer scratch.deinit();
        if (@TypeOf(details) == void) return sink.record(sink.context, &.{});
        const payload = converter.toPayload(scratch.allocator(), details) catch {
            ctx.logger_.warn("dropping a heartbeat: its details could not be encoded", .{});
            return;
        };
        sink.record(sink.context, &.{payload});
    }

    /// Whether the previous attempt heartbeated with details.
    pub fn hasHeartbeatDetails(ctx: *const Context) bool {
        return ctx.heartbeat_details.len > 0;
    }

    /// The previous attempt's last heartbeat details, decoded as `T` into the activity's
    /// memory.
    pub fn heartbeatDetails(ctx: *Context, comptime T: type) error{ NoHeartbeatDetails, OutOfMemory, InvalidPayload, UnknownEncoding }!T {
        if (ctx.heartbeat_details.len == 0) return error.NoHeartbeatDetails;
        return converter.fromPayload(T, ctx.arena, ctx.heartbeat_details[0]) catch |err| switch (err) {
            error.WrongArgumentCount => error.InvalidPayload,
            else => |e| e,
        };
    }
};

/// What a worker registers: a name and a type-erased call that decodes arguments, runs the
/// function (on `receiver` for activities that are methods), and encodes the result.
pub const Registered = struct {
    name: []const u8,
    invoke: *const fn (receiver: ?*anyopaque, ctx: *Context, args: []const converter.Payload) anyerror!converter.Payload,
    receiver: ?*anyopaque = null,
};

/// Names an activity. `func` is `fn (*Context, args...) !Result`, or a method
/// `fn (*Self, *Context, args...) !Result` that the worker calls on an instance registered
/// with `registerActivityWith`. The result is a type that workflows pass to
/// `executeActivity` and workers pass to `registerActivity`.
pub fn Def(comptime name: []const u8, comptime func: anytype) type {
    const F = @TypeOf(func);
    const params = @typeInfo(F).@"fn".param_types;
    const bound = params.len >= 2 and params[0] != *Context and params[1] == *Context;
    if (!bound and (params.len == 0 or params[0] != *Context))
        @compileError("activity " ++ name ++ " must take *temporal.activity.Context first, or a receiver pointer and then *temporal.activity.Context");
    const Ret = @typeInfo(F).@"fn".return_type.?;
    return struct {
        pub const activity_name = name;
        pub const function = func;
        /// The type activities that are methods are bound to; `void` for plain functions.
        pub const Receiver = if (bound) @typeInfo(params[0].?).pointer.child else void;
        pub const Args = ArgsAfter(F, if (bound) 2 else 1);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(receiver: ?*anyopaque, ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const args = try converter.fromPayloads(Args, ctx.arena, payloads);
            return converter.toPayload(ctx.arena, try call(receiver, ctx, args));
        }

        /// Calls the function directly (receiver ignored for plain functions).
        pub fn call(receiver: ?*anyopaque, ctx: *Context, args: Args) Ret {
            if (bound) {
                const r: *Receiver = @ptrCast(@alignCast(receiver.?));
                return @call(.auto, func, .{ r, ctx } ++ args);
            }
            return @call(.auto, func, .{ctx} ++ args);
        }

        /// The registration for a plain function. Activities that are methods are registered
        /// with `with`.
        pub const registered: Registered = if (bound)
            @compileError("activity " ++ name ++ " is a method; register it with registerActivityWith and an instance")
        else
            .{ .name = name, .invoke = invoke };

        /// The registration binding a method activity to `receiver`, which must outlive the
        /// worker.
        pub fn with(receiver: *Receiver) Registered {
            return .{ .name = name, .invoke = invoke, .receiver = receiver };
        }
    };
}

/// The parameter types after the context, as a tuple type.
pub fn ArgsOf(comptime F: type) type {
    return ArgsAfter(F, 1);
}

fn ArgsAfter(comptime F: type, comptime skip: usize) type {
    const params = @typeInfo(F).@"fn".param_types;
    var types: [params.len - skip]type = undefined;
    for (params[skip..], &types) |p, *t| t.* = p.?;
    return @Tuple(&types);
}

test "Def decodes arguments, calls the function, and encodes the result" {
    const S = struct {
        fn greet(ctx: *Context, name: []const u8) ![]const u8 {
            return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
        }
    };
    const Greet = Def("Activity", S.greet);
    try std.testing.expectEqualStrings("Activity", Greet.activity_name);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    const args = try converter.toPayloads(a, .{@as([]const u8, "World")});
    const out = try Greet.invoke(null, &ctx, args.items);
    try std.testing.expectEqualStrings("\"Hello World!\"", out.data);
}

test "Def binds methods to a receiver" {
    const Activities = struct {
        greeting: []const u8,
        fn say(a: *@This(), ctx: *Context, name: []const u8) ![]const u8 {
            return std.fmt.allocPrint(ctx.allocator(), "{s} {s}!", .{ a.greeting, name });
        }
    };
    const Say = Def("SayGreeting", Activities.say);
    try std.testing.expect(Say.Receiver == Activities);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var impl: Activities = .{ .greeting = "Hello" };
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    const reg = Say.with(&impl);
    const args = try converter.toPayloads(a, .{@as([]const u8, "World")});
    const out = try reg.invoke(reg.receiver, &ctx, args.items);
    try std.testing.expectEqualStrings("\"Hello World!\"", out.data);
}

test "heartbeats reach the sink and details decode" {
    const Sink = struct {
        last: ?[]const u8 = null,
        fn record(p: *anyopaque, details: []const converter.Payload) void {
            const self: *@This() = @ptrCast(@alignCast(p));
            self.last = if (details.len > 0) std.testing.allocator.dupe(u8, details[0].data) catch null else null;
        }
    };
    var sink: Sink = .{};
    defer if (sink.last) |l| std.testing.allocator.free(l);
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const previous = try converter.toPayload(arena.allocator(), @as(i64, 5));
    var ctx: Context = .{
        .arena = arena.allocator(),
        .io_ = std.testing.io,
        .info_ = undefined,
        .logger_ = .{},
        .heartbeat_details = &.{previous},
        .heartbeat_sink = .{ .gpa = std.testing.allocator, .context = &sink, .record = Sink.record },
    };
    try std.testing.expect(ctx.hasHeartbeatDetails());
    try std.testing.expectEqual(@as(i64, 5), try ctx.heartbeatDetails(i64));
    ctx.heartbeat(@as(i64, 6));
    try std.testing.expectEqualStrings("6", sink.last.?);
}

test "a cancel reaches the activity's Io and records the first reason" {
    const S = struct {
        fn sleepForever(ctx: *Context) !void {
            while (true) try ctx.io().sleep(.fromSeconds(60), .awake);
        }
    };
    var ctx: Context = .{ .arena = std.testing.allocator, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    try std.testing.expectEqual(null, ctx.cancelReason());
    var task = try std.testing.io.concurrent(S.sleepForever, .{&ctx});
    try std.testing.expect(ctx.setCancelReason(.timed_out));
    try std.testing.expect(!ctx.setCancelReason(.canceled));
    try std.testing.expectError(error.Canceled, task.cancel(std.testing.io));
    try std.testing.expectEqual(CancelReason.timed_out, ctx.cancelReason().?);
}
