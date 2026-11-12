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
//! An activity that finishes elsewhere (a person approves it, another system calls back)
//! returns `ctx.completeAsync()` after handing `ctx.info().task_token` to whatever completes
//! it through the client.
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
//!
//! A dynamic activity runs every activity type the worker has no `Def` registered for, such
//! as types a workflow calls with `executeActivityByName`. Define it with `DynamicDef`,
//! register it with `Worker.registerDynamicActivity` (or `registerDynamicActivityWith` for a
//! method), and read the type from `ctx.info().activity_type` and the arguments from
//! `converter.EncodedValues`:
//!
//!     fn anyActivity(ctx: *temporal.activity.Context, args: temporal.converter.EncodedValues) ![]const u8 {
//!         const name, const count = try args.decode(struct { []const u8, u32 }, ctx.allocator());
//!         return std.fmt.allocPrint(ctx.allocator(), "{s}: {s} x{d}", .{ ctx.info().activity_type, name, count });
//!     }
//!     pub const AnyActivity = temporal.activity.DynamicDef(anyActivity);
//!     // worker: try w.registerDynamicActivity(AnyActivity);

const std = @import("std");
const converter = @import("converter.zig");
const client_mod = @import("client.zig");
const failure_mod = @import("failure.zig");
const log = @import("log.zig");
const propagation = @import("propagation.zig");
const interceptor = @import("interceptor.zig");
const metrics_mod = @import("metrics.zig");
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
    /// Identifies this attempt to the server. Hand a copy to whoever completes the activity
    /// after `completeAsync`; like the other fields, it is freed once the activity's result
    /// has been reported.
    task_token: []const u8 = "",
    /// When the activity was first scheduled. Null when the core didn't send it.
    scheduled_time: ?std.Io.Timestamp = null,
    /// When this attempt was scheduled: after a retry's backoff, later than `scheduled_time`.
    /// Null when the core didn't send it.
    current_attempt_scheduled_time: ?std.Io.Timestamp = null,
    /// When this attempt started, as the server recorded it. Null when the core didn't send
    /// it.
    started_time: ?std.Io.Timestamp = null,
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
    /// The worker's client; null outside a worker unless the test environment sets one.
    client_: ?*client_mod.Client = null,
    /// The activity's header, sorted by name.
    header_: converter.Header = .{},
    /// What the client's propagators extracted from `header_`.
    propagated_: propagation.Values = .empty,
    /// The attempt's outbound interceptors (`interceptor.ActivityOutbound`).
    outbound: []const interceptor.ActivityOutbound = &.{},
    /// Records with the activity's attributes; the worker frees it after the activity.
    metrics_: metrics_mod.Handler = .noop,
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

    /// The client of the worker running the activity, for starting, signaling, querying or
    /// listing workflows from it. The client is safe to use from activities running at the
    /// same time: its calls go through the core connection, which takes concurrent requests,
    /// and otherwise only read the client, whose allocator is shared with the worker and so
    /// must be thread-safe already. Don't `close` it. Returns `error.NoClient` (logged) for
    /// an activity run without one, such as in an `ActivityEnvironment` whose `client` isn't
    /// set.
    pub fn client(ctx: *const Context) error{NoClient}!*client_mod.Client {
        return ctx.client_ orelse {
            ctx.logger_.warn("activity.Context.client: this activity runs without a client; in tests, set ActivityEnvironment.client", .{});
            return error.NoClient;
        };
    }

    pub fn info(ctx: *const Context) *const Info {
        return &ctx.info_;
    }

    /// The header the activity was scheduled with, written by its workflow's (or client's)
    /// interceptors and propagators. Lives as long as `allocator()` memory.
    pub fn headers(ctx: *const Context) converter.Header {
        return ctx.header_;
    }

    /// The values the client's propagators (`client.Options.propagators`) extracted from the
    /// activity's header.
    pub fn propagated(ctx: *const Context) *const propagation.Values {
        return &ctx.propagated_;
    }

    /// Records metrics through the client's exporter (`client.Options.metrics`) with
    /// `namespace`, `task_queue`, `activity_type`, and `workflow_type` attributes, as
    /// sdk-core's activity metrics have them; records nothing without
    /// one. Valid until the activity returns: free the metrics created from it, and handlers
    /// from its `with`, before then. See `temporal.metrics`.
    pub fn metrics(ctx: *const Context) metrics_mod.Handler {
        var h = ctx.metrics_;
        h.owned = false;
        return h;
    }

    /// A logger carrying the activity's ID, type, and attempt and its workflow's type, ID, and
    /// run ID as fields, writing to the worker's sink (`worker.Options.logger`), as the
    /// activity's outbound interceptors (`interceptor.ActivityOutbound.logger`) return it.
    pub fn logger(ctx: *const Context) log.Logger {
        if (ctx.outbound.len == 0) return ctx.logger_;
        return ctx.outboundNext().logger(.{});
    }

    fn outboundNext(ctx: *const Context) interceptor.ActivityOutbound.Next {
        return .{ .chain = ctx.outbound, .root = .{ .ptr = @constCast(ctx), .vtable = &outbound_root } };
    }

    const outbound_root: interceptor.ActivityOutbound.VTable = .{ .logger = rootLogger, .heartbeat = rootHeartbeat };

    fn rootLogger(ptr: *anyopaque, _: interceptor.LoggerInput, _: interceptor.ActivityOutbound.Next) log.Logger {
        const ctx: *const Context = @ptrCast(@alignCast(ptr));
        return ctx.logger_;
    }

    fn rootHeartbeat(ptr: *anyopaque, input: interceptor.HeartbeatInput, _: interceptor.ActivityOutbound.Next) void {
        const ctx: *const Context = @ptrCast(@alignCast(ptr));
        const sink = ctx.heartbeat_sink orelse return;
        sink.record(sink.context, input.details);
    }

    /// Fails the activity with a failure you describe. Return the error this gives back.
    pub fn fail(ctx: *Context, a: failure_mod.Application) error{ApplicationFailure} {
        ctx.pending_failure = a;
        return error.ApplicationFailure;
    }

    /// Ends the activity's function without completing the activity: it stays running on the
    /// server until something completes it with the client (`Client.completeActivity`,
    /// `failActivity`, or `reportActivityCancellation`, by `info().task_token` or by ID), or
    /// until it times out. Return the error this gives back. The worker keeps nothing for
    /// the activity afterwards, so heartbeats and cancellation go through the client
    /// (`Client.recordActivityHeartbeat`). Local activities can't complete asynchronously:
    /// one that tries fails without retrying.
    pub fn completeAsync(ctx: *Context) error{CompleteAsync} {
        _ = ctx;
        return error.CompleteAsync;
    }

    /// Reports that the activity is alive, with progress `details` (any value the converter
    /// encodes, or `{}` for none). A later attempt reads the last details with
    /// `heartbeatDetails`. Heartbeats are throttled by the worker, so calling this often is
    /// cheap. To report several values, pass a struct.
    pub fn heartbeat(ctx: *Context, details: anytype) void {
        const sink = ctx.heartbeat_sink orelse return;
        var scratch: std.heap.ArenaAllocator = .init(sink.gpa);
        defer scratch.deinit();
        if (@TypeOf(details) == void) return ctx.outboundNext().heartbeat(.{ .details = &.{} });
        const payload = converter.toPayload(scratch.allocator(), details) catch {
            ctx.logger_.warn("dropping a heartbeat: its details could not be encoded", .{});
            return;
        };
        ctx.outboundNext().heartbeat(.{ .details = &.{payload} });
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
///
/// Naming rules, enforced when a worker registers the activity:
/// - `name` is the activity type. One function may run under several names; the worker
///   registers and runs each separately. There is no option to forbid this, because each
///   extra name is written in code: a second Def, a pub alias in a struct passed to
///   `Worker.registerActivities` (`pub const legacyCharge = charge;` registers both names),
///   or one struct registered twice under different prefixes. A typical use keeps an old
///   type running for workflows that still schedule it.
/// - A worker holds each name once: registering a name again returns
///   `error.AlreadyRegistered`.
/// - Names starting with "__temporal_" belong to the SDK and return
///   `error.ReservedActivityName`.
/// - `Worker.registerActivityImpl` and `testing.WorkflowEnvironment.registerActivityAs`
///   register another implementation under an existing Def's name, such as a test double.
/// - `defs` names each activity method of a struct as the prefix plus the method name, and
///   `Worker.registerActivities` registers the names of that same `defs` value.
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

/// How `defs` names a struct's activity methods.
pub const StructOptions = struct {
    /// Prepended to each method name to form its activity type, for example "Billing." to
    /// register `charge` as "Billing.charge".
    prefix: []const u8 = "",
};

/// The activity Defs for the activity methods of struct `T`, each named `options.prefix`
/// plus the method name. Workflows call them as fields of the result, and workers register
/// the same value with `Worker.registerActivities`, so both sides use the same names:
///
///     pub const billing = activity.defs(Billing, .{ .prefix = "Billing." });
///     _ = try ctx.executeActivity(billing.charge, options, .{order}).get();
///     try worker.registerActivities(&billing_impl, billing);
///
/// Every pub fn shaped `fn (*T, *Context, args...) !Result` (or taking `*const T`) that
/// returns an error union is an activity method, except the dynamic activity shape
/// `fn (*T, *Context, converter.EncodedValues) !Result`, which is skipped so the struct can
/// also hold its dynamic fallback (register that with `registerDynamicActivityWith`). The
/// arguments and result of an activity method must be convertible by the payload converter;
/// one that isn't is a compile error naming the method and the type, so a helper of that
/// shape, such as `fn (*T, *Context, *std.Io.Writer) !void`, must not be pub. Other pub
/// declarations, such as constants, `init`, functions without a `*Context` second
/// parameter, or functions that can't fail, are skipped. A pub alias of an activity method
/// is an activity under the alias's name too. A struct with no activity method is a compile
/// error.
///
/// No pub declaration of `T` may depend on `defs(T, ...)`: finding `T`'s methods reads the
/// type of each pub declaration of `T`, so such a declaration depends on itself and fails to
/// compile with a dependency loop. That covers a pub `defs(T, ...)` constant inside `T` and a
/// pub declaration reading a non-pub one. Declaring the result outside `T` is always safe. A
/// file used as the struct follows the same rule at its top level.
pub fn defs(comptime T: type, comptime options: StructOptions) MethodDefs(T, options) {
    return .{};
}

/// The type of `defs(T, options)`: one comptime field per activity method, holding its Def.
pub fn MethodDefs(comptime T: type, comptime options: StructOptions) type {
    const names = activityMethodNames(T);
    var attrs: [names.len]std.lang.Type.Struct.FieldAttributes = undefined;
    for (names, &attrs) |method, *attr| {
        const D = Def(options.prefix ++ method, @field(T, method));
        attr.* = .{ .@"comptime" = true, .default_value_ptr = &D };
    }
    return @Struct(.auto, null, names, &@splat(type), &attrs);
}

fn activityMethodNames(comptime T: type) []const []const u8 {
    const info = switch (@typeInfo(T)) {
        .@"struct" => |s| s,
        else => @compileError("activity.defs and registerActivities take a struct type, not " ++ @typeName(T)),
    };
    var names: []const []const u8 = &.{};
    for (info.decl_names) |name| {
        const F = @TypeOf(@field(T, name));
        if (!isActivityMethod(T, F)) continue;
        checkConvertible(T, name, F);
        names = names ++ .{name};
    }
    if (names.len == 0)
        @compileError(@typeName(T) ++ " has no activity methods: give it a pub fn (*" ++ @typeName(T) ++ ", *temporal.activity.Context, args...) !Result");
    const array = names[0..names.len].*;
    return &array;
}

fn isActivityMethod(comptime T: type, comptime F: type) bool {
    const f = switch (@typeInfo(F)) {
        .@"fn" => |f| f,
        else => return false,
    };
    if (f.is_generic or f.param_types.len < 2) return false;
    const fallible = if (f.return_type) |R| @typeInfo(R) == .error_union else false;
    const dynamic = f.param_types.len == 3 and f.param_types[2] == converter.EncodedValues;
    return fallible and !dynamic and (f.param_types[0] == *T or f.param_types[0] == *const T) and f.param_types[1] == *Context;
}

fn checkConvertible(comptime T: type, comptime method: []const u8, comptime F: type) void {
    const f = @typeInfo(F).@"fn";
    const what = @typeName(T) ++ "." ++ method;
    const fix = "; if it is a helper rather than an activity, make it non-pub";
    for (f.param_types[2..], 3..) |P, i| {
        if (converter.unconvertibleType(P.?, .decode)) |bad| @compileError(std.fmt.comptimePrint(
            "activity method {s}: parameter {d} type {s} can't be decoded by the payload converter (because of {s}){s}",
            .{ what, i, @typeName(P.?), @typeName(bad), fix },
        ));
    }
    const R = @typeInfo(f.return_type.?).error_union.payload;
    if (converter.unconvertibleType(R, .encode)) |bad| @compileError(std.fmt.comptimePrint(
        "activity method {s}: result type {s} can't be encoded by the payload converter (because of {s}){s}",
        .{ what, @typeName(R), @typeName(bad), fix },
    ));
}

/// Defines a dynamic activity: one function that runs every activity type the worker has no
/// `Def` registered for. `func` is `fn (*Context, converter.EncodedValues) !Result`, or a
/// method `fn (*Self, *Context, converter.EncodedValues) !Result` bound with
/// `registerDynamicActivityWith`. It reads the type it runs as from `ctx.info().activity_type`
/// and decodes its arguments itself. The result is a type workers pass to
/// `registerDynamicActivity`.
pub fn DynamicDef(comptime func: anytype) type {
    const F = @TypeOf(func);
    const bad = "a dynamic activity must be fn (*temporal.activity.Context, temporal.converter.EncodedValues) !Result, or a method taking a receiver pointer first, not " ++ @typeName(F);
    const params = switch (@typeInfo(F)) {
        .@"fn" => |f| f.param_types,
        else => @compileError(bad),
    };
    const bound = params.len == 3 and params[0] != *Context and params[1] == *Context and params[2] == converter.EncodedValues;
    if (!bound and !(params.len == 2 and params[0] == *Context and params[1] == converter.EncodedValues))
        @compileError(bad);
    const Ret = @typeInfo(F).@"fn".return_type.?;
    return struct {
        pub const function = func;
        /// The type a dynamic activity that is a method is bound to; `void` for a plain
        /// function.
        pub const Receiver = if (bound) @typeInfo(params[0].?).pointer.child else void;
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(receiver: ?*anyopaque, ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            return converter.toPayload(ctx.arena, try call(receiver, ctx, .{ .payloads = payloads }));
        }

        /// Calls the function directly (receiver ignored for plain functions).
        pub fn call(receiver: ?*anyopaque, ctx: *Context, args: converter.EncodedValues) Ret {
            if (bound) {
                const r: *Receiver = @ptrCast(@alignCast(receiver.?));
                return func(r, ctx, args);
            }
            return func(ctx, args);
        }

        /// The registration for a plain function; its name is empty because it answers to
        /// every unregistered type. A method is registered with `with`.
        pub const registered: Registered = if (bound)
            @compileError("this dynamic activity is a method; register it with registerDynamicActivityWith and an instance")
        else
            .{ .name = "", .invoke = invoke };

        /// The registration binding a method to `receiver`, which must outlive the worker.
        pub fn with(receiver: *Receiver) Registered {
            return .{ .name = "", .invoke = invoke, .receiver = receiver };
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

test "defs names each activity method with the prefix and skips other pub declarations" {
    const Billing = struct {
        fee: u32,

        pub const currency = "EUR";
        // Not pub, so finding the activity methods doesn't read it (see `defs`).
        const own = defs(@This(), .{ .prefix = "Own." });

        pub fn init(fee: u32) @This() {
            return .{ .fee = fee };
        }

        pub fn charge(b: *@This(), ctx: *Context, amount: u32) !u32 {
            _ = ctx;
            return amount + b.fee;
        }

        pub fn refund(b: *const @This(), ctx: *Context, amount: u32) !u32 {
            _ = ctx;
            return amount - b.fee;
        }

        pub fn total(b: *@This(), amount: u32) u32 {
            return amount + b.fee;
        }

        pub fn feeFor(b: *@This(), ctx: *Context, amount: u32) u32 {
            _ = ctx;
            return amount * b.fee;
        }

        pub const bill = charge;

        fn audit(b: *@This(), ctx: *Context) !void {
            _ = b;
            _ = ctx;
        }
    };
    const plain = defs(Billing, .{});
    try std.testing.expectEqualStrings("charge", plain.charge.activity_name);
    try std.testing.expectEqualStrings("refund", plain.refund.activity_name);
    try std.testing.expectEqualStrings("bill", plain.bill.activity_name);
    try std.testing.expect(!@hasField(@TypeOf(plain), "feeFor"));
    try std.testing.expectEqual(3, @typeInfo(@TypeOf(plain)).@"struct".field_names.len);
    try std.testing.expectEqualStrings("Own.charge", Billing.own.charge.activity_name);

    const acts = defs(Billing, .{ .prefix = "Billing." });
    try std.testing.expectEqualStrings("Billing.charge", acts.charge.activity_name);
    try std.testing.expectEqualStrings("Billing.refund", acts.refund.activity_name);
    try std.testing.expect(acts.charge.Receiver == Billing);
    try std.testing.expect(acts.refund.Receiver == Billing);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var impl: Billing = .init(5);
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    const args = try converter.toPayloads(a, .{@as(u32, 10)});
    const charged = acts.charge.with(&impl);
    try std.testing.expectEqualStrings("15", (try charged.invoke(charged.receiver, &ctx, args.items)).data);
    const refunded = acts.refund.with(&impl);
    try std.testing.expectEqualStrings("5", (try refunded.invoke(refunded.receiver, &ctx, args.items)).data);
}

test "defs skips the dynamic activity shape and non-pub helpers that take the context" {
    const Reports = struct {
        title: []const u8,

        pub fn render(r: *@This(), ctx: *Context, rows: []const u32) ![]const u8 {
            var out: std.Io.Writer.Allocating = .init(ctx.allocator());
            try r.writeHeader(ctx, &out.writer);
            for (rows) |row| try out.writer.print("{d}\n", .{row});
            return out.written();
        }

        pub fn fallback(r: *@This(), ctx: *Context, args: converter.EncodedValues) ![]const u8 {
            _ = r;
            return std.fmt.allocPrint(ctx.allocator(), "{s} with {d} arguments", .{ ctx.info().activity_type, args.len() });
        }

        fn writeHeader(r: *@This(), ctx: *Context, out: *std.Io.Writer) !void {
            _ = ctx;
            try out.print("{s}\n", .{r.title});
        }
    };
    const acts = defs(Reports, .{ .prefix = "Reports." });
    try std.testing.expectEqual(1, @typeInfo(@TypeOf(acts)).@"struct".field_names.len);
    try std.testing.expectEqualStrings("Reports.render", acts.render.activity_name);
    try std.testing.expect(DynamicDef(Reports.fallback).Receiver == Reports);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var impl: Reports = .{ .title = "Totals" };
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    const args = try converter.toPayloads(a, .{@as([]const u32, &.{ 1, 2 })});
    const rendered = acts.render.with(&impl);
    try std.testing.expectEqualStrings("\"Totals\\n1\\n2\\n\"", (try rendered.invoke(rendered.receiver, &ctx, args.items)).data);
}

test "defs checks methods sharing a large nested record without raising the branch quota" {
    const Address = struct {
        line1: []const u8,
        line2: ?[]const u8 = null,
        city: []const u8,
        region: ?[]const u8 = null,
        postal_code: []const u8,
        country: enum { de, fr, nl, us },
    };
    const Money = struct { amount_minor: i64, currency: [3]u8 };
    const Item = struct {
        sku: []const u8,
        name: []const u8,
        quantity: u32,
        unit_price: Money,
        discount: ?Money = null,
        tags: []const []const u8 = &.{},
    };
    const Order = struct {
        id: []const u8,
        customer_id: []const u8,
        customer_email: []const u8,
        status: enum { draft, placed, paid, shipped, delivered, cancelled },
        billing: Address,
        shipping: Address,
        return_to: ?Address = null,
        items: []const Item,
        subtotal: Money,
        tax: Money,
        shipping_fee: Money,
        total: Money,
        coupon: ?[]const u8 = null,
        notes: ?[]const u8 = null,
        gift: bool = false,
        placed_at_unix_ms: ?i64 = null,
        paid_at_unix_ms: ?i64 = null,
        priority: u8 = 0,
        weight_grams: u32 = 0,
        history: []const struct { status: []const u8, at_unix_ms: i64 } = &.{},
    };
    const Shop = struct {
        pub fn place(s: *@This(), ctx: *Context, o: Order) !Order {
            _ = s;
            _ = ctx;
            return o;
        }
        pub fn pay(s: *@This(), ctx: *Context, o: Order, by: Money) !Order {
            _ = s;
            _ = ctx;
            var paid = o;
            paid.status = .paid;
            paid.total = by;
            return paid;
        }
        pub fn ship(s: *@This(), ctx: *Context, o: Order, to: ?Address) !Order {
            _ = s;
            _ = ctx;
            var shipped = o;
            shipped.status = .shipped;
            if (to) |a| shipped.shipping = a;
            return shipped;
        }
        pub fn cancel(s: *@This(), ctx: *Context, o: Order, reason: []const u8) !Order {
            _ = s;
            _ = ctx;
            _ = reason;
            var cancelled = o;
            cancelled.status = .cancelled;
            return cancelled;
        }
        pub fn lines(s: *@This(), ctx: *Context, orders: []const Order) ![]const Item {
            _ = s;
            _ = ctx;
            return if (orders.len > 0) orders[0].items else &.{};
        }
    };
    const acts = defs(Shop, .{ .prefix = "Shop." });
    try std.testing.expectEqual(5, @typeInfo(@TypeOf(acts)).@"struct".field_names.len);
    try std.testing.expectEqualStrings("Shop.ship", acts.ship.activity_name);

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var impl: Shop = .{};
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = undefined, .logger_ = .{} };
    const home: Address = .{ .line1 = "1 Main St", .city = "Springfield", .postal_code = "12345", .country = .us };
    const zero: Money = .{ .amount_minor = 0, .currency = "USD".* };
    const order: Order = .{
        .id = "o-1",
        .customer_id = "c-1",
        .customer_email = "c@example.com",
        .status = .placed,
        .billing = home,
        .shipping = home,
        .items = &.{.{ .sku = "s-1", .name = "Widget", .quantity = 2, .unit_price = zero }},
        .subtotal = zero,
        .tax = zero,
        .shipping_fee = zero,
        .total = zero,
    };
    const args = try converter.toPayloads(a, .{ order, @as(?Address, null) });
    const shipping = acts.ship.with(&impl);
    const shipped = try converter.fromPayload(Order, a, try shipping.invoke(shipping.receiver, &ctx, args.items));
    try std.testing.expectEqual(.shipped, shipped.status);
    try std.testing.expectEqualStrings("Widget", shipped.items[0].name);
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

test "DynamicDef hands the arguments over undecoded, to a function or a method" {
    const S = struct {
        fn echo(ctx: *Context, args: converter.EncodedValues) ![]const u8 {
            const name = try args.get([]const u8, ctx.allocator(), 0);
            return std.fmt.allocPrint(ctx.allocator(), "{s}: {s} ({d} args)", .{ ctx.info().activity_type, name, args.len() });
        }
        prefix: []const u8,
        fn method(s: *@This(), ctx: *Context, args: converter.EncodedValues) ![]const u8 {
            return std.fmt.allocPrint(ctx.allocator(), "{s}{d}", .{ s.prefix, try args.get(i64, ctx.allocator(), 0) });
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var info: Info = undefined;
    info.activity_type = "Anything";
    var ctx: Context = .{ .arena = a, .io_ = std.testing.io, .info_ = info, .logger_ = .{} };

    const Echo = DynamicDef(S.echo);
    const args = try converter.toPayloads(a, .{ @as([]const u8, "x"), @as(i64, 1) });
    const out = try Echo.registered.invoke(null, &ctx, args.items);
    try std.testing.expectEqualStrings("\"Anything: x (2 args)\"", out.data);

    var impl: S = .{ .prefix = "n=" };
    const reg = DynamicDef(S.method).with(&impl);
    const wrong = try converter.toPayloads(a, .{@as([]const u8, "x")});
    try std.testing.expectError(error.InvalidPayload, reg.invoke(reg.receiver, &ctx, wrong.items));
    const right = try converter.toPayloads(a, .{@as(i64, 7)});
    try std.testing.expectEqualStrings("\"n=7\"", (try reg.invoke(reg.receiver, &ctx, right.items)).data);
}
