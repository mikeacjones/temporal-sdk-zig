//! Activities (Go `activity` package parity).
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
};

pub const Context = struct {
    arena: Allocator,
    io_: std.Io,
    info_: Info,
    logger_: log.Logger,
    /// Set by `fail` and reported instead of the returned error's name.
    pending_failure: ?failure_mod.Application = null,

    /// Memory that lives until the activity's result has been reported.
    pub fn allocator(ctx: *Context) Allocator {
        return ctx.arena;
    }

    /// The worker's Io. Activities may block, sleep, and do I/O on it.
    pub fn io(ctx: *Context) std.Io {
        return ctx.io_;
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
};

/// What a worker registers: a name and a type-erased call that decodes arguments, runs the
/// function, and encodes the result.
pub const Registered = struct {
    name: []const u8,
    invoke: *const fn (ctx: *Context, args: []const converter.Payload) anyerror!converter.Payload,
};

/// Names an activity function. The result is a type that workflows pass to
/// `executeActivity` and workers pass to `registerActivity`.
pub fn Def(comptime name: []const u8, comptime func: anytype) type {
    const F = @TypeOf(func);
    const fn_info = @typeInfo(F).@"fn";
    if (fn_info.param_types.len == 0 or fn_info.param_types[0] != *Context)
        @compileError("activity " ++ name ++ " must take *temporal.activity.Context as its first parameter");
    const Ret = fn_info.return_type.?;
    return struct {
        pub const activity_name = name;
        pub const function = func;
        pub const Args = ArgsOf(F);
        pub const Result = switch (@typeInfo(Ret)) {
            .error_union => |eu| eu.payload,
            else => Ret,
        };

        pub fn invoke(ctx: *Context, payloads: []const converter.Payload) anyerror!converter.Payload {
            const args = try converter.fromPayloads(Args, ctx.arena, payloads);
            const result = try @call(.auto, func, .{ctx} ++ args);
            return converter.toPayload(ctx.arena, result);
        }

        pub const registered: Registered = .{ .name = name, .invoke = invoke };
    };
}

/// The parameter types after the context, as a tuple type.
pub fn ArgsOf(comptime F: type) type {
    const params = @typeInfo(F).@"fn".param_types;
    var types: [params.len - 1]type = undefined;
    for (params[1..], &types) |p, *t| t.* = p.?;
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
    const out = try Greet.invoke(&ctx, args.items);
    try std.testing.expectEqualStrings("\"Hello World!\"", out.data);
}
