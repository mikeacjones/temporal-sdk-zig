//! Feature declarations and the context available to their driver.
const std = @import("std");
const temporal = @import("temporal");

pub const Context = @import("Context.zig");
pub const history = @import("history.zig");

pub const Feature = struct {
    has_worker: bool,
    worker_options: temporal.worker.Options,
    register: *const fn (*temporal.worker.Worker) anyerror!void,
    execute: *const fn (*Context) anyerror!void,
    /// Options may borrow slices from file-scope constants for the client and its worker.
    configureClient: ?*const fn (*temporal.client.Options) void = null,
};

pub fn define(comptime spec: anytype) Feature {
    inline for (@typeInfo(@TypeOf(spec)).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "execute") and
            !std.mem.eql(u8, name, "workflows") and
            !std.mem.eql(u8, name, "activities") and
            !std.mem.eql(u8, name, "worker_options") and
            !std.mem.eql(u8, name, "configureClient"))
        {
            @compileError("unknown feature field ." ++ name ++ "; expected .execute, .workflows, .activities, .worker_options or .configureClient");
        }
    }
    if (!@hasField(@TypeOf(spec), "execute")) @compileError("a feature requires .execute");

    const workflows = if (@hasField(@TypeOf(spec), "workflows")) spec.workflows else .{};
    const activities = if (@hasField(@TypeOf(spec), "activities")) spec.activities else .{};
    const worker_options = comptime blk: {
        var options: temporal.worker.Options = .{};
        if (@hasField(@TypeOf(spec), "worker_options")) {
            const supplied = spec.worker_options;
            if (@TypeOf(supplied) == temporal.worker.Options) break :blk supplied;
            for (@typeInfo(@TypeOf(supplied)).@"struct".field_names) |name| {
                @field(options, name) = @field(supplied, name);
            }
        }
        break :blk options;
    };
    return .{
        .has_worker = workflows.len != 0 or activities.len != 0,
        .worker_options = worker_options,
        .register = struct {
            fn register(worker: *temporal.worker.Worker) anyerror!void {
                inline for (workflows) |Workflow| try worker.registerWorkflow(Workflow);
                inline for (activities) |Activity| try worker.registerActivity(Activity);
            }
        }.register,
        .execute = spec.execute,
        .configureClient = if (@hasField(@TypeOf(spec), "configureClient")) spec.configureClient else null,
    };
}

test {
    _ = Context;
    _ = history;
}

test "conformance define defaults to a client-only feature" {
    const feature = define(.{ .execute = struct {
        fn execute(_: *Context) error{Example}!void {
            return error.Example;
        }
    }.execute });
    try std.testing.expect(!feature.has_worker);
    try std.testing.expectEqual(null, feature.configureClient);
    var ctx: Context = undefined;
    try std.testing.expectError(error.Example, feature.execute(&ctx));
}

test "conformance define retains the client configuration callback" {
    const Example = struct {
        fn execute(_: *Context) !void {}
        fn configure(options: *temporal.client.Options) void {
            options.encode_failure_attributes = true;
        }
    };
    const feature = define(.{ .execute = Example.execute, .configureClient = Example.configure });
    var options: temporal.client.Options = .{};
    feature.configureClient.?(&options);
    try std.testing.expect(options.encode_failure_attributes);
}

test "conformance define accepts worker options written as a struct literal" {
    const execute = struct {
        fn execute(_: *Context) anyerror!void {}
    }.execute;
    const defaults = define(.{ .worker_options = .{}, .execute = execute });
    try std.testing.expectEqual(@as(u32, 1000), defaults.worker_options.max_cached_workflows);
    const feature = define(.{
        .worker_options = .{ .max_cached_workflows = 0, .max_concurrent_workflow_task_pollers = 1 },
        .execute = execute,
    });
    try std.testing.expectEqual(@as(u32, 0), feature.worker_options.max_cached_workflows);
    try std.testing.expectEqual(@as(usize, 1), feature.worker_options.max_concurrent_workflow_task_pollers);
}

test "conformance define preserves typed worker options and defaults for omitted fields" {
    const execute = struct {
        fn execute(_: *Context) anyerror!void {}
    }.execute;
    const options = comptime temporal.worker.Options{ .max_cached_workflows = 0, .graceful_shutdown_period = .fromSeconds(3) };
    const typed = comptime define(.{ .worker_options = options, .execute = execute });
    try std.testing.expectEqualDeep(options, typed.worker_options);
    const literal = comptime define(.{
        .worker_options = .{ .graceful_shutdown_period = std.Io.Duration.fromSeconds(3) },
        .execute = execute,
    });
    try std.testing.expectEqualDeep(temporal.worker.Options{ .graceful_shutdown_period = .fromSeconds(3) }, literal.worker_options);
}
