//! Feature declarations and the context available to their driver.
const std = @import("std");
const temporal = @import("temporal");

pub const Context = @import("Context.zig");
pub const history = @import("history.zig");
pub const stored = @import("stored.zig");
pub const sercontext = @import("sercontext.zig");
pub const SideWorker = @import("SideWorker.zig");
pub const deployments = @import("deployments.zig");

pub const Feature = struct {
    has_worker: bool,
    worker_options: temporal.worker.Options,
    /// Workflow type names in declaration order.
    workflow_types: []const []const u8,
    register: *const fn (*temporal.worker.Worker) anyerror!void,
    /// Registers the workflows of `.workflows` with a replayer.
    registerReplayer: *const fn (*temporal.testing.WorkflowReplayer) anyerror!void,
    execute: *const fn (*Context) anyerror!void,
    /// Options may borrow slices from file-scope constants for the client and its worker.
    configureClient: ?*const fn (*temporal.client.Options) void = null,
    /// Extra `temporal server start-dev` arguments. A feature that sets them runs on a dev
    /// server of its own instead of the shared one. Borrowed from file-scope constants.
    dev_server_args: []const []const u8 = &.{},

    /// The options every worker of this feature runs with: `worker_options` with
    /// `failure_stack_traces` off, so no absolute source path reaches a recorded history.
    pub fn workerOptions(feature: *const Feature) temporal.worker.Options {
        var options = feature.worker_options;
        options.failure_stack_traces = false;
        return options;
    }

    /// Replayer options for a stored run of this feature: the given namespace and task queue,
    /// and the codecs and failure converter `.configureClient` sets on a default
    /// `temporal.client.Options`. The slices borrow what `configureClient` borrowed.
    pub fn replayerOptions(feature: *const Feature, namespace: []const u8, task_queue: []const u8) temporal.testing.WorkflowReplayer.Options {
        var client_options: temporal.client.Options = .{};
        if (feature.configureClient) |configure| configure(&client_options);
        return .{
            .namespace = namespace,
            .task_queue = task_queue,
            .codecs = client_options.codecs,
            .failure_converter = client_options.failure_converter,
        };
    }
};

pub fn define(comptime spec: anytype) Feature {
    inline for (@typeInfo(@TypeOf(spec)).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "execute") and
            !std.mem.eql(u8, name, "workflows") and
            !std.mem.eql(u8, name, "activities") and
            !std.mem.eql(u8, name, "nexus_services") and
            !std.mem.eql(u8, name, "worker_options") and
            !std.mem.eql(u8, name, "configureClient") and
            !std.mem.eql(u8, name, "dev_server_args"))
        {
            @compileError("unknown feature field ." ++ name ++ "; expected .execute, .workflows, .activities, .nexus_services, .worker_options, .configureClient or .dev_server_args");
        }
    }
    if (!@hasField(@TypeOf(spec), "execute")) @compileError("a feature requires .execute");

    const workflows = if (@hasField(@TypeOf(spec), "workflows")) spec.workflows else .{};
    const activities = if (@hasField(@TypeOf(spec), "activities")) spec.activities else .{};
    const nexus_services = if (@hasField(@TypeOf(spec), "nexus_services")) spec.nexus_services else .{};
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
        .has_worker = workflows.len != 0 or activities.len != 0 or nexus_services.len != 0,
        .worker_options = worker_options,
        .workflow_types = comptime blk: {
            var names: [workflows.len][]const u8 = undefined;
            for (workflows, &names) |Workflow, *name| name.* = Workflow.workflow_name;
            const result = names;
            break :blk &result;
        },
        .register = struct {
            fn register(worker: *temporal.worker.Worker) anyerror!void {
                inline for (workflows) |Workflow| try worker.registerWorkflow(Workflow);
                inline for (activities) |Activity| try worker.registerActivity(Activity);
                inline for (nexus_services) |Service| try worker.registerNexusService(Service);
            }
        }.register,
        .registerReplayer = struct {
            fn registerReplayer(replayer: *temporal.testing.WorkflowReplayer) anyerror!void {
                inline for (workflows) |Workflow| try replayer.registerWorkflow(Workflow);
            }
        }.registerReplayer,
        .execute = spec.execute,
        .configureClient = if (@hasField(@TypeOf(spec), "configureClient")) spec.configureClient else null,
        .dev_server_args = if (@hasField(@TypeOf(spec), "dev_server_args")) spec.dev_server_args else &.{},
    };
}

test {
    _ = Context;
    _ = history;
    _ = stored;
    _ = sercontext;
    _ = SideWorker;
    _ = deployments;
}

test "conformance define defaults to a client-only feature" {
    const feature = define(.{ .execute = struct {
        fn execute(_: *Context) error{Example}!void {
            return error.Example;
        }
    }.execute });
    try std.testing.expect(!feature.has_worker);
    try std.testing.expectEqual(0, feature.workflow_types.len);
    try std.testing.expectEqual(null, feature.configureClient);
    var ctx: Context = undefined;
    try std.testing.expectError(error.Example, feature.execute(&ctx));
}

test "conformance define lists workflow types in declaration order" {
    const Example = struct {
        fn run(_: *temporal.workflow.Context) !void {}
        fn execute(_: *Context) !void {}
    };
    const feature = define(.{
        .workflows = .{ temporal.workflow.Def("Parent", Example.run), temporal.workflow.Def("Child", Example.run) },
        .execute = Example.execute,
    });
    try std.testing.expectEqualDeep(&[_][]const u8{ "Parent", "Child" }, feature.workflow_types);
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

test "conformance define retains the dev server arguments" {
    const execute = struct {
        fn execute(_: *Context) !void {}
    }.execute;
    const plain = define(.{ .execute = execute });
    try std.testing.expectEqual(0, plain.dev_server_args.len);
    const feature = define(.{ .execute = execute, .dev_server_args = &temporal.testing.DevServer.standalone_activity_args });
    try std.testing.expectEqual(&temporal.testing.DevServer.standalone_activity_args[0], &feature.dev_server_args[0]);
    try std.testing.expectEqual(temporal.testing.DevServer.standalone_activity_args.len, feature.dev_server_args.len);
}

test "conformance worker options turn failure stack traces off" {
    const execute = struct {
        fn execute(_: *Context) !void {}
    }.execute;
    const feature = define(.{ .worker_options = .{ .failure_stack_traces = true, .max_cached_workflows = 3 }, .execute = execute });
    const options = feature.workerOptions();
    try std.testing.expect(!options.failure_stack_traces);
    try std.testing.expectEqual(@as(u32, 3), options.max_cached_workflows);
}

test "conformance replayer options carry the codecs set by configureClient" {
    const Example = struct {
        var context: u8 = 0;
        const codecs = [_]temporal.converter.PayloadCodec{.{ .ptr = &context, .vtable = &.{ .encode = passThrough, .decode = passThrough } }};
        fn passThrough(_: *anyopaque, _: std.mem.Allocator, _: std.Io, payloads: []const temporal.converter.Payload) temporal.converter.CodecError![]temporal.converter.Payload {
            return @constCast(payloads);
        }
        fn execute(_: *Context) !void {}
        fn configure(options: *temporal.client.Options) void {
            options.codecs = &codecs;
        }
    };
    const feature = define(.{ .execute = Example.execute, .configureClient = Example.configure });
    const options = feature.replayerOptions("ns", "queue");
    try std.testing.expectEqualStrings("ns", options.namespace);
    try std.testing.expectEqualStrings("queue", options.task_queue);
    try std.testing.expectEqual(1, options.codecs.len);
    try std.testing.expectEqual(&Example.codecs[0], &options.codecs[0]);
    try std.testing.expectEqual(null, options.failure_converter);
}

test "conformance replayer options default without configureClient" {
    const feature = define(.{ .execute = struct {
        fn execute(_: *Context) !void {}
    }.execute });
    const options = feature.replayerOptions("default", "conformance-a/b");
    try std.testing.expectEqualDeep(temporal.testing.WorkflowReplayer.Options{ .namespace = "default", .task_queue = "conformance-a/b" }, options);
}

test "conformance registerReplayer registers each declared workflow type" {
    const Example = struct {
        fn run(_: *temporal.workflow.Context) !void {}
        fn execute(_: *Context) !void {}
    };
    const feature = define(.{
        .workflows = .{ temporal.workflow.Def("Parent", Example.run), temporal.workflow.Def("Child", Example.run) },
        .execute = Example.execute,
    });
    var replayer: temporal.testing.WorkflowReplayer = .init(std.testing.allocator, std.testing.io);
    defer replayer.deinit();
    try feature.registerReplayer(&replayer);
    try std.testing.expectEqual(2, replayer.workflows.count());
    try std.testing.expect(replayer.workflows.contains("Parent"));
    try std.testing.expect(replayer.workflows.contains("Child"));
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

test "conformance define requires a worker for a Nexus-only feature" {
    const nexus = temporal.nexus;
    const Operation = nexus.OperationDef("echo", []const u8, []const u8);
    const Service = nexus.ServiceDef("test-service", .{Operation});
    const functions = struct {
        fn echo(_: *nexus.Context, input: []const u8) ![]const u8 {
            return input;
        }

        fn execute(_: *Context) !void {}
    };
    const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Operation, functions.echo)});
    const feature = define(.{ .nexus_services = .{Handler}, .execute = functions.execute });
    try std.testing.expect(feature.has_worker);
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
