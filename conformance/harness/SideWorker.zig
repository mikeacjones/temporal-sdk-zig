//! A second worker beside the feature's own, on the same client and task queue, with
//! definitions and options of its own: for example another build of a versioned worker.
//! `start` makes one and `stop` ends and frees it; a feature calls `stop` in a `defer` right
//! after `start`, so it runs before the runner stops the feature's worker and the client.
const SideWorker = @This();

const std = @import("std");
const temporal = @import("temporal");
const Context = @import("Context.zig");
const Io = std.Io;
const Worker = temporal.worker.Worker;

const log = std.log.scoped(.conformance);

worker: Worker,
running: Io.Future(Worker.RunError!void),
gpa: std.mem.Allocator,
io: Io,

/// Starts a second worker on `ctx.client` and `ctx.task_queue`, beside the feature's own,
/// with `options` (with `failure_stack_traces` off) and `definitions`, and returns once it
/// polls. `definitions` is a struct literal with any of `.workflows`, `.activities`,
/// `.nexus_services`, each a tuple as in `harness.define`; another field is a compile error.
/// The caller owns the result and must call `stop`. Every failure but cancellation is
/// reported through `ctx.fail`, with a message starting "side worker: ".
pub fn start(ctx: *Context, options: temporal.worker.Options, comptime definitions: anytype) error{ FeatureFailed, Canceled }!*SideWorker {
    inline for (@typeInfo(@TypeOf(definitions)).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "workflows") and
            !std.mem.eql(u8, name, "activities") and
            !std.mem.eql(u8, name, "nexus_services"))
        {
            @compileError("unknown side worker field ." ++ name ++ "; expected .workflows, .activities or .nexus_services");
        }
    }
    const workflows = if (@hasField(@TypeOf(definitions), "workflows")) definitions.workflows else .{};
    const activities = if (@hasField(@TypeOf(definitions), "activities")) definitions.activities else .{};
    const nexus_services = if (@hasField(@TypeOf(definitions), "nexus_services")) definitions.nexus_services else .{};

    const gpa = ctx.gpa;
    const io = ctx.io;
    var worker_options = options;
    worker_options.failure_stack_traces = false;
    const side = gpa.create(SideWorker) catch |err| return ctx.fail("side worker: allocating returned error.{t}", .{err});
    side.* = .{ .worker = undefined, .running = undefined, .gpa = gpa, .io = io };
    side.worker = Worker.init(gpa, io, ctx.client, ctx.task_queue, worker_options) catch |err| {
        gpa.destroy(side);
        return ctx.fail("side worker: worker init returned error.{t}", .{err});
    };
    const worker = &side.worker;
    var started = false;
    errdefer {
        if (started) {
            worker.stop();
            // The run task must end before the worker is freed, even if this task was canceled.
            const protection = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(protection);
            side.running.await(io) catch {};
        }
        worker.deinit();
        gpa.destroy(side);
    }
    register(worker, workflows, activities, nexus_services) catch |err|
        return ctx.fail("side worker: registering definitions returned error.{t}", .{err});
    side.running = io.concurrent(Worker.run, .{worker}) catch |err|
        return ctx.fail("side worker: starting worker task returned error.{t}", .{err});
    started = true;
    worker.waitUntilRunning() catch |start_err| switch (start_err) {
        error.Canceled => return error.Canceled,
        error.WorkerFailedToStart => {
            started = false;
            worker.stop();
            side.running.await(io) catch |err| {
                const message = worker.fatalMessage();
                return ctx.fail("side worker: worker failed to start with error.{t}{s}{s}", .{ err, if (message == null) "" else ": ", message orelse "" });
            };
            return ctx.fail("side worker: worker failed to start with error.{t}", .{start_err});
        },
    };
    return side;
}

fn register(worker: *Worker, comptime workflows: anytype, comptime activities: anytype, comptime nexus_services: anytype) !void {
    inline for (workflows) |Workflow| try worker.registerWorkflow(Workflow);
    inline for (activities) |Activity| try worker.registerActivity(Activity);
    inline for (nexus_services) |Service| try worker.registerNexusService(Service);
}

/// Stops the worker, waits for its run task, and frees it. Logs a warning if the run
/// ended with an error. Safe to call from a `defer`, also when the task was canceled.
pub fn stop(side: *SideWorker) void {
    const gpa = side.gpa;
    const io = side.io;
    side.worker.stop();
    {
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        side.running.await(io) catch |err| {
            const message = side.worker.fatalMessage();
            log.warn("side worker on task queue {s} ended with error.{t}{s}{s}", .{ side.worker.task_queue, err, if (message == null) "" else ": ", message orelse "" });
        };
    }
    side.worker.deinit();
    gpa.destroy(side);
}

fn testContext(client: *temporal.client.Client, arena: std.mem.Allocator) Context {
    return .{
        .gpa = std.testing.allocator,
        .io = std.testing.io,
        .arena = arena,
        .client = client,
        .feature = "test/side",
        .namespace = "default",
        .task_queue = "conformance-test/side",
    };
}

test "conformance SideWorker frees the worker when it fails to start" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var client = try temporal.client.Client.init(gpa, std.testing.io, .{});
    defer client.close();
    var ctx = testContext(&client, arena.allocator());
    const run = struct {
        fn run(_: *temporal.workflow.Context) !void {}
    }.run;
    // Option validation fails when the worker runs, before the lazy client connects.
    try std.testing.expectError(error.FeatureFailed, start(&ctx, .{ .max_activities_per_second = 0 }, .{
        .workflows = .{temporal.workflow.Def("Workflow", run)},
    }));
    try std.testing.expectEqualStrings("side worker: worker failed to start with error.InvalidOptions", ctx.failure_message_.?);
}

test "conformance SideWorker reports a worker with nothing registered" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var client = try temporal.client.Client.init(gpa, std.testing.io, .{});
    defer client.close();
    var ctx = testContext(&client, arena.allocator());
    try std.testing.expectError(error.FeatureFailed, start(&ctx, .{}, .{}));
    try std.testing.expectEqualStrings("side worker: worker failed to start with error.NothingRegistered", ctx.failure_message_.?);
}

test "conformance SideWorker refuses a versioned workflow without a behavior" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    var client = try temporal.client.Client.init(gpa, std.testing.io, .{});
    defer client.close();
    var ctx = testContext(&client, arena.allocator());
    const run = struct {
        fn run(_: *temporal.workflow.Context) !void {}
    }.run;
    try std.testing.expectError(error.FeatureFailed, start(&ctx, .{ .deployment = .{ .deployment_name = "side", .build_id = "2.0" } }, .{
        .workflows = .{temporal.workflow.Def("Workflow", run)},
    }));
    try std.testing.expectEqualStrings("side worker: worker failed to start with error.InvalidOptions", ctx.failure_message_.?);
}
