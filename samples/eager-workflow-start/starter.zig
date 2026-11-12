//! Eager workflow start: the starter and a worker share one client in one process, so the
//! server can hand the new workflow's first task straight back to that worker in the start
//! response, saving the worker a poll and cutting start latency.
//!
//! This program runs both the worker and the eager start; worker.zig is only a plain worker
//! for the same task queue.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

const task_queue = "eager_wf_start";

fn runWorker(w: *temporal.worker.Worker) temporal.worker.Worker.RunError!void {
    return w.run();
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // 1. Create the shared client.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    // 2. Start the worker before the workflow, and wait until it is registered with the
    // client: an eager start only reaches a worker the client already knows.
    var w = try temporal.worker.Worker.init(gpa, io, &client, task_queue, .{});
    defer w.deinit();
    try w.registerWorkflow(helloworld.HelloWorld);
    try w.registerActivity(helloworld.Greet);
    var running = try io.concurrent(runWorker, .{&w});
    defer {
        w.stop();
        running.await(io) catch {};
    }
    w.waitUntilRunning() catch {
        const err = if (running.await(io)) error.WorkerFailedToStart else |e| e;
        std.log.err("Unable to start worker: {t}", .{err});
        std.process.exit(1);
    };

    // 3. Ask for an eager start, 4. on the same client the worker uses.
    var run = client.executeWorkflow(helloworld.HelloWorld, .{
        .id = try std.fmt.allocPrint(arena, "eager_wf{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = task_queue,
        .enable_eager_start = true,
    }, .{"Temporal"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Started workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });
    std.log.info("Eagerly started: {}", .{run.eagerly_started});

    // 5. Wait for workflow completion.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Workflow result: {s}", .{result});
}
