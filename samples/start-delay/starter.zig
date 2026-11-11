//! Start delay: start the hello world workflow with a start delay, so its first workflow task
//! runs only after the delay.
//!
//! Usage: starter [delay-seconds]. The delay defaults to 5 minutes; samples-test passes a few
//! seconds and checks that the workflow didn't finish before the delay passed.

const std = @import("std");
const temporal = @import("temporal");
const helloworld = @import("helloworld");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const delay_seconds: i64 = if (args.next()) |a| std.fmt.parseInt(i64, a, 10) catch {
        std.log.err("delay-seconds must be a whole number of seconds, got \"{s}\"", .{a});
        std.process.exit(2);
    } else 5 * 60;

    // The client is a heavyweight object that should be created once per process.
    const options = try temporal.client.loadDefaultOptions(arena);
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client: {t}", .{err});
        std.process.exit(1);
    };
    defer client.close();

    const started = std.Io.Timestamp.now(io, .real);
    var run = client.executeWorkflow(helloworld.HelloWorld, .{
        .id = try std.fmt.allocPrint(arena, "start-delay-{s}", .{try temporal.client.uuid(arena, io)}),
        .task_queue = "start-delay",
        .start_delay = .fromSeconds(delay_seconds),
    }, .{"from a delayed workflow"}) catch |err| {
        std.log.err("Unable to execute workflow: {t}", .{err});
        std.process.exit(1);
    };
    defer run.deinit();
    std.log.info("Scheduled workflow WorkflowID={s} RunID={s}", .{ run.id, run.run_id });

    // Wait for the result to check that the delay was honored.
    const result = run.get(arena) catch |err| {
        std.log.err("Unable get workflow result: {t}", .{err});
        std.process.exit(1);
    };
    const elapsed = started.durationTo(std.Io.Timestamp.now(io, .real));
    if (elapsed.nanoseconds < @as(i96, delay_seconds) * std.time.ns_per_s) {
        std.log.err("the workflow finished after {d} ms, before its {d} s start delay", .{ @divTrunc(elapsed.nanoseconds, std.time.ns_per_ms), delay_seconds });
        std.process.exit(1);
    }
    std.log.info("Workflow result: {s}", .{result});
}
