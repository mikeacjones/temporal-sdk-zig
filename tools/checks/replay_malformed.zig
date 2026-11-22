//! Replays malformed histories through one `WorkflowReplayer` session and then a valid one,
//! in a process of its own, so that a replay that hangs or aborts fails the
//! `check replay malformed histories` case (run through time-limit) instead of
//! `zig build test`.
//!
//!   replay-malformed <helloworld_history.json>
//!     Exits 0 when both malformed histories come back as `error.InvalidHistory` with a
//!     message naming the problem and the valid history after them replays cleanly; prints
//!     what differed and exits 1 otherwise.
//!
//! The malformed histories are made from the valid one: its started event without a task
//! queue, which the core's replay poller unwraps; its first workflow task started event
//! followed by an activity event, which the core's `HistoryInfo::new_from_history` rejects;
//! a first event ID above 1, for which the core fetches history from its mocked client,
//! which panics; and a last workflow task started event ID past the last event, for which
//! the core fails its poll and never activates the run.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}
const Greet = activity.Def("Greet", greet);

fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return greeting.get();
}
const Hello = workflow.Def("HelloWorld", hello);

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len != 2) {
        std.debug.print("usage: replay-malformed <helloworld_history.json>\n", .{});
        return 2;
    }
    const json = try std.Io.Dir.cwd().readFileAlloc(io, args[1], arena, .limited(16 * 1024 * 1024));

    const no_task_queue = try temporal.history.fromJson(arena, json);
    no_task_queue.events.items[0].attributes.?.workflow_execution_started_event_attributes.task_queue = null;

    // Events 3 and 4 are the first workflow task's started and completed events.
    var unfinished_task = try temporal.history.fromJson(arena, json);
    _ = unfinished_task.events.orderedRemove(3);

    // The core takes this for a sticky-queue task and asks its mocked client for the rest.
    const shifted = try temporal.history.fromJson(arena, json);
    shifted.events.items[0].event_id = 2;

    // Event 9 is the last workflow task's started event; the history ends at event 11.
    const short = try temporal.history.fromJson(arena, json);
    short.events.items[8].event_id = 12;

    const valid = try temporal.history.fromJson(arena, json);

    var r: WorkflowReplayer = .init(gpa, io);
    defer r.deinit();
    try r.registerWorkflow(Hello);
    const histories = [_]temporal.history.WorkflowHistory{
        .{ .workflow_id = "no-task-queue", .history = no_task_queue },
        .{ .workflow_id = "unfinished-task", .history = unfinished_task },
        .{ .workflow_id = "shifted", .history = shifted },
        .{ .workflow_id = "short", .history = short },
        .{ .workflow_id = "valid", .history = valid },
    };
    const results = try r.replayHistories(arena, &histories);

    var ok = true;
    ok = expect(results[0], error.InvalidHistory, "task queue") and ok;
    ok = expect(results[1], error.InvalidHistory, "event 3") and ok;
    ok = expect(results[2], error.InvalidHistory, "first event has event ID 2") and ok;
    ok = expect(results[3], error.InvalidHistory, "before its last workflow task started event, 12") and ok;
    ok = expect(results[4], null, "") and ok;
    return if (ok) 0 else 1;
}

fn expect(result: WorkflowReplayer.ReplayResult, err: ?WorkflowReplayer.ReplayError, message_part: []const u8) bool {
    // Not `result.err == err`: Zig 0.17.1's own x86_64 backend, its default for Debug on
    // x86_64 Linux, compiles `==` between two optional error sets to read a "has a value"
    // byte past each 2-byte operand, which is stack garbage, so equal errors can compare
    // unequal. Comparing the unwrapped errors is compiled correctly.
    const err_matches = if (err) |e| (if (result.err) |got| got == e else false) else result.err == null;
    const matches = err_matches and std.mem.indexOf(u8, result.message, message_part) != null;
    if (!matches) std.debug.print("replay-malformed: {s}: expected {?t} with a message naming \"{s}\", got {?t}: {s}\n", .{ result.workflow_id, err, message_part, result.err, result.message });
    return matches;
}
