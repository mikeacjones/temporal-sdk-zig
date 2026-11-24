//! Replays generated malformed histories through `WorkflowReplayer`, each in its own replay
//! session followed by the unmodified history, in a process of its own, so that a replay
//! that hangs, aborts or leaks fails the `check replay malformed histories` case (run
//! through time-limit) instead of `zig build test`, and names the history that did it.
//!
//!   replay-malformed [--only <text>] <helloworld_history.json> <history.json>...
//!     Exits 0 when every case that ran passed, 1 when one failed, and 2 on bad arguments
//!     (no file, a first file other than helloworld_history.json, an unknown option,
//!     `--only` without text, or no case matching it). `--only` runs only the cases whose
//!     name contains <text> (a plain, case-sensitive substring).
//!
//! The cases, in this order, are named:
//! - `no-task-queue`, `unfinished-task`, `shifted`, `short`: four hand-made histories from
//!   the first file, which must be helloworld_history.json. Each must come back as
//!   `error.InvalidHistory` with a message naming its problem.
//! - For each file, in argument order, `<file> unmodified` (the file as it is; it must
//!   replay cleanly), then its generated mutations, each kind by ascending k, where `<file>`
//!   is the basename, k the 1-based event position and n the file's event count:
//!   `<file> truncate-after <k>` (k in 1..n-1, keeps events 1..k),
//!   `<file> remove <k>` (1..n), `<file> shift-ids-from <k>` (1..n, adds 1 to the event ID
//!   of event k and every later one), `<file> take-next-id <k>` (1..n-1, gives event k the
//!   ID of event k+1), `<file> clear-task-queue 1`, `<file> clear-workflow-type 1`,
//!   `<file> clear-workflow-execution-timeout 1`, `<file> clear-started-attributes 1` (null
//!   the started event's field or its whole attributes), and
//!   `<file> task-started-past-end <k>` for each workflow task started event k (sets its ID
//!   to the last event's ID + 1). Nothing is random: the set depends only on the files.
//!
//! Each case gets a fresh `std.heap.DebugAllocator`, a `WorkflowReplayer` on it with the
//! workflows of the three test histories registered, and one `replayHistories` call with
//! the case's history (as workflow ID <name>) and then the file unmodified (as
//! `unmodified`). A generated case passes when that call, the replayer's teardown included,
//! returns within 10 s, nothing leaks, the first result is a success or
//! `error.InvalidHistory`, `error.ReplayFailed` or `error.Nondeterminism` with a non-empty
//! message, and the unmodified history after it replays cleanly.
//!
//! It prints, to stderr: `replay-malformed: <name>` before each case, so that the last
//! such line before an abort names the history; `replay-malformed: <name> did not return
//! within 10 s` and exits 1 at once on a hang; `replay-malformed: <name>: leaked memory`,
//! `...: replayHistories failed: <error>`, `...: expected success or InvalidHistory, ...`,
//! or `...: the unmodified history did not replay after it: ...` for the other failures,
//! going on to the next case; and a closing line with the number of generated mutations
//! replayed. A full run with fewer than 100 generated mutations fails.

const std = @import("std");
const builtin = @import("builtin");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;
const History = temporal.history.History;

const usage_text = "usage: replay-malformed [--only <text>] <helloworld_history.json> <history.json>...\n";

/// How long one case, the replayer's teardown included, may take.
const limit_seconds = 10;

/// A full run must generate at least this many mutations, so the generator can't shrink
/// unnoticed.
const min_mutations = 100;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}
const Greet = activity.Def("Greet", greet);

fn hello(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    return greeting.get();
}
const Hello = workflow.Def("HelloWorld", hello);

// The workflow of local_activity_history.json: a copy of `Fixtures.localGreetings` and
// `Fixtures.flakyGreet` in src/testing.zig.

/// Fails its first two attempts, then greets.
fn flakyGreet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    if (ctx.info().attempt < 3) return error.NotYet;
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s} on attempt {d}!", .{ name, ctx.info().attempt });
}
const FlakyGreet = activity.Def("FlakyGreet", flakyGreet);

fn localGreetings(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const first = try ctx.executeLocalActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
    const second = try ctx.executeLocalActivity(FlakyGreet, .{
        .start_to_close_timeout = .fromSeconds(10),
        .retry_policy = .{ .initial_interval = .fromSeconds(1), .backoff_coefficient = 1 },
        .local_retry_threshold = .fromMilliseconds(500),
    }, .{name}).get();
    return std.fmt.allocPrint(ctx.allocator(), "{s} {s}", .{ first, second });
}
const LocalGreetings = workflow.Def("LocalGreetings", localGreetings);

// The workflow of signal_history.json: a copy of `SignalFixtures.collectApprovals` in
// src/testing.zig.

const Approve = workflow.SignalDef("approve", fn (approver: []const u8) void);

const Approvals = struct {
    ctx: *workflow.Context,
    greetings: std.ArrayList([]const u8) = .empty,
    received: usize = 0,

    /// Greets the approver with an activity, so the handler's commands are in history.
    fn approve(s: *Approvals, approver: []const u8) !void {
        s.received += 1;
        const greeting = try s.ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{approver}).get();
        try s.greetings.append(s.ctx.allocator(), greeting);
    }
    fn enough(s: *Approvals) bool {
        return s.received >= 2;
    }
};

/// Sleeps 2 s before registering its handler, so a signal sent at once is buffered; then
/// waits for two approvals and for their handlers to finish.
fn collectApprovals(ctx: *workflow.Context) ![]const u8 {
    const s = try ctx.allocator().create(Approvals);
    s.* = .{ .ctx = ctx };
    try ctx.io().sleep(.fromSeconds(2), .awake);
    try ctx.setSignalHandler(Approve, s, Approvals.approve);
    try ctx.await(s, Approvals.enough);
    try ctx.await(ctx, workflow.Context.allHandlersFinished);
    return std.mem.join(ctx.allocator(), " ", s.greetings.items);
}
const CollectApprovals = workflow.Def("CollectApprovals", collectApprovals);

/// What a case does to its file's history. The names, with `_` as `-`, are the case names'
/// kinds.
const Kind = enum {
    unmodified,
    truncate_after,
    remove,
    shift_ids_from,
    take_next_id,
    clear_task_queue,
    clear_workflow_type,
    clear_workflow_execution_timeout,
    clear_started_attributes,
    task_started_past_end,
};

/// The generated kinds, in run order.
const generated_kinds = [_]Kind{
    .truncate_after,
    .remove,
    .shift_ids_from,
    .take_next_id,
    .clear_task_queue,
    .clear_workflow_type,
    .clear_workflow_execution_timeout,
    .clear_started_attributes,
    .task_started_past_end,
};

const Case = struct {
    name: []const u8,
    /// The file's JSON.
    json: []const u8,
    kind: Kind,
    /// The 1-based event position the kind applies to.
    k: usize,
    /// For a hand-made case, the error and the text its message must contain.
    expected: ?struct { err: WorkflowReplayer.ReplayError, message_part: []const u8 } = null,
};

/// The case the watchdog times, from the moment it was armed. Immutable once published.
const Armed = struct {
    name: []const u8,
    since: std.Io.Timestamp,
};

/// The only state shared with the watchdog thread; null while no case runs.
var armed: std.atomic.Value(?*const Armed) = .init(null);

pub fn main(init: std.process.Init) !u8 {
    const io = init.io;
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);

    var only: ?[]const u8 = null;
    var first_file: usize = 1;
    if (args.len > 1 and std.mem.eql(u8, args[1], "--only")) {
        if (args.len < 3 or args[2].len == 0) return usage();
        only = args[2];
        first_file = 3;
    }
    const paths = args[first_file..];
    if (paths.len == 0) return usage();
    for (paths) |path| if (std.mem.startsWith(u8, path, "--")) return usage();
    if (!std.mem.eql(u8, std.fs.path.basename(paths[0]), "helloworld_history.json")) return usage();

    var cases: std.ArrayList(Case) = .empty;
    var generated: usize = 0;
    for (paths, 0..) |path, i| {
        const json = try std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(16 * 1024 * 1024));
        const history = try temporal.history.fromJson(arena, json);
        const events = history.events.items;
        if (i == 0) {
            if (events.len < 11) {
                std.debug.print("replay-malformed: {s} has {d} events; the hand-made cases need helloworld_history.json's 11\n", .{ path, events.len });
                return 2;
            }
            // Events 3 and 4 are the first workflow task's started and completed events;
            // without 4, started event 3 is followed by an activity event, which the core's
            // `HistoryInfo::new_from_history` rejects.
            // Event ID 2 on the first event makes the core take the task for a sticky-queue
            // one and ask its mocked client, which panics, for the rest.
            // Event 9 is the last workflow task's started event; the history ends at 11, so
            // the core fails its poll and never activates the run.
            try cases.appendSlice(arena, &.{
                .{ .name = "no-task-queue", .json = json, .kind = .clear_task_queue, .k = 1, .expected = .{ .err = error.InvalidHistory, .message_part = "task queue" } },
                .{ .name = "unfinished-task", .json = json, .kind = .remove, .k = 4, .expected = .{ .err = error.InvalidHistory, .message_part = "event 3" } },
                .{ .name = "shifted", .json = json, .kind = .take_next_id, .k = 1, .expected = .{ .err = error.InvalidHistory, .message_part = "first event has event ID 2" } },
                .{ .name = "short", .json = json, .kind = .task_started_past_end, .k = 9, .expected = .{ .err = error.InvalidHistory, .message_part = "before its last workflow task started event, 12" } },
            });
        }
        const file = std.fs.path.basename(path);
        try cases.append(arena, .{ .name = try std.fmt.allocPrint(arena, "{s} unmodified", .{file}), .json = json, .kind = .unmodified, .k = 0 });
        for (generated_kinds) |kind| {
            const n = events.len;
            const first: usize, const last: usize = switch (kind) {
                .truncate_after, .take_next_id => .{ 1, n -| 1 },
                .remove, .shift_ids_from, .task_started_past_end => .{ 1, n },
                .clear_task_queue, .clear_workflow_type, .clear_workflow_execution_timeout, .clear_started_attributes => .{ 1, 1 },
                .unmodified => unreachable,
            };
            var k = first;
            while (k <= last) : (k += 1) {
                if (kind == .task_started_past_end and events[k - 1].event_type != .EVENT_TYPE_WORKFLOW_TASK_STARTED) continue;
                try cases.append(arena, .{
                    .name = try std.fmt.allocPrint(arena, "{s} {s} {d}", .{ file, kindName(arena, kind), k }),
                    .json = json,
                    .kind = kind,
                    .k = k,
                });
                generated += 1;
            }
        }
    }

    const watchdog = try std.Thread.spawn(.{}, watch, .{io});
    watchdog.detach();

    var parse_arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer parse_arena.deinit();
    var ran: usize = 0;
    var ran_generated: usize = 0;
    var failed = false;
    for (cases.items) |case| {
        if (only) |text| if (std.mem.indexOf(u8, case.name, text) == null) continue;
        ran += 1;
        if (case.kind != .unmodified and case.expected == null) ran_generated += 1;
        _ = parse_arena.reset(.retain_capacity);
        if (!try runCase(io, arena, parse_arena.allocator(), case)) failed = true;
    }

    if (only) |text| {
        if (ran == 0) {
            std.debug.print("replay-malformed: no case matches \"{s}\"\n", .{text});
            return 2;
        }
        if (failed) return 1;
        std.debug.print("replay-malformed: replayed {d} cases matching \"{s}\"\n", .{ ran, text });
        return 0;
    }
    if (generated < min_mutations) {
        std.debug.print("replay-malformed: only {d} mutations were generated; expected at least {d}\n", .{ generated, min_mutations });
        return 1;
    }
    if (failed) return 1;
    std.debug.print("replay-malformed: replayed {d} generated mutations of {d} histories and 4 hand-made histories; all returned without a hang, an abort or a leak\n", .{ ran_generated, paths.len });
    return 0;
}

fn usage() u8 {
    std.debug.print(usage_text, .{});
    return 2;
}

fn kindName(arena: std.mem.Allocator, kind: Kind) []const u8 {
    const name = arena.dupe(u8, @tagName(kind)) catch @panic("out of memory");
    std.mem.replaceScalar(u8, name, '_', '-');
    return name;
}

/// Replays one case under the watchdog on an allocator of its own; true when it passed.
fn runCase(io: std.Io, arena: std.mem.Allocator, parse: std.mem.Allocator, case: Case) !bool {
    std.debug.print("replay-malformed: {s}\n", .{case.name});
    var mutated = try temporal.history.fromJson(parse, case.json);
    mutate(&mutated, case.kind, case.k);
    const unmodified = try temporal.history.fromJson(parse, case.json);
    const pair = [2]temporal.history.WorkflowHistory{
        .{ .workflow_id = case.name, .history = mutated },
        .{ .workflow_id = "unmodified", .history = unmodified },
    };
    const histories: []const temporal.history.WorkflowHistory = if (case.kind == .unmodified) pair[0..1] else &pair;

    const record = try arena.create(Armed);
    record.* = .{ .name = case.name, .since = std.Io.Timestamp.now(io, .awake) };
    armed.store(record, .release);

    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    var ok = true;
    {
        var r: WorkflowReplayer = .init(gpa_state.allocator(), io);
        defer r.deinit();
        try r.registerWorkflow(Hello);
        try r.registerWorkflow(LocalGreetings);
        try r.registerWorkflow(CollectApprovals);
        if (r.replayHistories(arena, histories)) |results| {
            ok = check(case, results);
        } else |err| {
            std.debug.print("replay-malformed: {s}: replayHistories failed: {t}\n", .{ case.name, err });
            ok = false;
        }
    }
    armed.store(null, .release);

    if (gpa_state.deinit() == .leak) {
        std.debug.print("replay-malformed: {s}: leaked memory\n", .{case.name});
        ok = false;
    }
    return ok;
}

/// Applies `kind` at 1-based event position `k` to `history` in place.
fn mutate(history: *History, kind: Kind, k: usize) void {
    const events = &history.events;
    switch (kind) {
        .unmodified => {},
        .truncate_after => events.shrinkRetainingCapacity(k),
        .remove => _ = events.orderedRemove(k - 1),
        .shift_ids_from => for (events.items[k - 1 ..]) |*event| {
            event.event_id += 1;
        },
        .take_next_id => events.items[k - 1].event_id = events.items[k].event_id,
        .clear_task_queue => if (startedAttributes(history)) |a| {
            a.task_queue = null;
        },
        .clear_workflow_type => if (startedAttributes(history)) |a| {
            a.workflow_type = null;
        },
        .clear_workflow_execution_timeout => if (startedAttributes(history)) |a| {
            a.workflow_execution_timeout = null;
        },
        .clear_started_attributes => events.items[0].attributes = null,
        .task_started_past_end => events.items[k - 1].event_id = events.items[events.items.len - 1].event_id + 1,
    }
}

fn startedAttributes(history: *History) ?*temporal.proto.temporal.api.history.v1.WorkflowExecutionStartedEventAttributes {
    if (history.events.items.len == 0) return null;
    if (history.events.items[0].attributes) |*attributes| switch (attributes.*) {
        .workflow_execution_started_event_attributes => |*started| return started,
        else => {},
    };
    return null;
}

/// Whether the case's results pass; prints why not.
fn check(case: Case, results: []const WorkflowReplayer.ReplayResult) bool {
    const want: usize = if (case.kind == .unmodified) 1 else 2;
    if (results.len != want) {
        std.debug.print("replay-malformed: {s}: expected {d} results, got {d}\n", .{ case.name, want, results.len });
        return false;
    }
    var ok = true;
    if (case.expected) |expected| {
        ok = expect(results[0], expected.err, expected.message_part);
    } else if (case.kind == .unmodified) {
        if (results[0].err) |err| {
            std.debug.print("replay-malformed: {s}: the unmodified history did not replay: {t}: {s}\n", .{ case.name, err, results[0].message });
            ok = false;
        }
    } else if (results[0].err) |err| switch (err) {
        error.InvalidHistory, error.ReplayFailed, error.Nondeterminism => if (results[0].message.len == 0) {
            std.debug.print("replay-malformed: {s}: expected success or InvalidHistory, ReplayFailed or Nondeterminism with a message, got {t}: {s}\n", .{ case.name, err, results[0].message });
            ok = false;
        },
        else => {
            std.debug.print("replay-malformed: {s}: expected success or InvalidHistory, ReplayFailed or Nondeterminism with a message, got {t}: {s}\n", .{ case.name, err, results[0].message });
            ok = false;
        },
    };
    if (want == 2) if (results[1].err) |err| {
        std.debug.print("replay-malformed: {s}: the unmodified history did not replay after it: {t}: {s}\n", .{ case.name, err, results[1].message });
        ok = false;
    };
    return ok;
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

/// Ends the process with status 1, naming the case, when one has been armed for longer than
/// `limit_seconds`. A replay that never returns can't be recovered in the process.
fn watch(io: std.Io) void {
    while (true) {
        std.Io.sleep(io, .fromMilliseconds(100), .awake) catch {};
        const record = armed.load(.acquire) orelse continue;
        const elapsed = record.since.durationTo(std.Io.Timestamp.now(io, .awake));
        if (elapsed.toSeconds() < limit_seconds) continue;
        std.debug.print("replay-malformed: {s} did not return within {d} s\n", .{ record.name, limit_seconds });
        exitNow(1);
    }
}

/// Exits without running exit handlers: a core thread may have panicked, and they must not
/// get the chance to wait on it.
fn exitNow(status: u8) noreturn {
    if (builtin.link_libc or builtin.os.tag.isDarwin()) std.c._exit(status);
    std.process.exit(status);
}
