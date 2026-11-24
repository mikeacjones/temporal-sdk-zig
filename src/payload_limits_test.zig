//! Payload size limits against a dev server: a worker fails a workflow task whose result is
//! larger than the namespace's limit (`worker.Options.fail_oversized_payloads`), or sends it
//! for the server to reject when the check is off; and a client logs one warning for each
//! payload or memo of a request over its warning size (`client.Options.payloads_warning_size`,
//! `memo_warning_size`) and still sends the request.

const std = @import("std");
const client_mod = @import("client.zig");
const worker_mod = @import("worker.zig");
const workflow = @import("workflow.zig");
const converter = @import("converter.zig");
const log_mod = @import("log.zig");
const proto = @import("proto.zig");
const DevServer = @import("testing.zig").DevServer;
const Client = client_mod.Client;
const Io = std.Io;

const testing = std.testing;

/// Over the dev server's default payload size limit (2 MiB), under gRPC's 4 MiB message limit.
const oversized_result = 3 * 1024 * 1024;

const Fixtures = struct {
    /// Returns `size` bytes of text.
    fn big(ctx: *workflow.Context, size: u32) ![]const u8 {
        const out = try ctx.allocator().alloc(u8, size);
        @memset(out, 'x');
        return out;
    }
    const Big = workflow.Def("PayloadLimitsBig", big);

    /// Never runs: the client test only starts it.
    fn idle(_: *workflow.Context, _: []const u8) !void {}
    const Idle = workflow.Def("PayloadLimitsIdle", idle);

    fn runWorker(w: *worker_mod.Worker) worker_mod.Worker.RunError!void {
        return w.run();
    }
};

/// Keeps the records of sdk-core's payload limit check (messages starting "[TMPRL1103]"),
/// with their level and their `size` and `limit` fields.
const LimitRecords = struct {
    mutex: Io.Mutex = .init,
    /// Broadcast for each record kept.
    changed: Io.Condition = .init,
    records: [max]Record = undefined,
    count: usize = 0,

    const max = 32;

    const Record = struct {
        level: log_mod.Level,
        /// Whether the message names a memo (else payloads).
        memo: bool,
        /// Whether the message names the error limit (else the warning limit).
        error_limit: bool,
        size: ?u64 = null,
        limit: ?u64 = null,
    };

    fn sink(r: *LimitRecords) log_mod.Sink {
        return .{ .ptr = r, .vtable = &.{ .log = record } };
    }

    fn record(ptr: ?*anyopaque, level: log_mod.Level, message: []const u8, fields: []const log_mod.Field) void {
        if (!std.mem.startsWith(u8, message, "[TMPRL1103]")) return;
        const r: *LimitRecords = @ptrCast(@alignCast(ptr.?));
        var rec: Record = .{
            .level = level,
            .memo = std.mem.indexOf(u8, message, "upload memo ") != null,
            .error_limit = std.mem.indexOf(u8, message, "the error limit") != null,
        };
        for (fields) |f| {
            if (std.mem.eql(u8, f.key, "size")) rec.size = std.fmt.parseInt(u64, f.value, 10) catch null;
            if (std.mem.eql(u8, f.key, "limit")) rec.limit = std.fmt.parseInt(u64, f.value, 10) catch null;
        }
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        if (r.count < max) r.records[r.count] = rec;
        r.count += 1;
        r.changed.broadcast(testing.io);
    }

    /// Waits up to 30 s for at least `n` records.
    fn waitFor(r: *LimitRecords, n: usize) !void {
        const deadline = Io.Clock.Timestamp.fromNow(testing.io, .{ .raw = .fromSeconds(30), .clock = .awake });
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        while (r.count < n) r.changed.waitTimeout(testing.io, &r.mutex, .{ .deadline = deadline }) catch |err| {
            std.log.err("waited for {d} payload limit records, got {d}", .{ n, r.count });
            return err;
        };
    }

    /// The records from the `from`-th on, copied into `out`.
    fn since(r: *LimitRecords, from: usize, out: *[max]Record) []const Record {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        const end = @min(r.count, max);
        const start = @min(from, end);
        @memcpy(out[0 .. end - start], r.records[start..end]);
        return out[0 .. end - start];
    }

    fn current(r: *LimitRecords) usize {
        r.mutex.lockUncancelable(testing.io);
        defer r.mutex.unlock(testing.io);
        return r.count;
    }
};

/// The causes of the run's WorkflowTaskFailed events, and whether it has a completed event
/// and a terminated event.
const HistoryFacts = struct {
    task_failure_causes: [8]proto.temporal.api.enums.v1.WorkflowTaskFailedCause = undefined,
    task_failures: usize = 0,
    completed: bool = false,
    terminated: bool = false,

    fn of(c: *Client, arena: std.mem.Allocator, id: []const u8, run_id: []const u8) !HistoryFacts {
        const history = try c.fetchHistory(arena, id, run_id);
        var facts: HistoryFacts = .{};
        for (history.events.items) |event| switch (event.attributes orelse continue) {
            .workflow_task_failed_event_attributes => |e| {
                if (facts.task_failures < facts.task_failure_causes.len) facts.task_failure_causes[facts.task_failures] = e.cause;
                facts.task_failures += 1;
            },
            .workflow_execution_completed_event_attributes => facts.completed = true,
            .workflow_execution_terminated_event_attributes => facts.terminated = true,
            else => {},
        };
        return facts;
    }
};

/// Starts a worker of `c` on `task_queue` with `fail_oversized_payloads = fail` and runs
/// `Fixtures.Big` with an oversized result on it, then checks what the run and the client's
/// logger saw.
fn expectOversizedResult(c: *Client, records: *LimitRecords, arena: std.mem.Allocator, task_queue: []const u8, fail: bool) !void {
    var w = try worker_mod.Worker.init(testing.allocator, testing.io, c, task_queue, .{ .fail_oversized_payloads = fail });
    defer w.deinit();
    try w.registerWorkflow(Fixtures.Big);
    var running = try testing.io.concurrent(Fixtures.runWorker, .{&w});
    defer {
        w.stop();
        running.await(testing.io) catch {};
    }
    try w.waitUntilRunning();

    const seen = records.current();
    // The execution timeout bounds `run.get` below should the run never close.
    var run = try c.executeWorkflow(Fixtures.Big, .{ .id = task_queue, .task_queue = task_queue, .execution_timeout = .fromSeconds(60) }, .{oversized_result});
    defer run.deinit();
    var buf: [LimitRecords.max]LimitRecords.Record = undefined;
    if (fail) {
        // Two failed attempts: the server recorded the first, then handed the task out again.
        try records.waitFor(seen + 2);
        for (records.since(seen, &buf)) |rec| {
            try testing.expectEqual(log_mod.Level.err, rec.level);
            try testing.expect(rec.error_limit and !rec.memo);
            try testing.expect(rec.size.? > oversized_result);
            try testing.expect(rec.limit.? > 0 and rec.limit.? < oversized_result);
        }
        const facts: HistoryFacts = try .of(c, arena, run.id, run.run_id);
        try testing.expect(facts.task_failures >= 1);
        try testing.expectEqual(.WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE, facts.task_failure_causes[0]);
        try testing.expect(!facts.completed and !facts.terminated);
        const described = try c.describeWorkflow(arena, run.id, run.run_id);
        try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, described.status);
        try c.terminateWorkflow(run.id, run.run_id, "test over");
    } else {
        // The completion reached the server, which refused the result.
        try testing.expectError(error.WorkflowTerminated, run.get(arena));
        const facts: HistoryFacts = try .of(c, arena, run.id, run.run_id);
        try testing.expect(facts.task_failures >= 1);
        try testing.expect(facts.task_failure_causes[0] != .WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE);
        try testing.expect(facts.terminated and !facts.completed);
        // The worker logged no error of its own: only the warning above the warning size.
        for (records.since(seen, &buf)) |rec| try testing.expect(!rec.error_limit and rec.level == .warn);
    }
}

test "a worker fails a workflow task with an oversized result and logs why; with the check off the server's rejection stands" {
    var server = try DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();

    var records: LimitRecords = .{};
    var options = server.clientOptions();
    options.logger = records.sink();
    var c = try Client.connect(testing.allocator, testing.io, options);
    defer c.close();

    try expectOversizedResult(&c, &records, arena_state.allocator(), "payload-limits-on", true);
    try expectOversizedResult(&c, &records, arena_state.allocator(), "payload-limits-off", false);
}

test "a client logs one warning for each payload and memo over its warning sizes and sends the request; zero sizes log none" {
    var server = try DevServer.start(testing.allocator, testing.io, .{});
    defer server.stop(testing.io);
    var arena_state: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const argument = try arena.alloc(u8, 300);
    @memset(argument, 'a');
    const note = try arena.alloc(u8, 200);
    @memset(note, 'm');
    const memo = try converter.toFields(arena, .{ .note = @as([]const u8, note) });

    var records: LimitRecords = .{};
    var options = server.clientOptions();
    options.logger = records.sink();
    options.payloads_warning_size = 0;
    options.memo_warning_size = 0;
    var c = try Client.connect(testing.allocator, testing.io, options);
    defer c.close();

    // Both warnings off. sdk-core logs on the calling thread before sending, so a warning
    // would already be in by the time the call returns; the reconnect below also orders it
    // before the next call's.
    var quiet = try c.executeWorkflow(Fixtures.Idle, .{ .id = "payload-limits-quiet", .task_queue = "payload-limits-none", .memo = memo }, .{argument});
    defer quiet.deinit();
    try testing.expect(quiet.run_id.len > 0);
    try testing.expectEqual(@as(usize, 0), records.current());

    var small = options.connectionOptions();
    small.payloads_warning_size = 100;
    small.memo_warning_size = 50;
    try c.reconnect(small);
    var warned = try c.executeWorkflow(Fixtures.Idle, .{ .id = "payload-limits-warned", .task_queue = "payload-limits-none", .memo = memo }, .{argument});
    defer warned.deinit();
    try testing.expect(warned.run_id.len > 0);
    try records.waitFor(2);

    var buf: [LimitRecords.max]LimitRecords.Record = undefined;
    const got = records.since(0, &buf);
    try testing.expectEqual(@as(usize, 2), got.len);
    var payloads_seen = false;
    var memo_seen = false;
    for (got) |rec| {
        try testing.expectEqual(log_mod.Level.warn, rec.level);
        try testing.expect(!rec.error_limit);
        if (rec.memo) {
            try testing.expect(!memo_seen);
            memo_seen = true;
            try testing.expectEqual(@as(?u64, 50), rec.limit);
            try testing.expect(rec.size.? > 200);
        } else {
            try testing.expect(!payloads_seen);
            payloads_seen = true;
            try testing.expectEqual(@as(?u64, 100), rec.limit);
            try testing.expect(rec.size.? > 300);
        }
    }
    try testing.expect(payloads_seen and memo_seen);

    // The workflow was started all the same.
    const described = try c.describeWorkflow(arena, warned.id, warned.run_id);
    try testing.expectEqual(client_mod.WorkflowExecutionStatus.running, described.status);
    try c.terminateWorkflow(quiet.id, quiet.run_id, "test over");
    try c.terminateWorkflow(warned.id, warned.run_id, "test over");
}
