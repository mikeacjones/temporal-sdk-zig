//! Collect and store the raw histories of passing conformance features.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const report = @import("report.zig");
const stored = harness.stored;
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Execution = temporal.proto.temporal.api.common.v1.WorkflowExecution;
const Timestamp = temporal.proto.google.protobuf.Timestamp;

pub const Run = struct { workflow_id: []const u8, workflow_type: []const u8, json: []const u8 };
pub const Set = struct { variant: ?[]const u8, namespace: []const u8, task_queue: []const u8, runs: []const Run };
pub const Collected = union(enum) { set: Set, failed: []const u8 };

pub const CollectOptions = struct {
    settle_interval: Io.Duration = .fromMilliseconds(100),
    settle_limit: Io.Duration = .fromSeconds(5),
};

const StartedRun = struct {
    execution: Execution,
    time: Timestamp,
    run: Run,

    fn lessThan(_: void, a: StartedRun, b: StartedRun) bool {
        if (a.time.seconds != b.time.seconds) return a.time.seconds < b.time.seconds;
        if (a.time.nanos != b.time.nanos) return a.time.nanos < b.time.nanos;
        return compareExecutions(a.execution, b.execution) == .lt;
    }
};

/// Finds the closed runs on the queue and returns their raw histories in start order.
/// The runs live in `arena`. The set borrows `namespace`, `task_queue` and `variant`.
pub fn collect(client: anytype, arena: Allocator, io: Io, namespace: []const u8, task_queue: []const u8, variant: ?[]const u8, options: CollectOptions) error{ OutOfMemory, Canceled }!Collected {
    if (variant) |name| {
        if (!stored.validName(name)) return failed(arena, "invalid variant name \"{s}\" on task queue {s}", .{ name, task_queue });
    }
    const query = try std.fmt.allocPrint(arena, "TaskQueue = '{s}'", .{task_queue});
    const start = Io.Timestamp.now(io, .boot);
    var previous: []const Execution = &.{};
    const settled: ?[]const Execution = while (true) {
        const current = listExecutions(client, arena, namespace, query) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            else => return failed(arena, "ListWorkflowExecutions on task queue {s} returned error.{t}", .{ task_queue, err }),
        };
        const stable = current.len != 0 and sameExecutions(previous, current);
        previous = current;
        const remaining = options.settle_limit.nanoseconds - start.durationTo(Io.Timestamp.now(io, .boot)).nanoseconds;
        if (remaining <= 0) break null;
        if (stable) break current;
        try io.sleep(.{ .nanoseconds = @max(0, @min(options.settle_interval.nanoseconds, remaining)) }, .boot);
        if (start.durationTo(Io.Timestamp.now(io, .boot)).nanoseconds >= options.settle_limit.nanoseconds) break null;
    };
    const executions = settled orelse {
        if (previous.len == 0) return failed(arena, "no workflow run found on task queue {s} within {f}", .{ task_queue, options.settle_limit });
        return failed(arena, "the runs on task queue {s} were still changing after {f}", .{ task_queue, options.settle_limit });
    };

    var closed: std.ArrayList(StartedRun) = .empty;
    for (executions) |execution| {
        const workflow_id = execution.workflow_id;
        const run_id = execution.run_id;
        const history = harness.Context.readRawHistory(client, arena, namespace, workflow_id, run_id) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => |e| return e,
            else => return failed(arena, "GetWorkflowExecutionHistory for workflow {s} run {s} returned error.{t}", .{ workflow_id, run_id, err }),
        };
        const events = history.events.items;
        const workflow_type = (if (events.len == 0) null else stored.startedWorkflowType(events[0])) orelse
            return failed(arena, "workflow {s} run {s} has no WorkflowExecutionStarted event with a workflow type", .{ workflow_id, run_id });
        if (!stored.validName(workflow_type))
            return failed(arena, "workflow {s} run {s} has invalid workflow type \"{s}\"", .{ workflow_id, run_id, workflow_type });
        switch (events[events.len - 1].event_type) {
            .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            .EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
            .EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
            .EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
            .EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
            .EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
            => {},
            else => {
                std.log.info("skipping open workflow {s} run {s}", .{ workflow_id, run_id });
                continue;
            },
        }
        try closed.append(arena, .{
            .execution = execution,
            .time = events[0].event_time orelse .{},
            .run = .{ .workflow_id = workflow_id, .workflow_type = workflow_type, .json = try historyJson(arena, history) },
        });
    }
    if (closed.items.len == 0)
        return failed(arena, "none of the {d} runs on task queue {s} had closed", .{ executions.len, task_queue });
    std.mem.sort(StartedRun, closed.items, {}, StartedRun.lessThan);
    const runs = try arena.alloc(Run, closed.items.len);
    for (closed.items, runs) |item, *run| run.* = item.run;
    return .{ .set = .{ .variant = variant, .namespace = namespace, .task_queue = task_queue, .runs = runs } };
}

fn listExecutions(client: anytype, arena: Allocator, namespace: []const u8, query: []const u8) temporal.client.CallServiceError![]const Execution {
    var executions: std.ArrayList(Execution) = .empty;
    var tokens: std.StringHashMapUnmanaged(void) = .empty;
    var token: []const u8 = "";
    while (true) {
        const page = try client.callService(.workflow, "ListWorkflowExecutions", arena, .{
            .namespace = namespace,
            .query = query,
            .page_size = 1000,
            .next_page_token = token,
        }, null);
        for (page.executions.items) |info| {
            const execution = info.execution orelse return error.InvalidResponse;
            if (execution.workflow_id.len == 0 or execution.run_id.len == 0) return error.InvalidResponse;
            try executions.append(arena, execution);
        }
        token = page.next_page_token;
        if (token.len == 0) break;
        const entry = try tokens.getOrPut(arena, token);
        if (entry.found_existing) return error.InvalidResponse;
    }
    const items = executions.items;
    std.mem.sort(Execution, items, {}, struct {
        fn lessThan(_: void, a: Execution, b: Execution) bool {
            return compareExecutions(a, b) == .lt;
        }
    }.lessThan);
    // Visibility pages can include the same execution more than once.
    var unique: usize = 0;
    for (items) |execution| {
        if (unique != 0 and compareExecutions(items[unique - 1], execution) == .eq) continue;
        items[unique] = execution;
        unique += 1;
    }
    return items[0..unique];
}

fn compareExecutions(a: Execution, b: Execution) std.math.Order {
    const order = std.mem.order(u8, a.workflow_id, b.workflow_id);
    return if (order == .eq) std.mem.order(u8, a.run_id, b.run_id) else order;
}

fn sameExecutions(a: []const Execution, b: []const Execution) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| if (compareExecutions(x, y) != .eq) return false;
    return true;
}

fn failed(arena: Allocator, comptime fmt: []const u8, args: anytype) error{OutOfMemory}!Collected {
    return .{ .failed = try std.fmt.allocPrint(arena, fmt, args) };
}

/// Re-indent the ProtoJSON export without changing numeric lexemes or decoding payloads.
pub fn historyJson(arena: Allocator, history: temporal.history.History) error{OutOfMemory}![]u8 {
    const encoded = try temporal.protojson.encode(arena, history);
    const value = std.json.parseFromSliceLeaky(std.json.Value, arena, encoded, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => unreachable, // The input was just encoded by protojson.
    };
    var output: Io.Writer.Allocating = .init(arena);
    std.json.Stringify.value(value, .{ .whitespace = .indent_2 }, &output.writer) catch return error.OutOfMemory;
    output.writer.writeByte('\n') catch return error.OutOfMemory;
    return output.toOwnedSlice();
}

/// Replaces the current version directory only for a passing feature with sets to store.
pub fn store(arena: Allocator, io: Io, root: ?[]const u8, feature: []const u8, version: []const u8, outcome: report.Outcome, sets: []const Set) error{OutOfMemory}!?[]const u8 {
    const base = root orelse return null;
    if (outcome != .pass or sets.len == 0) return null;
    var components = std.mem.splitScalar(u8, feature, '/');
    while (components.next()) |name| {
        if (!stored.validName(name)) return try std.fmt.allocPrint(arena, "invalid feature path \"{s}\"", .{feature});
    }
    const relative = stored.directoryPath(arena, feature, version, null) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidName => return try std.fmt.allocPrint(arena, "invalid SDK version \"{s}\" for {s}", .{ version, feature }),
    };
    const path = try std.fs.path.join(arena, &.{ base, relative });
    for (sets) |set| {
        if (set.variant) |name| {
            if (!stored.validName(name)) return try std.fmt.allocPrint(arena, "invalid variant name \"{s}\" for {s}", .{ name, path });
        }
        if (set.runs.len == 0 or set.runs.len > std.math.maxInt(u32))
            return try std.fmt.allocPrint(arena, "invalid run count {d} for {s}", .{ set.runs.len, path });
        for (set.runs) |run| {
            if (!stored.validName(run.workflow_type))
                return try std.fmt.allocPrint(arena, "workflow {s} has invalid workflow type \"{s}\" for {s}", .{ run.workflow_id, run.workflow_type, path });
        }
    }

    const cwd = Io.Dir.cwd();
    cwd.deleteTree(io, path) catch |err|
        return try std.fmt.allocPrint(arena, "cannot remove {s}: error.{t}; the directory may be incomplete", .{ path, err });
    for (sets) |set| {
        const leaf = if (set.variant) |name| try std.fs.path.join(arena, &.{ path, name }) else path;
        writeSet(arena, io, leaf, set) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return try std.fmt.allocPrint(arena, "cannot write {s}: error.{t}; {s} may be incomplete", .{ leaf, err, path }),
        };
    }
    return null;
}

fn writeSet(arena: Allocator, io: Io, path: []const u8, set: Set) !void {
    const dir = try Io.Dir.cwd().createDirPathOpen(io, path, .{});
    defer dir.close(io);
    const entries = try arena.alloc(stored.Run, set.runs.len);
    for (set.runs, entries, 1..) |run, *entry, n| {
        const name = try stored.fileName(arena, @intCast(n), run.workflow_type);
        try dir.writeFile(io, .{ .sub_path = name, .data = run.json });
        entry.* = .{ .file = name, .workflow_id = run.workflow_id };
    }
    var output: Io.Writer.Allocating = .init(arena);
    stored.writeManifest(.{ .namespace = set.namespace, .task_queue = set.task_queue, .runs = entries }, &output.writer) catch return error.OutOfMemory;
    try dir.writeFile(io, .{ .sub_path = stored.manifest_name, .data = output.written() });
}

test "conformance stored histories are valid raw history exports" {
    var state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    var problems: std.ArrayList([]const u8) = .empty;
    _ = try stored.load(arena, &@import("stored_histories").files, &problems);
    for (problems.items) |message| std.debug.print("{s}\n", .{message});
    try std.testing.expectEqual(0, problems.items.len);
}

test "conformance stored histories carry no file system path of the recording machine" {
    // A failure's stack trace names source files by absolute path, which holds the recording
    // checkout's location (and the user's home directory) and changes with every checkout.
    const key = "\"stackTrace\": \"";
    var leaks: usize = 0;
    for (@import("stored_histories").files) |file| {
        var rest: []const u8 = file.bytes;
        while (std.mem.indexOf(u8, rest, key)) |start| {
            rest = rest[start + key.len ..];
            const end = std.mem.indexOfScalar(u8, rest, '"') orelse rest.len;
            const trace = rest[0..end];
            var frames = std.mem.splitSequence(u8, trace, "\\n");
            while (frames.next()) |frame| {
                const absolute = std.mem.startsWith(u8, frame, "/") or
                    (frame.len > 2 and std.ascii.isAlphabetic(frame[0]) and frame[1] == ':' and (frame[2] == '\\' or frame[2] == '/'));
                if (!absolute) continue;
                std.debug.print("conformance/histories/{s}: stack trace frame with an absolute path: {s}\n", .{ file.path, frame });
                leaks += 1;
                break;
            }
        }
    }
    try std.testing.expectEqual(0, leaks);
}

const FakeService = struct {
    pages: []const Page,
    histories: []const Fixture = &.{},
    calls: usize = 0,
    reads: usize = 0,

    const Page = struct {
        token: []const u8 = "",
        next: []const u8 = "",
        executions: []const Execution,
    };
    const Fixture = struct {
        execution: Execution,
        time: Timestamp = .{},
        workflow_type: []const u8 = "Workflow",
        last: temporal.proto.temporal.api.enums.v1.EventType,
    };

    pub fn callService(
        self: *FakeService,
        comptime service: temporal.client.Service,
        comptime method: []const u8,
        arena: Allocator,
        request: temporal.client.ServiceRequest(service, method),
        _: ?*?temporal.client.RpcFailure,
    ) temporal.client.CallServiceError!temporal.client.ServiceResponse(service, method) {
        if (!std.mem.eql(u8, request.namespace, "ns")) return error.InvalidArgument;
        if (comptime std.mem.eql(u8, method, "ListWorkflowExecutions")) {
            if (!std.mem.eql(u8, request.query, "TaskQueue = 'queue'")) return error.InvalidArgument;
            if (self.calls == self.pages.len) return error.InvalidResponse;
            const page = self.pages[self.calls];
            self.calls += 1;
            if (!std.mem.eql(u8, request.next_page_token, page.token)) return error.InvalidArgument;
            var response: temporal.client.ServiceResponse(service, method) = .{ .next_page_token = page.next };
            for (page.executions) |execution| {
                // Deliberately stale status: only the last event tells whether a run is closed.
                try response.executions.append(arena, .{ .execution = execution, .status = .WORKFLOW_EXECUTION_STATUS_RUNNING });
            }
            return response;
        }
        if (comptime !std.mem.eql(u8, method, "GetWorkflowExecutionHistory")) @compileError("unexpected recorder RPC");
        const execution = request.execution orelse return error.InvalidArgument;
        if (request.next_page_token.len != 0 or request.wait_new_event or !request.skip_archival) return error.InvalidArgument;
        self.reads += 1;
        for (self.histories) |fixture| {
            if (compareExecutions(fixture.execution, execution) != .eq) continue;
            const payload = try temporal.converter.payloadWithEncoding(arena, "binary/fixture", "opaque\x00bytes");
            var input: temporal.converter.Payloads = .{};
            try input.payloads.append(arena, payload);
            var history: temporal.history.History = .{};
            try history.events.append(arena, .{
                .event_id = 1,
                .event_time = fixture.time,
                .event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
                .attributes = .{ .workflow_execution_started_event_attributes = .{
                    .workflow_type = .{ .name = fixture.workflow_type },
                    .original_execution_run_id = execution.run_id,
                    .input = input,
                } },
            });
            try history.events.append(arena, .{ .event_id = 2, .event_type = fixture.last });
            return .{ .history = history };
        }
        return error.NotFound;
    }
};

test "conformance collects settled pages and orders closed histories by start time and IDs" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();

    const a: Execution = .{ .workflow_id = "alpha", .run_id = "r2" };
    const b: Execution = .{ .workflow_id = "alpha", .run_id = "r1" };
    const c: Execution = .{ .workflow_id = "beta", .run_id = "r1" };
    const d: Execution = .{ .workflow_id = "first", .run_id = "r3" };
    const open: Execution = .{ .workflow_id = "open", .run_id = "r4" };
    var service: FakeService = .{
        .pages = &.{
            .{ .executions = &.{} },
            .{ .executions = &.{d} },
            .{ .executions = &.{ c, a, open }, .next = "p2" },
            .{ .token = "p2", .executions = &.{ d, b, a } },
            .{ .executions = &.{ open, b, d, a, c } },
        },
        .histories = &.{
            .{ .execution = a, .time = .{ .seconds = 2, .nanos = 3 }, .last = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED },
            .{ .execution = b, .time = .{ .seconds = 2, .nanos = 3 }, .last = .EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW },
            .{ .execution = c, .time = .{ .seconds = 2, .nanos = 3 }, .last = .EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, .workflow_type = "Child" },
            .{ .execution = d, .time = .{ .seconds = 2, .nanos = 2 }, .last = .EVENT_TYPE_WORKFLOW_EXECUTION_FAILED },
            .{ .execution = open, .last = .EVENT_TYPE_WORKFLOW_TASK_SCHEDULED },
        },
    };
    const result = try collect(&service, arena, std.testing.io, "ns", "queue", "variant", .{ .settle_interval = .zero });
    try std.testing.expectEqual(std.meta.Tag(Collected).set, std.meta.activeTag(result));
    try std.testing.expectEqual(5, service.calls);
    try std.testing.expectEqual(5, service.reads);
    try std.testing.expectEqualStrings("variant", result.set.variant.?);
    try std.testing.expectEqualStrings("ns", result.set.namespace);
    try std.testing.expectEqualStrings("queue", result.set.task_queue);
    try std.testing.expectEqual(4, result.set.runs.len);
    for (result.set.runs, [_]Execution{ d, b, a, c }) |run, want| {
        try std.testing.expectEqualStrings(want.workflow_id, run.workflow_id);
        try std.testing.expect(std.mem.startsWith(u8, run.json, "{\n  \"events\": ["));
        try std.testing.expect(std.mem.endsWith(u8, run.json, "\n}\n"));
        const history = try temporal.history.fromJson(arena, run.json);
        const attrs = history.events.items[0].attributes.?.workflow_execution_started_event_attributes;
        try std.testing.expectEqualStrings(want.run_id, attrs.original_execution_run_id);
        const payload = attrs.input.?.payloads.items[0];
        try std.testing.expectEqualStrings("binary/fixture", temporal.converter.encodingOf(payload).?);
        try std.testing.expectEqualStrings("opaque\x00bytes", payload.data);
    }
}

test "conformance collection reports empty or unsettled listings and open or unreadable runs" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const io = std.testing.io;
    const run: Execution = .{ .workflow_id = "wf", .run_id = "run" };
    const page: FakeService.Page = .{ .executions = &.{run} };
    const no_wait: CollectOptions = .{ .settle_interval = .zero, .settle_limit = .zero };
    const instant_poll: CollectOptions = .{ .settle_interval = .zero };

    var empty: FakeService = .{ .pages = &.{.{ .executions = &.{} }} };
    try std.testing.expectEqualStrings("no workflow run found on task queue queue within 0ns", (try collect(&empty, arena, io, "ns", "queue", null, no_wait)).failed);
    var unsettled: FakeService = .{ .pages = &.{page} };
    try std.testing.expectEqualStrings("the runs on task queue queue were still changing after 0ns", (try collect(&unsettled, arena, io, "ns", "queue", null, no_wait)).failed);

    const pages: []const FakeService.Page = &.{ page, page };
    var open: FakeService = .{ .pages = pages, .histories = &.{.{ .execution = run, .last = .EVENT_TYPE_WORKFLOW_TASK_STARTED }} };
    try std.testing.expectEqualStrings("none of the 1 runs on task queue queue had closed", (try collect(&open, arena, io, "ns", "queue", null, instant_poll)).failed);
    var missing: FakeService = .{ .pages = pages };
    try std.testing.expectEqualStrings("GetWorkflowExecutionHistory for workflow wf run run returned error.NotFound", (try collect(&missing, arena, io, "ns", "queue", null, instant_poll)).failed);
    var bad_type: FakeService = .{
        .pages = pages,
        .histories = &.{.{ .execution = run, .workflow_type = "../bad", .last = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED }},
    };
    try std.testing.expectEqualStrings("workflow wf run run has invalid workflow type \"../bad\"", (try collect(&bad_type, arena, io, "ns", "queue", null, instant_poll)).failed);
    var bad_variant: FakeService = .{ .pages = &.{} };
    try std.testing.expectEqualStrings("invalid variant name \"../bad\" on task queue queue", (try collect(&bad_variant, arena, io, "ns", "queue", "../bad", instant_poll)).failed);
    try std.testing.expectEqual(0, bad_variant.calls);
}

test "conformance store replaces the whole current version only for an opted-in passing feature" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const root = try tmp.dir.realPathFileAlloc(io, ".", arena);

    const old_files: []const []const u8 = &.{
        "area/name/0.0.0/stale.json",
        "area/name/0.0.0/stale-variant/1-W.json",
        "area/name/0.0.0/current/3-W.json",
        "area/name/1.0.0/1-W.json",
        "area/other/0.0.0/1-W.json",
    };
    for (old_files) |path| {
        try tmp.dir.createDirPath(io, std.fs.path.dirname(path).?);
        try tmp.dir.writeFile(io, .{ .sub_path = path, .data = "original" });
    }
    const sets: []const Set = &.{
        .{ .variant = "current", .namespace = "ns", .task_queue = "queue", .runs = &.{
            .{ .workflow_id = "parent", .workflow_type = "W", .json = "parent history" },
            .{ .workflow_id = "child", .workflow_type = "Child", .json = "child history" },
        } },
        .{ .variant = "second", .namespace = "ns", .task_queue = "second-queue", .runs = &.{
            .{ .workflow_id = "other-run", .workflow_type = "W", .json = "second history" },
        } },
    };
    try std.testing.expectEqual(null, try store(arena, io, null, "area/name", "0.0.0", .pass, sets));
    try std.testing.expectEqual(null, try store(arena, io, root, "area/name", "0.0.0", .{ .fail = "variant failed" }, sets));
    try std.testing.expectEqual(null, try store(arena, io, root, "area/name", "0.0.0", .pass, &.{}));
    for (old_files) |path| {
        try std.testing.expectEqualStrings("original", try tmp.dir.readFileAlloc(io, path, arena, .unlimited));
    }
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "area/name/0.0.0/current/runs.json", .{}));

    try std.testing.expectEqual(null, try store(arena, io, root, "area/name", "0.0.0", .pass, sets));
    for (old_files[0..3]) |path| try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, path, .{}));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, "area/name/0.0.0/stale-variant", .{}));
    for (old_files[3..]) |path| {
        try std.testing.expectEqualStrings("original", try tmp.dir.readFileAlloc(io, path, arena, .unlimited));
    }
    try std.testing.expectEqualStrings("parent history", try tmp.dir.readFileAlloc(io, "area/name/0.0.0/current/1-W.json", arena, .unlimited));
    try std.testing.expectEqualStrings("child history", try tmp.dir.readFileAlloc(io, "area/name/0.0.0/current/2-Child.json", arena, .unlimited));
    try std.testing.expectEqualStrings("second history", try tmp.dir.readFileAlloc(io, "area/name/0.0.0/second/1-W.json", arena, .unlimited));
    const manifest = try stored.parseManifest(arena, try tmp.dir.readFileAlloc(io, "area/name/0.0.0/current/runs.json", arena, .unlimited));
    try std.testing.expectEqualDeep(stored.Manifest{ .namespace = "ns", .task_queue = "queue", .runs = &.{
        .{ .file = "1-W.json", .workflow_id = "parent" },
        .{ .file = "2-Child.json", .workflow_id = "child" },
    } }, manifest);
}
