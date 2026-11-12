//! Tests for the metrics sample. A custom metrics handler keeps every value recorded through
//! it, so the tests read back what the activity recorded.

const std = @import("std");
const temporal = @import("temporal");
const sample = @import("metrics.zig");
const metrics = temporal.metrics;

const testing = std.testing;

/// A metrics handler that keeps every record, with its metric's name and its attributes
/// flattened, in an arena freed by `deinit`. Thread-safe: sdk-core records from its own
/// threads.
const Recorder = struct {
    gpa: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    arena: std.heap.ArenaAllocator,
    records: std.ArrayList(Record) = .empty,

    const Metric = struct { name: []const u8 };
    const Attributes = struct { list: []const metrics.Attribute };

    const Record = struct {
        name: []const u8,
        value: metrics.Value,
        attributes: []const metrics.Attribute,

        fn get(r: Record, key: []const u8) ?metrics.AttributeValue {
            for (r.attributes) |a| if (std.mem.eql(u8, a.key, key)) return a.value;
            return null;
        }
    };

    fn init(gpa: std.mem.Allocator) Recorder {
        return .{ .gpa = gpa, .arena = .init(gpa) };
    }

    fn deinit(r: *Recorder) void {
        r.records.deinit(r.gpa);
        r.arena.deinit();
    }

    fn handler(r: *Recorder) metrics.Handler {
        return .{ .ptr = r, .vtable = &.{
            .newMetric = newMetric,
            .freeMetric = free,
            .newAttributes = newAttributes,
            .freeAttributes = free,
            .record = record,
        } };
    }

    /// The records of the metric named `name`, allocated from `a`.
    fn recordsOf(r: *Recorder, a: std.mem.Allocator, name: []const u8) ![]Record {
        r.lock();
        defer r.mutex.unlock();
        var out: std.ArrayList(Record) = .empty;
        for (r.records.items) |rec| if (std.mem.eql(u8, rec.name, name)) try out.append(a, rec);
        return out.toOwnedSlice(a);
    }

    fn lock(r: *Recorder) void {
        while (!r.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn cast(ptr: ?*anyopaque) *Recorder {
        return @ptrCast(@alignCast(ptr.?));
    }

    fn newMetric(ptr: ?*anyopaque, definition: metrics.Definition) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const m = try r.arena.allocator().create(Metric);
        m.* = .{ .name = try r.arena.allocator().dupe(u8, definition.name) };
        return m;
    }

    // Everything lives in the arena until `deinit`.
    fn free(_: ?*anyopaque, _: *anyopaque) void {}

    fn newAttributes(ptr: ?*anyopaque, base: ?*anyopaque, attributes: []const metrics.Attribute) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        const inherited: []const metrics.Attribute = if (base) |b| @as(*const Attributes, @ptrCast(@alignCast(b))).list else &.{};
        const list = try a.alloc(metrics.Attribute, inherited.len + attributes.len);
        @memcpy(list[0..inherited.len], inherited);
        for (attributes, list[inherited.len..]) |attr, *copy| copy.* = .{
            .key = try a.dupe(u8, attr.key),
            .value = switch (attr.value) {
                .string => |s| .{ .string = try a.dupe(u8, s) },
                else => attr.value,
            },
        };
        const set = try a.create(Attributes);
        set.* = .{ .list = list };
        return set;
    }

    fn record(ptr: ?*anyopaque, metric: *anyopaque, value: metrics.Value, attributes: ?*anyopaque) void {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const m: *const Metric = @ptrCast(@alignCast(metric));
        const list: []const metrics.Attribute = if (attributes) |s| @as(*const Attributes, @ptrCast(@alignCast(s))).list else &.{};
        r.records.append(r.gpa, .{ .name = m.name, .value = value, .attributes = list }) catch {};
    }
};

fn expectOne(records: []const Recorder.Record) !Recorder.Record {
    try testing.expectEqual(1, records.len);
    try testing.expectEqualStrings("Activity", records[0].get("operation").?.string);
    return records[0];
}

/// Checks what one successful run of the activity recorded: one start and one success, a
/// run time of at least its one-second sleep, and a wait of at least `min_wait_ms`.
fn expectActivityMetrics(recorder: *Recorder, min_wait_ms: i64) !void {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const started = try expectOne(try recorder.recordsOf(a, sample.activity_started_count));
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, started.value);
    const succeeded = try expectOne(try recorder.recordsOf(a, sample.activity_success_count));
    try testing.expectEqual(metrics.Value{ .unsigned = 1 }, succeeded.value);
    try testing.expectEqual(0, (try recorder.recordsOf(a, sample.activity_failed_count)).len);

    const latency = try expectOne(try recorder.recordsOf(a, sample.activity_latency));
    try testing.expect(latency.value.duration.toMilliseconds() >= 1000);
    const wait = try expectOne(try recorder.recordsOf(a, sample.schedule_to_start_latency));
    try testing.expect(wait.value.duration.toMilliseconds() >= min_wait_ms);
}

test "the workflow's activity records its metrics through the client's handler" {
    var recorder: Recorder = .init(testing.allocator);
    defer recorder.deinit();
    {
        const env = try temporal.testing.WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .metrics = .{ .exporter = .{ .custom = recorder.handler() } },
        });
        defer env.deinit();
        try env.registerWorkflow(sample.Workflow);
        try env.registerActivity(sample.Activity);

        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        try env.executeWorkflow(sample.Workflow, .{}, arena.allocator(), null);
    }
    // The wait is measured from before the workflow's 500ms sleep.
    try expectActivityMetrics(&recorder, 500);

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const wait = try recorder.recordsOf(arena.allocator(), sample.schedule_to_start_latency);
    try testing.expectEqualStrings("Workflow", wait[0].get("workflow_type").?.string);
}

test "the activity records its metrics through the context's handler" {
    var recorder: Recorder = .init(testing.allocator);
    defer recorder.deinit();
    var env: temporal.testing.ActivityEnvironment = .init(testing.io);
    env.metrics = recorder.handler();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const scheduled: std.Io.Timestamp = .now(testing.io, .real);
    try env.executeActivity(sample.Activity, .{@intCast(scheduled.nanoseconds)}, arena.allocator());
    try expectActivityMetrics(&recorder, 0);
}
