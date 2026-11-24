//! A workflow and its two activities add to their own counters with an attribute, and the
//! driver checks the totals the client's custom metrics handler received: the two counters
//! and sdk-core's `temporal_request` count of activity completions.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");

const activity = temporal.activity;
const workflow = temporal.workflow;
const metrics = temporal.metrics;

const CommaJoin = activity.Def("CommaJoin", commaJoin);
const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .activities = .{CommaJoin},
    .execute = execute,
    .configureClient = configureClient,
});

fn configureClient(options: *temporal.client.Options) void {
    options.metrics = .{ .exporter = .{ .custom = recorder.handler() } };
}

fn runWorkflow(ctx: *workflow.Context) ![]const u8 {
    const counter = try ctx.metrics().counter("my_workflow_counter", .{});
    defer counter.deinit();
    counter.add(2, .{ .mytag = "mytagvalue" });

    const options: workflow.ActivityOptions = .{ .schedule_to_close_timeout = .fromSeconds(60) };
    const first = try ctx.executeActivity(CommaJoin, options, .{&[_][]const u8{ "foo", "bar" }}).get();
    return ctx.executeActivity(CommaJoin, options, .{&[_][]const u8{ first, "baz" }}).get();
}

fn commaJoin(ctx: *activity.Context, strs: []const []const u8) ![]const u8 {
    const counter = try ctx.metrics().counter("my_activity_counter", .{});
    defer counter.deinit();
    counter.add(2, .{ .mytag = "mytagvalue" });
    return std.mem.join(ctx.allocator(), ", ", strs);
}

fn execute(ctx: *harness.Context) !void {
    var run = try ctx.client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    const result = try run.get(ctx.arena);
    try ctx.expectEqualStrings("foo, bar, baz", result);

    // The workflow's counter: 2 once. The activity's: 2 for each of the two activities.
    try expectTotal(ctx, "my_workflow_counter", "mytag", "mytagvalue", 2);
    try expectTotal(ctx, "my_activity_counter", "mytag", "mytagvalue", 4);
    // sdk-core counts its calls; the worker completed two activity tasks.
    try expectTotal(ctx, "temporal_request", "operation", "RespondActivityTaskCompleted", 2);
}

fn expectTotal(ctx: *harness.Context, name: []const u8, key: []const u8, value: []const u8, expected: u64) error{FeatureFailed}!void {
    const actual = recorder.total(name, key, value);
    try ctx.expect(actual == expected, "counter {s} with {s}={s} totals {d}, expected {d}", .{ name, key, value, actual, expected });
}

/// The client's custom metrics handler. File-scope, because `configureClient` may only borrow
/// file-scope data and the handler must outlive the client and its workers.
var recorder: Recorder = .{};

/// Keeps every counter value it is handed, with its metric and attribute set. Its vtable is
/// called from sdk-core's threads and every thread that records, so each call takes the lock.
/// Handles, attribute sets and records come from an arena over the page allocator and are
/// never freed: a record can point at an attribute set the core has released, and the memory
/// lives until the feature's process exits.
const Recorder = struct {
    mutex: std.atomic.Mutex = .unlocked,
    arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
    /// The newest record first.
    records: ?*const Record = null,

    const Metric = struct { name: []const u8, kind: metrics.Kind };
    const Set = struct { base: ?*const Set, list: []const metrics.Attribute };
    const Record = struct { next: ?*const Record, metric: *const Metric, value: u64, attributes: ?*const Set };

    const vtable: metrics.VTable = .{
        .newMetric = newMetric,
        .freeMetric = freeMetric,
        .newAttributes = newAttributes,
        .freeAttributes = freeAttributes,
        .record = record,
    };

    fn handler(r: *Recorder) metrics.Handler {
        return .{ .ptr = r, .vtable = &vtable };
    }

    fn lock(r: *Recorder) void {
        while (!r.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// The sum of the counter values recorded for `name` whose attribute `key` is the string
    /// `value`.
    fn total(r: *Recorder, name: []const u8, key: []const u8, value: []const u8) u64 {
        r.lock();
        defer r.mutex.unlock();
        var sum: u64 = 0;
        var next = r.records;
        while (next) |rec| : (next = rec.next) {
            if (!std.mem.eql(u8, rec.metric.name, name)) continue;
            const found = lookup(rec.attributes, key) orelse continue;
            if (found == .string and std.mem.eql(u8, found.string, value)) sum +|= rec.value;
        }
        return sum;
    }

    /// The value of `key` in `set`: a set's own attributes replace its base's, and a later
    /// entry in a list replaces an earlier one.
    fn lookup(set: ?*const Set, key: []const u8) ?metrics.AttributeValue {
        var current = set;
        while (current) |s| : (current = s.base) {
            var i = s.list.len;
            while (i > 0) {
                i -= 1;
                if (std.mem.eql(u8, s.list[i].key, key)) return s.list[i].value;
            }
        }
        return null;
    }

    fn cast(ptr: ?*anyopaque) *Recorder {
        return @ptrCast(@alignCast(ptr.?));
    }

    fn newMetric(ptr: ?*anyopaque, definition: metrics.Definition) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        const m = try a.create(Metric);
        m.* = .{ .name = try a.dupe(u8, definition.name), .kind = definition.kind };
        return m;
    }

    fn freeMetric(_: ?*anyopaque, _: *anyopaque) void {}

    fn newAttributes(ptr: ?*anyopaque, base: ?*anyopaque, attributes: []const metrics.Attribute) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        const list = try a.alloc(metrics.Attribute, attributes.len);
        for (attributes, list) |in, *out| out.* = .{ .key = try a.dupe(u8, in.key), .value = switch (in.value) {
            .string => |s| .{ .string = try a.dupe(u8, s) },
            else => in.value,
        } };
        const set = try a.create(Set);
        set.* = .{ .base = @ptrCast(@alignCast(base)), .list = list };
        return set;
    }

    fn freeAttributes(_: ?*anyopaque, _: *anyopaque) void {}

    /// Keeps counter values only: they are all the driver reads.
    fn record(ptr: ?*anyopaque, metric: *anyopaque, value: metrics.Value, attributes: ?*anyopaque) void {
        const m: *const Metric = @ptrCast(@alignCast(metric));
        if (m.kind != .counter) return;
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const rec = r.arena.allocator().create(Record) catch return;
        rec.* = .{ .next = r.records, .metric = m, .value = value.unsigned, .attributes = @ptrCast(@alignCast(attributes)) };
        r.records = rec;
    }
};
