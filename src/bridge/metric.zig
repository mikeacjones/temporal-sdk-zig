//! Metrics through the core, in both directions:
//!
//! - `Meter` records to the runtime's meter, and through it to the runtime's exporter
//!   (Prometheus, OpenTelemetry, or a custom meter).
//! - `CustomMeter` adapts the core's custom meter callbacks to a `VTable`, so the core's
//!   metrics, and those recorded through a `Meter`, go to SDK code.
//!
//! Both speak `VTable`: `Meter.vtable` implements it over the core meter. Every core metric
//! call is thread-safe; custom meter callbacks run on the core's threads, concurrently.

const std = @import("std");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Runtime = @import("Runtime.zig");
const Allocator = std.mem.Allocator;
const Duration = std.Io.Duration;

/// The kinds of metric the core knows. The values are the core's.
pub const Kind = enum(c_uint) {
    counter = 1,
    histogram,
    histogram_float,
    histogram_duration,
    gauge,
    gauge_float,
    up_down_counter,
};

/// One recorded value. Its tag follows the metric's kind.
pub const Value = union(enum) {
    /// `counter`, `histogram`, and `gauge`.
    unsigned: u64,
    /// `up_down_counter`.
    signed: i64,
    /// `histogram_float` and `gauge_float`.
    float: f64,
    /// `histogram_duration`. The core keeps whole milliseconds; negative durations are 0.
    duration: Duration,
};

pub const Attribute = struct {
    key: []const u8,
    value: AttributeValue,
};

pub const AttributeValue = union(enum) {
    string: []const u8,
    int: i64,
    float: f64,
    bool: bool,
};

pub const Definition = struct {
    name: []const u8,
    description: []const u8 = "",
    unit: []const u8 = "",
    kind: Kind,
};

/// A metrics implementation. Metrics and attribute sets are opaque handles it creates and
/// frees. Every function may be called concurrently from any thread. Strings passed in are
/// only valid during the call.
pub const VTable = struct {
    /// Creates a metric.
    newMetric: *const fn (ptr: ?*anyopaque, definition: Definition) error{OutOfMemory}!*anyopaque,
    freeMetric: *const fn (ptr: ?*anyopaque, metric: *anyopaque) void,
    /// Creates an attribute set: those of `base` (null for none), then `attributes`; a key
    /// given again replaces the earlier value.
    newAttributes: *const fn (ptr: ?*anyopaque, base: ?*anyopaque, attributes: []const Attribute) error{OutOfMemory}!*anyopaque,
    freeAttributes: *const fn (ptr: ?*anyopaque, attributes: *anyopaque) void,
    /// Records `value`, whose tag matches the metric's kind, with `attributes` (null for
    /// none).
    record: *const fn (ptr: ?*anyopaque, metric: *anyopaque, value: Value, attributes: ?*anyopaque) void,
};

/// Whole milliseconds of `d`, clamped to the core's range.
pub fn durationMs(d: Duration) u64 {
    if (d.nanoseconds <= 0) return 0;
    return std.math.cast(u64, @divTrunc(d.nanoseconds, std.time.ns_per_ms)) orelse std.math.maxInt(u64);
}

// ---------------------------------------------------------------- the runtime's meter

/// The runtime's meter. User metric names are not prefixed (`Runtime.Metrics.prefix` applies
/// to the core's own metrics); attribute sets start with the runtime's default attributes
/// (`service_name`, unless turned off). Thread-safe. Destroy it before the runtime.
pub const Meter = struct {
    gpa: Allocator,
    ptr: *c.TemporalCoreMetricMeter,
    /// The runtime's default attributes, for recording without attributes: the core needs a
    /// set for every record.
    defaults: *c.TemporalCoreMetricAttributes,

    /// The runtime's meter, or null when the runtime has no metrics exporter.
    pub fn create(gpa: Allocator, runtime: Runtime) error{OutOfMemory}!?*Meter {
        const ptr = c.temporal_core_metric_meter_new(runtime.ptr) orelse return null;
        errdefer c.temporal_core_metric_meter_free(ptr);
        const m = try gpa.create(Meter);
        errdefer gpa.destroy(m);
        var none: [1]c.TemporalCoreMetricAttribute = undefined;
        const defaults = c.temporal_core_metric_attributes_new(ptr, &none, 0) orelse return error.OutOfMemory;
        m.* = .{ .gpa = gpa, .ptr = ptr, .defaults = defaults };
        return m;
    }

    /// Frees the meter. Metrics and attribute sets made from it must be freed first.
    pub fn destroy(m: *Meter) void {
        c.temporal_core_metric_attributes_free(m.defaults);
        c.temporal_core_metric_meter_free(m.ptr);
        m.gpa.destroy(m);
    }

    pub const vtable: VTable = .{
        .newMetric = newMetric,
        .freeMetric = freeMetric,
        .newAttributes = newAttributes,
        .freeAttributes = freeAttributes,
        .record = record,
    };

    fn cast(ptr: ?*anyopaque) *Meter {
        return @ptrCast(@alignCast(ptr.?));
    }

    fn newMetric(ptr: ?*anyopaque, definition: Definition) error{OutOfMemory}!*anyopaque {
        const m = cast(ptr);
        const options: c.TemporalCoreMetricOptions = .{
            .name = bytes.ref(definition.name),
            .description = bytes.ref(definition.description),
            .unit = bytes.ref(definition.unit),
            .kind = @backingInt(definition.kind),
        };
        return c.temporal_core_metric_new(m.ptr, &options) orelse error.OutOfMemory;
    }

    fn freeMetric(_: ?*anyopaque, metric: *anyopaque) void {
        c.temporal_core_metric_free(@ptrCast(metric));
    }

    fn newAttributes(ptr: ?*anyopaque, base: ?*anyopaque, attributes: []const Attribute) error{OutOfMemory}!*anyopaque {
        const m = cast(ptr);
        // The core reads the array as a Rust slice, which may not be null even when empty.
        var buf: [16]c.TemporalCoreMetricAttribute = undefined;
        const list = if (attributes.len <= buf.len) buf[0..attributes.len] else try m.gpa.alloc(c.TemporalCoreMetricAttribute, attributes.len);
        defer if (attributes.len > buf.len) m.gpa.free(list);
        for (attributes, list) |a, *out| out.* = coreAttribute(a);
        const set = if (base) |b|
            c.temporal_core_metric_attributes_new_append(m.ptr, @ptrCast(b), list.ptr, list.len)
        else
            c.temporal_core_metric_attributes_new(m.ptr, list.ptr, list.len);
        return set orelse error.OutOfMemory;
    }

    fn freeAttributes(_: ?*anyopaque, attributes: *anyopaque) void {
        c.temporal_core_metric_attributes_free(@ptrCast(attributes));
    }

    // The core panics (aborting the process) when a value's type doesn't match the metric's
    // kind; the typed metrics in `temporal.metrics` keep them matched.
    fn record(ptr: ?*anyopaque, metric: *anyopaque, value: Value, attributes: ?*anyopaque) void {
        const m = cast(ptr);
        const core_metric: *const c.TemporalCoreMetric = @ptrCast(metric);
        const set: *const c.TemporalCoreMetricAttributes = if (attributes) |a| @ptrCast(a) else m.defaults;
        switch (value) {
            .unsigned => |v| c.temporal_core_metric_record_integer(core_metric, v, set),
            .signed => |v| c.temporal_core_metric_record_integer_signed(core_metric, v, set),
            .float => |v| c.temporal_core_metric_record_float(core_metric, v, set),
            .duration => |d| c.temporal_core_metric_record_duration(core_metric, durationMs(d), set),
        }
    }
};

fn coreAttribute(a: Attribute) c.TemporalCoreMetricAttribute {
    return switch (a.value) {
        .string => |s| .{ .key = bytes.ref(a.key), .value = .{ .string_value = bytes.ref(s) }, .value_type = c.TemporalCoreMetricAttributeValueType_String },
        .int => |i| .{ .key = bytes.ref(a.key), .value = .{ .int_value = i }, .value_type = c.TemporalCoreMetricAttributeValueType_Int },
        .float => |f| .{ .key = bytes.ref(a.key), .value = .{ .float_value = f }, .value_type = c.TemporalCoreMetricAttributeValueType_Float },
        .bool => |b| .{ .key = bytes.ref(a.key), .value = .{ .bool_value = b }, .value_type = c.TemporalCoreMetricAttributeValueType_Bool },
    };
}

// ---------------------------------------------------------------- custom meters

/// The most custom meters alive in one process at a time.
pub const max_custom_meters = 32;

/// The core's custom meter callbacks, forwarding to a `VTable`. Create one with `create` and
/// pass it as `Runtime.Metrics.Exporter.custom`: the runtime then owns it and frees it with
/// its `meter_free` callback once the runtime and everything made from its meter are gone,
/// or when `Runtime.init` fails.
pub const CustomMeter = struct {
    core: c.TemporalCoreCustomMetricMeter,
    gpa: Allocator,
    ptr: ?*anyopaque,
    vtable: *const VTable,
    slot: usize,

    pub const CreateError = error{
        OutOfMemory,
        /// `max_custom_meters` runtimes with a custom meter already exist in this process.
        TooManyCustomMeters,
    };

    /// `ptr` and `vtable` must stay valid until the core frees the meter; they are called
    /// from the core's threads, concurrently. `gpa` must be thread-safe: the adapter
    /// allocates a small wrapper for each metric and attribute set the core creates.
    pub fn create(gpa: Allocator, ptr: ?*anyopaque, vtable: *const VTable) CreateError!*const c.TemporalCoreCustomMetricMeter {
        const m = try gpa.create(CustomMeter);
        errdefer gpa.destroy(m);
        m.* = .{ .core = undefined, .gpa = gpa, .ptr = ptr, .vtable = vtable, .slot = undefined };
        m.slot = for (&custom_slots, 0..) |*slot, i| {
            if (slot.cmpxchgStrong(null, m, .acq_rel, .monotonic) == null) break i;
        } else return error.TooManyCustomMeters;
        m.core = .{
            .metric_new = slot_callbacks[m.slot].metric_new,
            .metric_free = metricFree,
            .metric_record_integer = recordInteger,
            .metric_record_float = recordFloat,
            .metric_record_duration = recordDuration,
            .attributes_new = slot_callbacks[m.slot].attributes_new,
            .attributes_free = attributesFree,
            .meter_free = meterFree,
        };
        return &m.core;
    }

    fn metricNew(m: *CustomMeter, name: bytes.Ref, description: bytes.Ref, unit: bytes.Ref, raw_kind: c_uint) ?*const anyopaque {
        const kind = std.enums.fromInt(Kind, raw_kind) orelse return null;
        const metric = m.gpa.create(CustomMetric) catch return null;
        const user = m.vtable.newMetric(m.ptr, .{
            .name = bytes.slice(name),
            .description = bytes.slice(description),
            .unit = bytes.slice(unit),
            .kind = kind,
        }) catch {
            m.gpa.destroy(metric);
            return null;
        };
        metric.* = .{ .meter = m, .kind = kind, .user = user };
        return metric;
    }

    fn attributesNew(m: *CustomMeter, append_from: ?*const anyopaque, raw: [*c]const c.TemporalCoreCustomMetricAttribute, n: usize) ?*const anyopaque {
        var buf: [16]Attribute = undefined;
        const list = if (n <= buf.len) buf[0..n] else m.gpa.alloc(Attribute, n) catch return null;
        defer if (n > buf.len) m.gpa.free(list);
        for (list, 0..) |*a, i| a.* = zigAttribute(raw[i]) orelse return null;
        const set = m.gpa.create(CustomAttributes) catch return null;
        const base: ?*anyopaque = if (append_from) |p| @as(*const CustomAttributes, @ptrCast(@alignCast(p))).user else null;
        const user = m.vtable.newAttributes(m.ptr, base, list) catch {
            m.gpa.destroy(set);
            return null;
        };
        set.* = .{ .meter = m, .user = user };
        return set;
    }
};

const CustomMetric = struct {
    meter: *CustomMeter,
    kind: Kind,
    user: *anyopaque,
};

const CustomAttributes = struct {
    meter: *CustomMeter,
    user: *anyopaque,
};

var custom_slots: [max_custom_meters]std.atomic.Value(?*CustomMeter) = @splat(.init(null));

// `metric_new` and `attributes_new` get no meter pointer, so each meter takes a slot whose
// callbacks find it. The other callbacks reach the meter through the wrappers the core
// passes back.
const SlotCallbacks = struct {
    metric_new: c.TemporalCoreCustomMetricMeterMetricNewCallback,
    attributes_new: c.TemporalCoreCustomMetricMeterAttributesNewCallback,
};

const slot_callbacks = blk: {
    var callbacks: [max_custom_meters]SlotCallbacks = undefined;
    for (&callbacks, 0..) |*cb, i| cb.* = .{ .metric_new = &SlotTrampoline(i).metricNew, .attributes_new = &SlotTrampoline(i).attributesNew };
    break :blk callbacks;
};

fn SlotTrampoline(comptime i: usize) type {
    return struct {
        fn metricNew(name: bytes.Ref, description: bytes.Ref, unit: bytes.Ref, kind: c.TemporalCoreMetricKind) callconv(.c) ?*const anyopaque {
            const m = custom_slots[i].load(.acquire) orelse return null;
            return m.metricNew(name, description, unit, kind);
        }

        fn attributesNew(append_from: ?*const anyopaque, attributes: [*c]const c.TemporalCoreCustomMetricAttribute, n: usize) callconv(.c) ?*const anyopaque {
            const m = custom_slots[i].load(.acquire) orelse return null;
            return m.attributesNew(append_from, attributes, n);
        }
    };
}

fn zigAttribute(raw: c.TemporalCoreCustomMetricAttribute) ?Attribute {
    const value: AttributeValue = switch (raw.value_type) {
        c.TemporalCoreMetricAttributeValueType_String => .{ .string = if (raw.value.string_value.size == 0) "" else raw.value.string_value.data[0..raw.value.string_value.size] },
        c.TemporalCoreMetricAttributeValueType_Int => .{ .int = raw.value.int_value },
        c.TemporalCoreMetricAttributeValueType_Float => .{ .float = raw.value.float_value },
        c.TemporalCoreMetricAttributeValueType_Bool => .{ .bool = raw.value.bool_value },
        else => return null,
    };
    return .{ .key = bytes.slice(raw.key), .value = value };
}

fn customMetric(metric: ?*const anyopaque) ?*const CustomMetric {
    return @ptrCast(@alignCast(metric orelse return null));
}

fn userAttributes(attributes: ?*const anyopaque) ?*anyopaque {
    const set: *const CustomAttributes = @ptrCast(@alignCast(attributes orelse return null));
    return set.user;
}

fn metricFree(metric: ?*const anyopaque) callconv(.c) void {
    const cm = customMetric(metric) orelse return;
    const m = cm.meter;
    m.vtable.freeMetric(m.ptr, cm.user);
    m.gpa.destroy(cm);
}

fn recordInteger(metric: ?*const anyopaque, value: u64, attributes: ?*const anyopaque) callconv(.c) void {
    const cm = customMetric(metric) orelse return;
    // Up-down counters pass their signed value through the unsigned callback.
    const v: Value = if (cm.kind == .up_down_counter) .{ .signed = @bitCast(value) } else .{ .unsigned = value };
    cm.meter.vtable.record(cm.meter.ptr, cm.user, v, userAttributes(attributes));
}

fn recordFloat(metric: ?*const anyopaque, value: f64, attributes: ?*const anyopaque) callconv(.c) void {
    const cm = customMetric(metric) orelse return;
    cm.meter.vtable.record(cm.meter.ptr, cm.user, .{ .float = value }, userAttributes(attributes));
}

fn recordDuration(metric: ?*const anyopaque, value_ms: u64, attributes: ?*const anyopaque) callconv(.c) void {
    const cm = customMetric(metric) orelse return;
    const ns = @as(i96, value_ms) * std.time.ns_per_ms;
    cm.meter.vtable.record(cm.meter.ptr, cm.user, .{ .duration = .{ .nanoseconds = ns } }, userAttributes(attributes));
}

fn attributesFree(attributes: ?*const anyopaque) callconv(.c) void {
    const set: *const CustomAttributes = @ptrCast(@alignCast(attributes orelse return));
    const m = set.meter;
    m.vtable.freeAttributes(m.ptr, set.user);
    m.gpa.destroy(set);
}

fn meterFree(meter: [*c]const c.TemporalCoreCustomMetricMeter) callconv(.c) void {
    const core: *const c.TemporalCoreCustomMetricMeter = meter orelse return;
    const m: *CustomMeter = @constCast(@fieldParentPtr("core", core));
    // The core calls `metric_new` and `attributes_new` only while it holds the meter, so the
    // slot is free to reuse once this runs.
    custom_slots[m.slot].store(null, .release);
    m.gpa.destroy(m);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

/// A thread-safe `VTable` that keeps what it receives, for tests. Attribute sets are flattened
/// at record time, later keys replacing earlier ones.
pub const TestRecorder = struct {
    gpa: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    arena: std.heap.ArenaAllocator,
    metrics: std.ArrayList(*Metric) = .empty,
    records: std.ArrayList(Record) = .empty,
    live_metrics: usize = 0,
    live_attributes: usize = 0,

    pub const Metric = struct { definition: Definition };
    const Set = struct { base: ?*const Set, list: []const Attribute };

    pub const Record = struct {
        metric: *const Metric,
        value: Value,
        attributes: []const Attribute,

        pub fn get(r: Record, key: []const u8) ?AttributeValue {
            var i = r.attributes.len;
            while (i > 0) {
                i -= 1;
                if (std.mem.eql(u8, r.attributes[i].key, key)) return r.attributes[i].value;
            }
            return null;
        }
    };

    pub fn init(gpa: Allocator) TestRecorder {
        return .{ .gpa = gpa, .arena = .init(gpa) };
    }

    pub fn deinit(r: *TestRecorder) void {
        r.metrics.deinit(r.gpa);
        r.records.deinit(r.gpa);
        r.arena.deinit();
    }

    pub const vtable: VTable = .{
        .newMetric = newMetric,
        .freeMetric = freeMetric,
        .newAttributes = newAttributes,
        .freeAttributes = freeAttributes,
        .record = record,
    };

    fn lock(r: *TestRecorder) void {
        while (!r.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// The records of the metric named `name`, copied into `out`'s memory.
    pub fn recordsOf(r: *TestRecorder, a: Allocator, name: []const u8) ![]Record {
        r.lock();
        defer r.mutex.unlock();
        var out: std.ArrayList(Record) = .empty;
        for (r.records.items) |rec| if (std.mem.eql(u8, rec.metric.definition.name, name)) try out.append(a, rec);
        return out.toOwnedSlice(a);
    }

    pub fn hasMetric(r: *TestRecorder, name: []const u8) bool {
        r.lock();
        defer r.mutex.unlock();
        for (r.metrics.items) |m| if (std.mem.eql(u8, m.definition.name, name)) return true;
        return false;
    }

    fn cast(ptr: ?*anyopaque) *TestRecorder {
        return @ptrCast(@alignCast(ptr.?));
    }

    fn newMetric(ptr: ?*anyopaque, definition: Definition) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        const m = try a.create(Metric);
        m.* = .{ .definition = .{
            .name = try a.dupe(u8, definition.name),
            .description = try a.dupe(u8, definition.description),
            .unit = try a.dupe(u8, definition.unit),
            .kind = definition.kind,
        } };
        try r.metrics.append(r.gpa, m);
        r.live_metrics += 1;
        return m;
    }

    fn freeMetric(ptr: ?*anyopaque, _: *anyopaque) void {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        r.live_metrics -= 1;
    }

    fn newAttributes(ptr: ?*anyopaque, base: ?*anyopaque, attributes: []const Attribute) error{OutOfMemory}!*anyopaque {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        const list = try a.alloc(Attribute, attributes.len);
        for (attributes, list) |in, *out| out.* = .{ .key = try a.dupe(u8, in.key), .value = switch (in.value) {
            .string => |s| .{ .string = try a.dupe(u8, s) },
            else => in.value,
        } };
        const set = try a.create(Set);
        set.* = .{ .base = @ptrCast(@alignCast(base)), .list = list };
        r.live_attributes += 1;
        return set;
    }

    fn freeAttributes(ptr: ?*anyopaque, _: *anyopaque) void {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        r.live_attributes -= 1;
    }

    fn record(ptr: ?*anyopaque, metric: *anyopaque, value: Value, attributes: ?*anyopaque) void {
        const r = cast(ptr);
        r.lock();
        defer r.mutex.unlock();
        const a = r.arena.allocator();
        var flat: std.ArrayList(Attribute) = .empty;
        var chain: std.ArrayList(*const Set) = .empty;
        var set: ?*const Set = @ptrCast(@alignCast(attributes));
        while (set) |s| : (set = s.base) chain.append(a, s) catch return;
        var i = chain.items.len;
        while (i > 0) {
            i -= 1;
            flat.appendSlice(a, chain.items[i].list) catch return;
        }
        r.records.append(r.gpa, .{ .metric = @ptrCast(@alignCast(metric)), .value = value, .attributes = flat.items }) catch return;
    }
};

test "attributes convert to the core's layout and back" {
    const in: []const Attribute = &.{
        .{ .key = "s", .value = .{ .string = "text" } },
        .{ .key = "empty", .value = .{ .string = "" } },
        .{ .key = "i", .value = .{ .int = -7 } },
        .{ .key = "f", .value = .{ .float = 2.5 } },
        .{ .key = "b", .value = .{ .bool = true } },
    };
    for (in) |a| {
        const core = coreAttribute(a);
        try testing.expectEqualStrings(a.key, bytes.slice(core.key));
        // The custom meter callbacks get the same union, with strings as data and size.
        var custom: c.TemporalCoreCustomMetricAttribute = .{ .key = core.key, .value = undefined, .value_type = core.value_type };
        switch (a.value) {
            .string => |s| custom.value = .{ .string_value = .{ .data = s.ptr, .size = s.len } },
            .int => |v| custom.value = .{ .int_value = v },
            .float => |v| custom.value = .{ .float_value = v },
            .bool => |v| custom.value = .{ .bool_value = v },
        }
        const back = zigAttribute(custom).?;
        try testing.expectEqualStrings(a.key, back.key);
        try testing.expectEqualDeep(a.value, back.value);
    }
    var unknown: c.TemporalCoreCustomMetricAttribute = .{ .key = bytes.ref("k"), .value = .{ .int_value = 0 }, .value_type = 99 };
    try testing.expectEqual(null, zigAttribute(unknown));
    unknown.value_type = c.TemporalCoreMetricAttributeValueType_Int;
    try testing.expect(zigAttribute(unknown) != null);
}

test "durations are recorded as whole milliseconds, clamped" {
    try testing.expectEqual(1500, durationMs(.fromMicroseconds(1_500_900)));
    try testing.expectEqual(0, durationMs(.fromSeconds(-1)));
    try testing.expectEqual(std.math.maxInt(u64), durationMs(.max));
}

test "a runtime without metrics has no meter" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    try testing.expectEqual(null, try Meter.create(testing.allocator, rt));
}

test "the core meter records to a custom meter and back through its callbacks" {
    var recorder: TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    const custom = try CustomMeter.create(testing.allocator, &recorder, &TestRecorder.vtable);
    const rt = try Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .custom = custom } },
        .worker_heartbeat_interval_ms = 0,
    }, null);
    {
        defer rt.deinit();
        const meter = (try Meter.create(testing.allocator, rt)).?;
        defer meter.destroy();
        const v = &Meter.vtable;

        const counter = try v.newMetric(meter, .{ .name = "requests", .description = "Requests served", .unit = "1", .kind = .counter });
        defer v.freeMetric(meter, counter);
        const updown = try v.newMetric(meter, .{ .name = "in_flight", .kind = .up_down_counter });
        defer v.freeMetric(meter, updown);
        const latency = try v.newMetric(meter, .{ .name = "latency", .kind = .histogram_duration });
        defer v.freeMetric(meter, latency);
        const ratio = try v.newMetric(meter, .{ .name = "ratio", .kind = .gauge_float });
        defer v.freeMetric(meter, ratio);

        const base = try v.newAttributes(meter, null, &.{.{ .key = "region", .value = .{ .string = "west" } }});
        defer v.freeAttributes(meter, base);
        const more = try v.newAttributes(meter, base, &.{
            .{ .key = "region", .value = .{ .string = "east" } },
            .{ .key = "shard", .value = .{ .int = 3 } },
            .{ .key = "hot", .value = .{ .bool = true } },
        });
        defer v.freeAttributes(meter, more);

        v.record(meter, counter, .{ .unsigned = 2 }, base);
        v.record(meter, updown, .{ .signed = -4 }, more);
        v.record(meter, latency, .{ .duration = .fromMilliseconds(250) }, null);
        v.record(meter, ratio, .{ .float = 0.75 }, more);

        var arena: std.heap.ArenaAllocator = .init(testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        const requests = try recorder.recordsOf(a, "requests");
        try testing.expectEqual(1, requests.len);
        try testing.expectEqual(Kind.counter, requests[0].metric.definition.kind);
        try testing.expectEqualStrings("Requests served", requests[0].metric.definition.description);
        try testing.expectEqualStrings("1", requests[0].metric.definition.unit);
        try testing.expectEqual(Value{ .unsigned = 2 }, requests[0].value);
        try testing.expectEqualStrings("west", requests[0].get("region").?.string);
        // The runtime's default attribute comes first in every set.
        try testing.expectEqualStrings("temporal-core-sdk", requests[0].get("service_name").?.string);

        const in_flight = try recorder.recordsOf(a, "in_flight");
        try testing.expectEqual(Value{ .signed = -4 }, in_flight[0].value);
        try testing.expectEqualStrings("east", in_flight[0].get("region").?.string);
        try testing.expectEqual(3, in_flight[0].get("shard").?.int);
        try testing.expect(in_flight[0].get("hot").?.bool);

        const latencies = try recorder.recordsOf(a, "latency");
        try testing.expectEqual(250, durationMs(latencies[0].value.duration));
        try testing.expectEqualStrings("temporal-core-sdk", latencies[0].get("service_name").?.string);

        const ratios = try recorder.recordsOf(a, "ratio");
        try testing.expectEqual(0.75, ratios[0].value.float);
    }
    // Freeing the runtime released every metric and attribute set, and the meter itself.
    try testing.expectEqual(0, recorder.live_metrics);
    try testing.expectEqual(0, recorder.live_attributes);
    for (custom_slots) |slot| try testing.expect(slot.load(.acquire) == null);
}

test "a custom meter is freed when the runtime rejects its options" {
    var recorder: TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    const custom = try CustomMeter.create(testing.allocator, &recorder, &TestRecorder.vtable);
    // Rejected before the core sees the meter; the leak check covers the meter's memory.
    try testing.expectError(error.InvalidOptions, Runtime.init(testing.allocator, .{
        .metrics = .{ .exporter = .{ .custom = custom } },
        .worker_heartbeat_interval_ms = 1,
    }, null));
    for (custom_slots) |slot| try testing.expect(slot.load(.acquire) == null);
}
