//! Metrics: counters, histograms, and gauges, recorded with attributes.
//!
//! A client exports metrics when `client.Options.metrics` names an exporter: a Prometheus
//! endpoint, an OpenTelemetry collector, or a custom `Handler`. Workers on that client export
//! sdk-core's own metrics (workflow and activity completions, task latencies, poll counts,
//! and so on) through it, and code records its own through a `Handler`:
//!
//!     fn charge(ctx: *temporal.activity.Context, cents: u64) !void {
//!         const charges = try ctx.metrics().counter("payment_charges", .{ .description = "Charges made" });
//!         defer charges.deinit();
//!         charges.add(1, .{ .currency = "usd" });
//!         ...
//!     }
//!
//! `client.metrics()` records with the runtime's default attributes (`service_name`), an
//! activity's `ctx.metrics()` adds `namespace`, `task_queue`, `activity_type`, and
//! `workflow_type`, and a workflow's adds `namespace`, `task_queue`, and `workflow_type` (the
//! attributes sdk-core's own metrics carry) and records nothing while
//! the workflow replays history, so each value is recorded once. Without an exporter every
//! handler is `Handler.noop`.
//!
//! Attributes are a struct literal (`.{ .currency = "usd", .retries = 3 }`; strings,
//! integers, floats, bools, and enums, which are recorded by tag name) or a slice of
//! `Attribute`.
//!
//! Recording is thread-safe. The core's exporters keep their own state; a custom handler is
//! called from sdk-core's threads and from every thread that records, concurrently.
//!
//! A custom handler implements `VTable`: it creates metrics and attribute sets as opaque
//! handles and receives each recorded value:
//!
//!     const Statsd = struct {
//!         fn handler(s: *Statsd) temporal.metrics.Handler {
//!             return .{ .ptr = s, .vtable = &.{
//!                 .newMetric = newMetric, // allocate a handle holding the definition
//!                 .freeMetric = freeMetric,
//!                 .newAttributes = newAttributes, // copy `base`'s attributes and the new ones
//!                 .freeAttributes = freeAttributes,
//!                 .record = record, // send the value with the metric and attribute set
//!             } };
//!         }
//!         ...
//!     };
//!
//!     var client = try Client.connect(gpa, io, .{ .metrics = .{ .exporter = .{ .custom = statsd.handler() } } });

const std = @import("std");
const bridge = @import("bridge.zig");
const Allocator = std.mem.Allocator;
const Duration = std.Io.Duration;

pub const Kind = bridge.metric.Kind;
/// A recorded value as a custom handler receives it; its tag follows the metric's `Kind`.
pub const Value = bridge.metric.Value;
pub const Attribute = bridge.metric.Attribute;
pub const AttributeValue = bridge.metric.AttributeValue;
/// A metric's name, description, unit, and kind, as a custom handler receives it.
pub const Definition = bridge.metric.Definition;
/// What a custom `Handler` implements. See the namespace documentation.
pub const VTable = bridge.metric.VTable;
/// A key-value pair added to every metric (`Options.global_tags`).
pub const Tag = bridge.bytes.MetadataEntry;

/// Where a client's metrics go (`client.Options.metrics`).
pub const Options = struct {
    exporter: Exporter,
    /// Attributes added to every metric. Applied by the Prometheus and OpenTelemetry
    /// exporters; a custom handler adds its own, so they must be empty with `.custom`.
    global_tags: []const Tag = &.{},
    /// Prefix of sdk-core's metric names. Empty uses "temporal_". Metrics recorded through
    /// a `Handler` keep their names.
    metric_prefix: []const u8 = "",
    /// Add a `service_name` attribute ("temporal-core-sdk") to every metric.
    attach_service_name: bool = true,

    pub const Exporter = union(enum) {
        /// Serve metrics for Prometheus to scrape.
        prometheus: Prometheus,
        /// Push metrics to an OpenTelemetry collector.
        opentelemetry: OpenTelemetry,
        /// Hand every metric, sdk-core's included, to a custom handler. Borrowed: it must
        /// outlive the client and every worker on it. Attributes from `with` are not used.
        custom: Handler,
    };

    pub const Prometheus = struct {
        /// Address to serve the scrape endpoint on, such as "127.0.0.1:9464".
        bind_address: []const u8,
        /// Add `_total` to counter names.
        counters_total_suffix: bool = false,
        /// Add the unit to metric names.
        unit_suffix: bool = false,
        /// Record duration histograms in seconds instead of milliseconds.
        durations_as_seconds: bool = false,
        histogram_bucket_overrides: []const BucketOverride = &.{},
    };

    pub const OpenTelemetry = struct {
        /// Collector URL, such as "http://localhost:4317".
        url: []const u8,
        /// Headers sent with every export.
        headers: []const Tag = &.{},
        /// How often to export. Null uses sdk-core's default.
        periodicity: ?Duration = null,
        temporality: enum { cumulative, delta } = .cumulative,
        protocol: enum { grpc, http } = .grpc,
        /// Record duration histograms in seconds instead of milliseconds.
        durations_as_seconds: bool = false,
        histogram_bucket_overrides: []const BucketOverride = &.{},
    };

    /// The bucket boundaries (`buckets`) of one histogram, by metric name (`metric`).
    pub const BucketOverride = bridge.Runtime.Metrics.BucketOverride;
};

/// Options of a new metric.
pub const MetricOptions = struct {
    description: []const u8 = "",
    unit: []const u8 = "",
};

/// Creates metrics and records them, with attributes added to everything recorded through
/// it. A value type: copies share the implementation and the attribute set.
pub const Handler = struct {
    ptr: ?*anyopaque,
    vtable: *const VTable,
    /// The attribute set added to every record; null for none.
    attributes: ?*anyopaque = null,
    /// When set and true, records are dropped (the workflow is replaying).
    replaying: ?*const bool = null,
    /// Set on handlers from `with`, which `deinit` frees.
    owned: bool = false,

    /// Records nothing.
    pub const noop: Handler = .{ .ptr = null, .vtable = &noop_vtable };

    /// Returns a handler that adds `attributes` (a struct literal or slice of `Attribute`) to
    /// everything recorded through it, after this handler's. Free it with `deinit` once the
    /// metrics created from it are freed.
    pub fn with(h: Handler, attributes: anytype) error{OutOfMemory}!Handler {
        var buf: AttributeArray(@TypeOf(attributes)) = undefined;
        const list = attributeList(&buf, attributes);
        var result = h;
        result.attributes = try h.vtable.newAttributes(h.ptr, h.attributes, list);
        result.owned = true;
        return result;
    }

    /// Frees a handler from `with`. Does nothing for other handlers (`client.metrics()`,
    /// `ctx.metrics()`), which their owner frees.
    pub fn deinit(h: Handler) void {
        if (h.owned) if (h.attributes) |a| h.vtable.freeAttributes(h.ptr, a);
    }

    /// A monotonic counter of whole numbers.
    pub fn counter(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!Counter {
        return .init(h, name, options);
    }

    /// A counter that also goes down.
    pub fn upDownCounter(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!UpDownCounter {
        return .init(h, name, options);
    }

    /// A distribution of whole numbers.
    pub fn histogram(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!Histogram {
        return .init(h, name, options);
    }

    pub fn histogramFloat(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!HistogramFloat {
        return .init(h, name, options);
    }

    /// A distribution of durations, kept as whole milliseconds (or seconds with
    /// `durations_as_seconds`).
    pub fn histogramDuration(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!HistogramDuration {
        return .init(h, name, options);
    }

    /// The last value set, a whole number.
    pub fn gauge(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!Gauge {
        return .init(h, name, options);
    }

    pub fn gaugeFloat(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!GaugeFloat {
        return .init(h, name, options);
    }
};

pub const Counter = Metric(.counter);
pub const UpDownCounter = Metric(.up_down_counter);
pub const Histogram = Metric(.histogram);
pub const HistogramFloat = Metric(.histogram_float);
pub const HistogramDuration = Metric(.histogram_duration);
pub const Gauge = Metric(.gauge);
pub const GaugeFloat = Metric(.gauge_float);

/// A metric created by a `Handler`. Counters `add`, histograms `record`, and gauges `set`;
/// each takes the value and attributes (a struct literal or slice of `Attribute`) added after
/// the handler's. Free it with `deinit` before its handler. Recording never fails: a value
/// whose attributes can't be allocated is dropped.
pub fn Metric(comptime kind: Kind) type {
    return struct {
        const Self = @This();

        handler: Handler,
        metric: *anyopaque,

        pub const Input = switch (kind) {
            .counter, .histogram, .gauge => u64,
            .up_down_counter => i64,
            .histogram_float, .gauge_float => f64,
            .histogram_duration => Duration,
        };

        fn init(h: Handler, name: []const u8, options: MetricOptions) error{OutOfMemory}!Self {
            var handler = h;
            handler.owned = false;
            return .{ .handler = handler, .metric = try h.vtable.newMetric(h.ptr, .{
                .name = name,
                .description = options.description,
                .unit = options.unit,
                .kind = kind,
            }) };
        }

        pub fn deinit(m: Self) void {
            m.handler.vtable.freeMetric(m.handler.ptr, m.metric);
        }

        pub fn add(m: Self, value: Input, attributes: anytype) void {
            comptime if (kind != .counter and kind != .up_down_counter) @compileError("only counters add; use " ++ (if (kind == .gauge or kind == .gauge_float) "set" else "record"));
            m.put(value, attributes);
        }

        pub fn record(m: Self, value: Input, attributes: anytype) void {
            comptime if (kind != .histogram and kind != .histogram_float and kind != .histogram_duration) @compileError("only histograms record; use " ++ (if (kind == .gauge or kind == .gauge_float) "set" else "add"));
            m.put(value, attributes);
        }

        pub fn set(m: Self, value: Input, attributes: anytype) void {
            comptime if (kind != .gauge and kind != .gauge_float) @compileError("only gauges set; use " ++ (if (kind == .counter or kind == .up_down_counter) "add" else "record"));
            m.put(value, attributes);
        }

        fn put(m: Self, input: Input, attributes: anytype) void {
            const h = m.handler;
            if (h.replaying) |r| if (r.*) return;
            const value: Value = switch (kind) {
                .counter, .histogram, .gauge => .{ .unsigned = input },
                .up_down_counter => .{ .signed = input },
                .histogram_float, .gauge_float => .{ .float = input },
                .histogram_duration => .{ .duration = input },
            };
            var buf: AttributeArray(@TypeOf(attributes)) = undefined;
            const list = attributeList(&buf, attributes);
            if (list.len == 0) return h.vtable.record(h.ptr, m.metric, value, h.attributes);
            const set_ = h.vtable.newAttributes(h.ptr, h.attributes, list) catch return;
            defer h.vtable.freeAttributes(h.ptr, set_);
            h.vtable.record(h.ptr, m.metric, value, set_);
        }
    };
}

fn isAttributeSlice(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => |p| switch (p.size) {
            .slice => p.child == Attribute,
            .one => @typeInfo(p.child) == .array and @typeInfo(p.child).array.child == Attribute,
            else => false,
        },
        else => false,
    };
}

/// Room for the attributes of a struct literal; nothing for a slice.
fn AttributeArray(comptime T: type) type {
    if (isAttributeSlice(T)) return [0]Attribute;
    return switch (@typeInfo(T)) {
        .@"struct" => |s| [s.field_names.len]Attribute,
        else => @compileError("metric attributes must be a struct literal or a slice of temporal.metrics.Attribute, not " ++ @typeName(T)),
    };
}

fn attributeList(buf: anytype, attributes: anytype) []const Attribute {
    const T = @TypeOf(attributes);
    if (comptime isAttributeSlice(T)) return attributes;
    const names = @typeInfo(T).@"struct".field_names;
    inline for (names, 0..) |name, i| buf[i] = .{ .key = name, .value = attributeValue(name, @field(attributes, name)) };
    return buf;
}

fn attributeValue(comptime name: []const u8, v: anytype) AttributeValue {
    const V = @TypeOf(v);
    if (V == AttributeValue) return v;
    return switch (@typeInfo(V)) {
        .bool => .{ .bool = v },
        .int, .comptime_int => .{ .int = std.math.lossyCast(i64, v) },
        .float, .comptime_float => .{ .float = std.math.lossyCast(f64, v) },
        .@"enum", .enum_literal => .{ .string = @tagName(v) },
        .pointer => |p| if ((p.size == .slice and p.child == u8) or
            (p.size == .one and @typeInfo(p.child) == .array and @typeInfo(p.child).array.child == u8))
            .{ .string = v }
        else
            @compileError("metric attribute ." ++ name ++ " must be a string, integer, float, bool, or enum, not " ++ @typeName(V)),
        else => @compileError("metric attribute ." ++ name ++ " must be a string, integer, float, bool, or enum, not " ++ @typeName(V)),
    };
}

var noop_metric: u8 = 0;

const noop_vtable: VTable = .{
    .newMetric = noopNew,
    .freeMetric = noopFree,
    .newAttributes = noopNewAttributes,
    .freeAttributes = noopFree,
    .record = noopRecord,
};

fn noopNew(_: ?*anyopaque, _: Definition) error{OutOfMemory}!*anyopaque {
    return &noop_metric;
}

fn noopNewAttributes(_: ?*anyopaque, _: ?*anyopaque, _: []const Attribute) error{OutOfMemory}!*anyopaque {
    return &noop_metric;
}

fn noopFree(_: ?*anyopaque, _: *anyopaque) void {}

fn noopRecord(_: ?*anyopaque, _: *anyopaque, _: Value, _: ?*anyopaque) void {}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const TestRecorder = bridge.metric.TestRecorder;

test "struct literal attributes convert by field name and type" {
    const Color = enum { red, blue };
    var buf: AttributeArray(@TypeOf(.{ .s = "x", .i = @as(u32, 7), .big = @as(u64, std.math.maxInt(u64)), .f = 1.5, .b = false, .e = Color.blue, .lit = .green, .raw = AttributeValue{ .int = -1 } })) = undefined;
    const name: []const u8 = "dyn";
    const list = attributeList(&buf, .{ .s = name, .i = @as(u32, 7), .big = @as(u64, std.math.maxInt(u64)), .f = 1.5, .b = false, .e = Color.blue, .lit = .green, .raw = AttributeValue{ .int = -1 } });
    try testing.expectEqual(8, list.len);
    try testing.expectEqualStrings("s", list[0].key);
    try testing.expectEqualStrings("dyn", list[0].value.string);
    try testing.expectEqual(7, list[1].value.int);
    try testing.expectEqual(std.math.maxInt(i64), list[2].value.int);
    try testing.expectEqual(1.5, list[3].value.float);
    try testing.expectEqual(false, list[4].value.bool);
    try testing.expectEqualStrings("blue", list[5].value.string);
    try testing.expectEqualStrings("green", list[6].value.string);
    try testing.expectEqual(-1, list[7].value.int);

    const slice: []const Attribute = &.{.{ .key = "k", .value = .{ .string = "v" } }};
    var none: AttributeArray(@TypeOf(slice)) = undefined;
    try testing.expectEqual(slice.ptr, attributeList(&none, slice).ptr);
    var empty: AttributeArray(@TypeOf(.{})) = undefined;
    try testing.expectEqual(0, attributeList(&empty, .{}).len);
}

test "a custom handler receives metrics, attribute chains, and typed values" {
    var recorder: TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    const root: Handler = .{ .ptr = &recorder, .vtable = &TestRecorder.vtable };
    const scoped = try root.with(.{ .region = "west" });
    defer scoped.deinit();

    const runs = try scoped.counter("runs", .{ .description = "Runs", .unit = "1" });
    defer runs.deinit();
    runs.add(3, .{ .region = "east", .ok = true });
    runs.add(1, .{});
    const depth = try scoped.upDownCounter("depth", .{});
    defer depth.deinit();
    depth.add(-2, .{});
    const sizes = try root.histogram("sizes", .{});
    defer sizes.deinit();
    sizes.record(42, &[_]Attribute{.{ .key = "kind", .value = .{ .string = "blob" } }});
    const waits = try root.histogramDuration("waits", .{ .unit = "ms" });
    defer waits.deinit();
    waits.record(.fromMilliseconds(12), .{});
    const temp = try root.gaugeFloat("temp", .{});
    defer temp.deinit();
    temp.set(21.5, .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const run_records = try recorder.recordsOf(a, "runs");
    try testing.expectEqual(2, run_records.len);
    try testing.expectEqual(Kind.counter, run_records[0].metric.definition.kind);
    try testing.expectEqualStrings("Runs", run_records[0].metric.definition.description);
    try testing.expectEqual(Value{ .unsigned = 3 }, run_records[0].value);
    try testing.expectEqualStrings("east", run_records[0].get("region").?.string);
    try testing.expect(run_records[0].get("ok").?.bool);
    try testing.expectEqualStrings("west", run_records[1].get("region").?.string);
    try testing.expectEqual(Value{ .signed = -2 }, (try recorder.recordsOf(a, "depth"))[0].value);
    const size_records = try recorder.recordsOf(a, "sizes");
    try testing.expectEqualStrings("blob", size_records[0].get("kind").?.string);
    try testing.expectEqual(null, size_records[0].get("region"));
    try testing.expectEqual(12, bridge.metric.durationMs((try recorder.recordsOf(a, "waits"))[0].value.duration));
    try testing.expectEqual(Value{ .float = 21.5 }, (try recorder.recordsOf(a, "temp"))[0].value);
    // Each record's own attribute set is freed after the record; only `scoped`'s is live.
    try testing.expectEqual(1, recorder.live_attributes);
}

test "a replaying handler drops records and noop records nothing" {
    var recorder: TestRecorder = .init(testing.allocator);
    defer recorder.deinit();
    var replaying = true;
    const h: Handler = .{ .ptr = &recorder, .vtable = &TestRecorder.vtable, .replaying = &replaying };
    const runs = try h.counter("runs", .{});
    defer runs.deinit();
    runs.add(1, .{ .a = 1 });
    replaying = false;
    runs.add(2, .{});
    const records = try recorder.recordsOf(testing.allocator, "runs");
    defer testing.allocator.free(records);
    try testing.expectEqual(1, records.len);
    try testing.expectEqual(Value{ .unsigned = 2 }, records[0].value);

    const scoped = try Handler.noop.with(.{ .a = 1 });
    defer scoped.deinit();
    const g = try scoped.gauge("g", .{});
    defer g.deinit();
    g.set(5, .{ .b = 2 });
}
