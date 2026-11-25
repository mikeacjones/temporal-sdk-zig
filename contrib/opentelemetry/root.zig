//! OpenTelemetry tracing for the Temporal SDK: a `temporal.tracing.Tracer` on an
//! OpenTelemetry tracer from open-telemetry/opentelemetry-zig, whose spans the caller's
//! provider exports (over OTLP HTTP/protobuf with upstream's `trace.OTLPExporter`).
//!
//!     var otel_tracer: temporal_opentelemetry.Tracer = .init(gpa, io, try provider.getTracer(.{ .name = "orders" }));
//!     defer otel_tracer.deinit(); // after provider.shutdown()
//!     var tracing: temporal.tracing.Interceptor = .init(otel_tracer.tracer());
//!
//! Span contexts travel as W3C trace context: the fields `traceparent` and, when not empty,
//! `tracestate`.
//!
//! Upstream borrows a span's name, attribute keys and values and status description until
//! the span has been exported, which happens after it ends, so this tracer keeps its copies
//! of them until `Tracer.deinit`: memory grows by the size of those strings (tens of bytes)
//! for every span the tracer starts.

const std = @import("std");
const temporal = @import("temporal");
const otel = @import("opentelemetry-sdk");
const Allocator = std.mem.Allocator;
const tracing = temporal.tracing;
const trace = otel.api.trace;

const log = std.log.scoped(.temporal_opentelemetry);

/// A `temporal.tracing.Tracer` on an OpenTelemetry tracer. Thread-safe.
pub const Tracer = struct {
    gpa: Allocator,
    io: std.Io,
    otel_tracer: *trace.Tracer,
    /// Held around upstream's `startSpan`, which draws span IDs outside any lock of its own.
    start_mutex: std.Io.Mutex = .init,
    /// Guards `kept`.
    kept_mutex: std.Io.Mutex = .init,
    /// The copies of span names, attribute keys and values and failure texts, which upstream
    /// borrows until it has exported the span. Freed by `deinit`.
    kept: std.ArrayList([]u8) = .empty,

    /// `gpa` holds each span, and the copies of its strings, and must be thread-safe.
    /// `tracer` comes from the caller's `otel.trace.TracerProvider.getTracer` and is
    /// borrowed, as is `io`. Release with `deinit` after the provider has been shut down.
    pub fn init(gpa: Allocator, io: std.Io, tracer_: *trace.Tracer) Tracer {
        return .{ .gpa = gpa, .io = io, .otel_tracer = tracer_ };
    }

    /// Frees the strings of finished spans. Call it after the provider's `shutdown` (so
    /// every span has been exported) and after the Temporal client and workers are closed.
    pub fn deinit(t: *Tracer) void {
        for (t.kept.items) |block| t.gpa.free(block);
        t.kept.deinit(t.gpa);
        t.* = undefined;
    }

    /// The interface to give `temporal.tracing.Interceptor.init`. `t` must not move.
    pub fn tracer(t: *Tracer) tracing.Tracer {
        return tracing.Tracer.from(Tracer, t);
    }

    /// Keeps `block` until `deinit`.
    fn keep(t: *Tracer, block: []u8) error{OutOfMemory}!void {
        t.kept_mutex.lockUncancelable(t.io);
        defer t.kept_mutex.unlock(t.io);
        try t.kept.append(t.gpa, block);
    }

    pub fn startSpan(t: *Tracer, arena: Allocator, options: tracing.StartSpanOptions) error{OutOfMemory}!tracing.Span {
        // One block holds the span's name and its tags' names and values.
        const name = try options.spanName(arena);
        var size = name.len;
        for (options.tags) |tag| size += tag.name.len + tag.value.len;
        const block = try t.gpa.alloc(u8, size);
        // Kept from here on, whatever happens to the span: upstream may have borrowed it.
        t.keep(block) catch |err| {
            t.gpa.free(block);
            return err;
        };
        var rest = block;
        const owned_name = take(&rest, name);
        const attributes = try arena.alloc(otel.Attribute, options.tags.len);
        for (attributes, options.tags) |*attribute, tag| {
            const key = take(&rest, tag.name);
            attribute.* = .{ .key = key, .value = .{ .string = take(&rest, tag.value) } };
        }

        var start: trace.Tracer.StartOptions = .{
            .kind = switch (options.operation.kind()) {
                .client => .Client,
                .server => .Server,
            },
            .attributes = attributes,
            .start_timestamp = if (options.start_time) |ts| std.math.cast(u64, ts.nanoseconds) else null,
        };
        const remote = if (options.parent) |p| try spanContext(arena, p) else null;
        var parent_context: ?otel.api.context.Context = null;
        if (remote) |r| {
            parent_context = try trace.insertSpanContext(t.gpa, r);
            start.parent_context = parent_context;
        } else {
            start.root = true;
        }
        defer if (parent_context) |*c| {
            trace.freeSerializedSpanContext(t.gpa, c.*);
            c.deinit();
        };

        const span = try t.gpa.create(trace.Span);
        errdefer t.gpa.destroy(span);
        span.* = blk: {
            t.start_mutex.lockUncancelable(t.io);
            defer t.start_mutex.unlock(t.io);
            break :blk t.otel_tracer.startSpan(t.gpa, owned_name, start) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => {
                    log.warn("starting the OpenTelemetry span {s} failed: {t}; the call goes on untraced", .{ owned_name, err });
                    t.gpa.destroy(span);
                    return .{};
                },
            };
        };
        const context = fromSpanContext(arena, span.span_context) catch |err| {
            span.end(null);
            span.deinit();
            return err;
        };
        return .{ .handle = span, .context = context };
    }

    pub fn finishSpan(t: *Tracer, s: tracing.Span, options: tracing.FinishSpanOptions) void {
        const span: *trace.Span = @ptrCast(@alignCast(s.handle orelse return));
        if (options.failure) |failure| {
            const description: []const u8 = if (t.gpa.dupe(u8, failure)) |copy| blk: {
                t.keep(copy) catch {
                    t.gpa.free(copy);
                    break :blk "out of memory recording the failure";
                };
                break :blk copy;
            } else |_| "out of memory recording the failure";
            span.setStatus(trace.Status.error_with_description(description));
        }
        span.end(null);
        span.deinit();
        t.gpa.destroy(span);
    }
};

/// The first `text.len` bytes of `rest`, filled with `text`; `rest` moves past them.
fn take(rest: *[]u8, text: []const u8) []const u8 {
    const out = rest.*[0..text.len];
    @memcpy(out, text);
    rest.* = rest.*[text.len..];
    return out;
}

const fields_getter: otel.api.propagation.TextMapGetter(tracing.SpanContext) = .{
    .getFn = struct {
        fn get(c: *const tracing.SpanContext, name: []const u8) ?[]const u8 {
            return c.get(name);
        }
    }.get,
    .keysFn = struct {
        fn keys(_: *const tracing.SpanContext) []const []const u8 {
            return &.{};
        }
    }.keys,
};

/// The OpenTelemetry span context in `context`, marked remote, or null when it has no
/// valid `traceparent`. For code that parents its own OpenTelemetry spans under the
/// current Temporal span. Trace state memory comes from `arena`.
pub fn spanContext(arena: Allocator, context: tracing.SpanContext) error{OutOfMemory}!?trace.SpanContext {
    return trace.propagator.extract(arena, &context, fields_getter) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
    };
}

const Fields = struct {
    arena: Allocator,
    list: std.ArrayList(tracing.Field) = .empty,
};

const fields_setter: otel.api.propagation.TextMapSetter(Fields) = .{
    .setFn = struct {
        fn set(f: *Fields, name: []const u8, value: []const u8) anyerror!void {
            try f.list.append(f.arena, .{ .name = name, .value = value });
        }
    }.set,
};

/// The fields for `span_context`, allocated from `arena`; empty when it is not valid. For
/// code that puts its own span into a `propagation.Values` under `temporal.tracing.span_key`.
pub fn fromSpanContext(arena: Allocator, span_context: trace.SpanContext) error{OutOfMemory}!tracing.SpanContext {
    var fields: Fields = .{ .arena = arena };
    trace.propagator.inject(arena, span_context, &fields, fields_setter) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => unreachable, // The setter fails only for lack of memory.
    };
    return .{ .fields = fields.list.items };
}

test {
    _ = @import("root_test.zig");
}
