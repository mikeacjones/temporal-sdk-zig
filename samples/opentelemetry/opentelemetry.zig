//! OpenTelemetry tracing: the client and the worker trace through `temporal.tracing.Interceptor`
//! on the OpenTelemetry tracer of `temporal_opentelemetry`, so one trace runs from the
//! starter's `StartWorkflow:Workflow` span through `RunWorkflow:Workflow` and
//! `StartActivity:Activity` to the worker's `RunActivity:Activity`. The activity adds a span of
//! its own, `custom-span`, as a child of `RunActivity:Activity`, with two attributes and an
//! event.
//!
//! Both programs print their spans to stdout as JSON by default. With
//! OTEL_EXPORTER_OTLP_ENDPOINT set (for example http://localhost:4318, an OpenTelemetry
//! Collector or Jaeger), they export them over OTLP HTTP/protobuf instead; see
//! `setup.Options`.
//!
//! The activity's attributes (`isTrue`, `stringAttr`) and its event (`Done Activity`) are on
//! `custom-span`, which stays open for the whole activity: the activity's own
//! `RunActivity:Activity` span belongs to the tracing interceptor, which gives the activity its
//! context but not the span itself.

const std = @import("std");
const temporal = @import("temporal");
const temporal_opentelemetry = @import("temporal_opentelemetry");
const otel = @import("opentelemetry-sdk");
const workflow = temporal.workflow;
const activity = temporal.activity;
const trace = otel.api.trace;

pub const task_queue = "otel";
pub const setup = @import("opentelemetry_setup.zig");

fn helloWorkflow(ctx: *workflow.Context, name: []const u8) !void {
    const logger = ctx.logger();
    logger.info("HelloWorld workflow started", .{ .name = name });

    ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("HelloWorld workflow completed.", .{});
}

pub const Workflow = workflow.Def("Workflow", helloWorkflow);

/// The activity's state: the tracer it starts `custom-span` on. Borrowed; it must outlive the
/// worker.
pub const Activities = struct {
    /// Holds the span; thread-safe.
    gpa: std.mem.Allocator,
    tracer: *trace.Tracer,

    pub fn run(a: *Activities, ctx: *activity.Context, name: []const u8) !void {
        ctx.logger().info("Activity", .{ .name = name });

        var span = try a.startCustomSpan(ctx);
        defer if (span) |*s| {
            s.end(null);
            s.deinit();
        };

        const io = ctx.io();
        try io.sleep(.fromSeconds(1), .awake);
        try io.sleep(.fromSeconds(1), .awake);
        if (span) |*s| try s.addEvent("Done Activity", null, null);
    }

    /// `custom-span`, a child of the activity's `RunActivity` span (a root span when the
    /// activity runs untraced), or null when the tracer could not start it.
    fn startCustomSpan(a: *Activities, ctx: *activity.Context) error{OutOfMemory}!?trace.Span {
        const remote = if (ctx.propagated().get(temporal.tracing.span_key)) |parent|
            try temporal_opentelemetry.spanContext(ctx.allocator(), parent)
        else
            null;

        // The SDK borrows attribute keys and values until the span is exported.
        const attributes = [_]otel.Attribute{
            .{ .key = "isTrue", .value = .{ .bool = true } },
            .{ .key = "stringAttr", .value = .{ .string = "Ciao" } },
        };
        var start: trace.Tracer.StartOptions = .{ .attributes = &attributes };
        var parent_context: ?otel.api.context.Context = null;
        if (remote) |r| {
            parent_context = try trace.insertSpanContext(a.gpa, r);
            start.parent_context = parent_context;
        } else {
            start.root = true;
        }
        defer if (parent_context) |*c| {
            trace.freeSerializedSpanContext(a.gpa, c.*);
            c.deinit();
        };

        return a.tracer.startSpan(a.gpa, "custom-span", start) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => {
                ctx.logger().warn("Starting custom-span failed; it is not traced.", .{ .err = err });
                return null;
            },
        };
    }
};

pub const Activity = activity.Def("Activity", Activities.run);
