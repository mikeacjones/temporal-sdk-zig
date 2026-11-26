//! OpenTelemetry tracing in three parts, each on its own task queue with a workflow type
//! `Workflow`:
//!
//! - `automatic_instrumentation`: only the tracing interceptor's spans. The workflow runs
//!   `Activity`, which greets the name it is given. Its worker also serves metrics for
//!   Prometheus.
//! - `workflow_activity_propagation`: the workflow runs the activity inside a span of its
//!   own, `workflow-operation`, started with `temporal.tracing.startSpan`, so the activity's
//!   `StartActivity:Activity` span is its child. The activity opens an OpenTelemetry span,
//!   `activity-operation`, under its `RunActivity:Activity` span.
//! - `client_update_propagation`: the workflow takes update `greet`. Its validator, which
//!   rejects an empty name, opens `validate-update` under `ValidateUpdate:greet`, and its
//!   handler opens `handle-update` under `HandleUpdate:greet`. The starter sends the update
//!   under an OpenTelemetry span of its own, `send-update`, which is the root of the trace.
//!
//! A span started by workflow code is not started again when the workflow is replayed, and
//! is scoped to the coroutine that started it: a coroutine the workflow starts with
//! `io.async` or a `Group` does not run under it.

const std = @import("std");
const temporal = @import("temporal");
const temporal_opentelemetry = @import("temporal_opentelemetry");
const otel = @import("opentelemetry-sdk");
const workflow = temporal.workflow;
const activity = temporal.activity;
const tracing = temporal.tracing;
const trace = otel.api.trace;

pub const setup = @import("opentelemetry").setup;

fn greeting(a: std.mem.Allocator, name: []const u8) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(a, "Hello, {s}!", .{name});
}

pub const automatic_instrumentation = struct {
    pub const task_queue = "automatic-instrumentation";

    fn run(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        return ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
    }

    pub const Workflow = workflow.Def("Workflow", run);

    fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
        return greeting(ctx.allocator(), name);
    }

    pub const Activity = activity.Def("Activity", greet);
};

pub const workflow_activity_propagation = struct {
    pub const task_queue = "workflow-activity-propagation";

    fn run(ctx: *workflow.Context, name: []const u8) ![]const u8 {
        const span = tracing.startSpan(ctx, "workflow-operation", .{});
        defer span.end();

        return ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
    }

    pub const Workflow = workflow.Def("Workflow", run);

    /// The activity's state: the tracer it starts `activity-operation` on. Borrowed; it must
    /// outlive the worker.
    pub const Activities = struct {
        /// Holds the span; thread-safe.
        gpa: std.mem.Allocator,
        tracer: *trace.Tracer,

        pub fn greet(a: *Activities, ctx: *activity.Context, name: []const u8) ![]const u8 {
            var span = try a.startOperationSpan(ctx);
            defer if (span) |*s| {
                s.end(null);
                s.deinit();
            };
            return greeting(ctx.allocator(), name);
        }

        /// `activity-operation`, a child of the activity's `RunActivity` span (a root span
        /// when the activity runs untraced), or null when the tracer could not start it.
        fn startOperationSpan(a: *Activities, ctx: *activity.Context) error{OutOfMemory}!?trace.Span {
            const remote = if (ctx.propagated().get(tracing.span_key)) |parent|
                try temporal_opentelemetry.spanContext(ctx.allocator(), parent)
            else
                null;

            var start: trace.Tracer.StartOptions = .{};
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

            return a.tracer.startSpan(a.gpa, "activity-operation", start) catch |err| switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => {
                    ctx.logger().warn("Starting activity-operation failed; it is not traced.", .{ .err = err });
                    return null;
                },
            };
        }
    };

    pub const Activity = activity.Def("Activity", Activities.greet);
};

pub const client_update_propagation = struct {
    pub const task_queue = "client-update-propagation";

    /// Greets `name`; the validator rejects an empty one.
    pub const Greet = workflow.UpdateDef("greet", fn (name: []const u8) []const u8);

    /// The validator's error, the message of a rejected update's failure.
    pub const ValidationError = error{EmptyName};

    const State = struct {
        ctx: *workflow.Context,
        result: ?[]const u8 = null,

        fn greet(s: *State, name: []const u8) ![]const u8 {
            const span = tracing.startSpan(s.ctx, "handle-update", .{});
            defer span.end();

            s.result = try greeting(s.ctx.allocator(), name);
            return s.result.?;
        }

        fn validate(s: *State, name: []const u8) ValidationError!void {
            const span = tracing.startSpan(s.ctx, "validate-update", .{});
            defer span.end();

            if (name.len == 0) return error.EmptyName;
        }

        fn done(s: *State) bool {
            return s.result != null and s.ctx.allHandlersFinished();
        }
    };

    /// Waits for one accepted `greet` and for every handler to finish, then returns the
    /// update's greeting.
    fn run(ctx: *workflow.Context) ![]const u8 {
        const s = try ctx.allocator().create(State);
        s.* = .{ .ctx = ctx };
        try ctx.setUpdateHandlerWithValidator(Greet, s, State.greet, State.validate);
        try ctx.await(s, State.done);
        return s.result.?;
    }

    pub const Workflow = workflow.Def("Workflow", run);
};
