//! Context propagation: a `Values` struct set by the starter travels to the workflow and on to
//! its activity in the `custom-header` header field, carried by `propagator`. Both programs
//! also trace every request their client sends with `Tracer`, a client interceptor.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const propagation = temporal.propagation;
const interceptor = temporal.interceptor;

/// The value the sample propagates.
pub const Values = struct {
    key: []const u8,
    value: []const u8,
};

/// The header field that carries the value.
pub const header_key = "custom-header";

/// The key the value is stored under: in the starter's `propagation.Values` and in what
/// workflows and activities read with `ctx.propagated()`.
pub const propagate_key = propagation.key(Values, header_key);

/// Writes the value under `propagate_key` into the `custom-header` field of every outgoing
/// call, encoded with the default converter, and reads it back when a call is received.
/// Set it in `client.Options.propagators`; the client's workers use it too.
pub const propagator = propagation.HeaderPropagator(propagate_key).propagator;

/// A client interceptor that traces each request the client sends: it logs a span when the
/// request starts and when it finishes, with its method and whether its header carries the
/// propagated value. Stateless apart from `service_name`, so it is thread-safe.
pub const Tracer = struct {
    service_name: []const u8,

    const log = std.log.scoped(.tracer);

    pub fn call(t: *Tracer, input: *interceptor.ClientCall, next: interceptor.Client.Next) interceptor.ClientError!void {
        const propagated = if (input.header) |h| h.header().get(header_key) != null else false;
        log.info("{s}: span started method={s} propagated={}", .{ t.service_name, input.method, propagated });
        next.call(input) catch |err| {
            log.info("{s}: span finished method={s} error={t}", .{ t.service_name, input.method, err });
            return err;
        };
        log.info("{s}: span finished method={s}", .{ t.service_name, input.method });
    }

    pub fn client(t: *Tracer) interceptor.Client {
        return .from(Tracer, t);
    }
};

fn ctxPropWorkflow(ctx: *workflow.Context) !void {
    // Such a short timeout to make the sample fail over very fast.
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(2) };
    const logger = ctx.logger();

    if (ctx.propagated().get(propagate_key)) |vals| {
        logger.info("custom context propagated to workflow", .{ .key = vals.key, .value = vals.value });
    }

    // The workflow's propagated values are injected into the activity's header.
    const values = ctx.executeActivity(SampleActivity, options, .{}).get() catch |err| {
        logger.err("Workflow failed.", .{ .Error = err });
        return err;
    };
    if (values) |vals| {
        logger.info("context propagated to activity", .{ .key = vals.key, .value = vals.value });
    }
    logger.info("Workflow completed.", .{});
}

pub const CtxPropWorkflow = workflow.Def("CtxPropWorkflow", ctxPropWorkflow);

/// Returns the value propagated to the activity, or null if none was.
fn sampleActivity(ctx: *activity.Context) !?Values {
    return ctx.propagated().get(propagate_key);
}

pub const SampleActivity = activity.Def("SampleActivity", sampleActivity);
