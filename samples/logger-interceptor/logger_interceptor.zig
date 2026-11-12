//! Logger interceptor: a worker interceptor that adds fields of its own to the loggers that
//! workflow and activity code get from `ctx.logger()`.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const interceptor = temporal.interceptor;
const log = temporal.log;
const Allocator = std.mem.Allocator;

fn helloWorld(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("HelloWorld workflow started", .{ .name = name });

    const greeting = ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .Error = err });
        return err;
    };

    logger.info("HelloWorld workflow completed", .{ .result = result });
    return result;
}

pub const Workflow = workflow.Def("Workflow", helloWorld);

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    ctx.logger().info("Activity", .{ .name = name });
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);

/// Which fields the interceptor adds. Each function returns `logger` with its fields added
/// (`log.Logger.with`), allocating from `arena`, which lives as long as the run or attempt.
/// A null function adds nothing.
pub const InterceptorOptions = struct {
    /// Runs inside the workflow, once per run (and again on replay): it must follow the
    /// workflow rules.
    workflow_fields: ?*const fn (ctx: *workflow.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger = null,
    /// Runs on the activity's thread, once per attempt.
    activity_fields: ?*const fn (ctx: *activity.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger = null,
};

/// The worker interceptor: install it with `worker.Options.interceptors`. It only reads its
/// options, so one value serves every worker and run; it must outlive the workers.
pub const WorkerInterceptor = struct {
    options: InterceptorOptions,

    pub fn interceptWorkflow(w: *WorkerInterceptor, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const fields = w.options.workflow_fields orelse return .{};
        const run = try ctx.allocator().create(WorkflowOutbound);
        run.* = .{ .ctx = ctx, .fields = fields };
        return .{ .outbound = interceptor.WorkflowOutbound.from(WorkflowOutbound, run) };
    }

    pub fn interceptActivity(w: *WorkerInterceptor, ctx: *activity.Context) error{OutOfMemory}!interceptor.Activity {
        const fields = w.options.activity_fields orelse return .{};
        const attempt = try ctx.allocator().create(ActivityOutbound);
        attempt.* = .{ .ctx = ctx, .fields = fields };
        return .{ .outbound = interceptor.ActivityOutbound.from(ActivityOutbound, attempt) };
    }
};

/// One run's outbound interceptor, in the run's memory. The logger is built on the first
/// `ctx.logger()` call and reused, so the run's memory doesn't grow with each call.
const WorkflowOutbound = struct {
    ctx: *workflow.Context,
    fields: *const fn (ctx: *workflow.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger,
    cached: ?log.Logger = null,

    pub fn logger(o: *WorkflowOutbound, input: interceptor.LoggerInput, next: interceptor.WorkflowOutbound.Next) log.Logger {
        if (o.cached) |l| return l;
        const base = next.logger(input);
        // Without memory for the fields, log without them rather than lose the message.
        const l = o.fields(o.ctx, base, o.ctx.allocator()) catch return base;
        o.cached = l;
        return l;
    }
};

/// One attempt's outbound interceptor, in the attempt's memory; like `WorkflowOutbound`, it
/// builds the logger once.
const ActivityOutbound = struct {
    ctx: *activity.Context,
    fields: *const fn (ctx: *activity.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger,
    cached: ?log.Logger = null,

    pub fn logger(o: *ActivityOutbound, input: interceptor.LoggerInput, next: interceptor.ActivityOutbound.Next) log.Logger {
        if (o.cached) |l| return l;
        const base = next.logger(input);
        const l = o.fields(o.ctx, base, o.ctx.allocator()) catch return base;
        o.cached = l;
        return l;
    }
};

/// `t` as an RFC 3339 UTC time to the second, such as `2026-11-12T09:30:00Z`, in `arena`.
pub fn rfc3339(arena: Allocator, t: std.Io.Timestamp) error{OutOfMemory}![]const u8 {
    const epoch = std.time.epoch;
    const seconds: epoch.EpochSeconds = .{ .secs = @intCast(@max(0, @divFloor(t.nanoseconds, std.time.ns_per_s))) };
    const year_day = seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = seconds.getDaySeconds();
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year_day.year,
        month_day.month.numeric(),
        @as(u6, month_day.day_index) + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

/// The worker's workflow fields: `WorkflowStartTime`, when the run started.
pub fn workflowStartTime(ctx: *workflow.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger {
    const start = ctx.info().start_time orelse return logger;
    return logger.with(arena, .{ .WorkflowStartTime = try rfc3339(arena, start) });
}

/// The worker's activity fields: `ActivityStartTime`, when the attempt started.
pub fn activityStartTime(ctx: *activity.Context, logger: log.Logger, arena: Allocator) error{OutOfMemory}!log.Logger {
    const start = ctx.info().started_time orelse return logger;
    return logger.with(arena, .{ .ActivityStartTime = try rfc3339(arena, start) });
}
