//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const workflow = temporal.workflow;
const activity = temporal.activity;
const interceptor = temporal.interceptor;
const log = temporal.log;
const testing = std.testing;
const Io = std.Io;
const Payload = temporal.converter.Payload;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}

const Greet = activity.Def("greet", greet);

/// Keeps each record as a line of text.
const LineSink = struct {
    io: std.Io,
    arena: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    lines: std.ArrayList([]const u8) = .empty,

    fn sink(s: *LineSink) log.Sink {
        return .{ .ptr = s, .vtable = &.{ .log = recordLine } };
    }
};

// example: observability-logging
/// A `LineSink`'s log function: records arrive from many threads at once.
fn recordLine(ptr: ?*anyopaque, level: log.Level, message: []const u8, fields: []const log.Field) void {
    const s: *LineSink = @ptrCast(@alignCast(ptr.?));
    s.mutex.lockUncancelable(s.io);
    defer s.mutex.unlock(s.io);
    var line: std.Io.Writer.Allocating = .init(s.arena);
    line.writer.print("{t} {s}", .{ level, message }) catch return;
    for (fields) |f| line.writer.print(" {s}={s}", .{ f.key, f.value }) catch return;
    s.lines.append(s.arena, line.written()) catch return;
}

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = try ctx.logger().with(ctx.allocator(), .{ .customer = name });
    logger.info("greeting started", .{});
    const result = try ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
    logger.info("greeting done", .{ .result = result });
    return result;
}
// end-example

const Greeting = workflow.Def("greeting", greeting);

/// Whether a line of `s` starts with `prefix` and contains `part`.
fn hasLine(s: *const LineSink, prefix: []const u8, part: []const u8) bool {
    for (s.lines.items) |line| {
        if (std.mem.startsWith(u8, line, prefix) and std.mem.indexOf(u8, line, part) != null) return true;
    }
    return false;
}

test "observability: workflow logs go to the client's sink" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var lines: LineSink = .{ .io = testing.io, .arena = arena.allocator() };
    {
        const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .logger = lines.sink() });
        defer env.deinit();
        try env.registerWorkflow(Greeting);
        try env.registerActivity(Greet);
        _ = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);
    }
    try testing.expect(hasLine(&lines, "info greeting started", "customer=Ziggy"));
    try testing.expect(hasLine(&lines, "info greeting done result=Hello, Ziggy!", "WorkflowType=greeting"));
}

// example: observability-metrics
fn countedGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const greetings = try ctx.metrics().counter("greetings", .{ .description = "Greetings requested" });
    defer greetings.deinit();
    greetings.add(1, .{ .language = "en" });
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

// Serve metrics at http://<address>/metrics for Prometheus, or push them to a collector.
fn prometheusMetrics(address: []const u8) temporal.metrics.Options {
    return .{ .exporter = .{ .prometheus = .{ .bind_address = address } }, .global_tags = &.{.{ .key = "app", .value = "greetings" }} };
}
const otel_metrics: temporal.metrics.Options = .{ .exporter = .{ .opentelemetry = .{ .url = "http://localhost:4317" } } };
// end-example

const CountedGreeting = workflow.Def("counted_greeting", countedGreeting);

/// A localhost port that was free a moment ago: the Prometheus exporter doesn't report the
/// port it bound.
fn freePort(io: Io) !u16 {
    const any: Io.net.IpAddress = try .parseIp4("127.0.0.1", 0);
    var server = try any.listen(io, .{});
    defer server.deinit(io);
    return server.socket.address.getPort();
}

test "observability: a workflow's counter is scraped from the Prometheus endpoint" {
    _ = otel_metrics;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const address = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{try freePort(testing.io)});

    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .metrics = prometheusMetrics(address) });
    defer env.deinit();
    try env.registerWorkflow(CountedGreeting);
    try env.registerActivity(Greet);
    _ = try env.executeWorkflow(CountedGreeting, .{"Ziggy"}, a, null);

    var http: std.http.Client = .{ .allocator = testing.allocator, .io = testing.io };
    defer http.deinit();
    var body: Io.Writer.Allocating = .init(a);
    const url = try std.fmt.allocPrint(a, "http://{s}/metrics", .{address});
    const response = try http.fetch(.{ .location = .{ .url = url }, .response_writer = &body.writer });
    try testing.expectEqual(std.http.Status.ok, response.status);
    var lines = std.mem.splitScalar(u8, body.written(), '\n');
    const sample = while (lines.next()) |line| {
        if (std.mem.startsWith(u8, line, "greetings{")) break line;
    } else return error.TestMetricNotScraped;
    try testing.expect(std.mem.indexOf(u8, sample, "language=\"en\"") != null);
    try testing.expect(std.mem.indexOf(u8, sample, "app=\"greetings\"") != null);
    try testing.expect(std.mem.endsWith(u8, sample, " 1"));
}

// example: observability-interceptors
/// Logs each activity a workflow schedules.
const ActivityAudit = struct {
    pub fn interceptWorkflow(_: *ActivityAudit, ctx: *workflow.Context) error{OutOfMemory}!interceptor.Workflow {
        const run = try ctx.allocator().create(Scheduling);
        run.* = .{ .ctx = ctx };
        return .{ .outbound = interceptor.WorkflowOutbound.from(Scheduling, run) };
    }
};
const Scheduling = struct {
    ctx: *workflow.Context,
    pub fn executeActivity(s: *Scheduling, input: interceptor.ActivityInput, next: interceptor.WorkflowOutbound.Next) workflow.ActivityFuture(Payload) {
        s.ctx.logger().info("scheduling activity", .{ .activity = input.activity_type });
        return next.executeActivity(input);
    }
};
// end-example

test "observability: a worker interceptor sees the activities a workflow schedules" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var lines: LineSink = .{ .io = testing.io, .arena = arena.allocator() };
    var audit: ActivityAudit = .{};
    const interceptors = [_]interceptor.Worker{.from(ActivityAudit, &audit)};
    {
        const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
            .logger = lines.sink(),
            .worker_interceptors = &interceptors,
        });
        defer env.deinit();
        try env.registerWorkflow(Greeting);
        try env.registerActivity(Greet);
        _ = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);
    }
    try testing.expect(hasLine(&lines, "info scheduling activity activity=greet", "WorkflowType=greeting"));
}

fn tenantGreeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(TenantGreet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}

const TenantGreet = activity.Def("tenant_greet", tenantGreet);
const TenantGreeting = workflow.Def("tenant_greeting", tenantGreeting);

// example: observability-propagation
const tenant = temporal.propagation.key([]const u8, "tenant");
const propagators = [_]temporal.propagation.ContextPropagator{temporal.propagation.HeaderPropagator(tenant).propagator};
// TenantGreeting calls this activity, and the tenant goes along in its header.
fn tenantGreet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    const who = ctx.propagated().get(tenant) orelse "nobody";
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s} of {s}!", .{ name, who });
}
test "observability: a tenant travels from the client to the activity" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(TenantGreeting);
    try env.registerActivity(TenantGreet);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var values: temporal.propagation.Values = .init(arena.allocator());
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(TenantGreeting, .{ .context = &values }, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ziggy of acme!", result);
}
// end-example
