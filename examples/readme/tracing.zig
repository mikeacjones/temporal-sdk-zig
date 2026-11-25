//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const workflow = temporal.workflow;
const activity = temporal.activity;
const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
const Greet = activity.Def("traced_greet", greet);

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}
const Greeting = workflow.Def("traced_greeting", greeting);

// example: observability-tracing
const tracing = temporal.tracing;

/// A tracer that numbers its spans and remembers each one's name and parent. Called from
/// many threads at once.
const CountingTracer = struct {
    io: std.Io,
    arena: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    spans: std.ArrayList(Started) = .empty,

    const Started = struct { name: []const u8, parent: ?usize };

    pub fn startSpan(t: *CountingTracer, arena: std.mem.Allocator, options: tracing.StartSpanOptions) error{OutOfMemory}!tracing.Span {
        t.mutex.lockUncancelable(t.io);
        defer t.mutex.unlock(t.io);
        // The parent is a span context this tracer wrote, here or in another process.
        const parent_id = if (options.parent) |p| p.get("span-id") else null;
        const parent = if (parent_id) |id| std.fmt.parseInt(usize, id, 10) catch null else null;
        try t.spans.append(t.arena, .{ .name = try options.spanName(t.arena), .parent = parent });
        // What travels to the span's children: allocated from the arena the call gives.
        const fields = try arena.alloc(tracing.Field, 1);
        fields[0] = .{ .name = "span-id", .value = try std.fmt.allocPrint(arena, "{d}", .{t.spans.items.len - 1}) };
        return .{ .context = .{ .fields = fields } };
    }

    pub fn finishSpan(_: *CountingTracer, _: tracing.Span, _: tracing.FinishSpanOptions) void {}
};
// end-example

/// The names of `span` and its ancestors, nearest first.
fn ancestry(t: *const CountingTracer, a: std.mem.Allocator, name: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var at: ?usize = for (t.spans.items, 0..) |s, i| {
        if (std.mem.eql(u8, s.name, name)) break i;
    } else null;
    while (at) |i| : (at = t.spans.items[i].parent) try out.append(a, t.spans.items[i].name);
    return out.items;
}

// example: observability-tracing-setup
test "observability: the activity's span descends from the client's" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var counting: CountingTracer = .{ .io = testing.io, .arena = arena.allocator() };
    // The interceptor and the slices must stay where they are while the client runs.
    var traced: tracing.Interceptor = .init(tracing.Tracer.from(CountingTracer, &counting));
    const client_interceptors = [_]temporal.interceptor.Client{traced.client()};
    const worker_interceptors = [_]temporal.interceptor.Worker{traced.worker()};
    const propagators = [_]temporal.propagation.ContextPropagator{traced.propagator()};

    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{
        .interceptors = &client_interceptors,
        .worker_interceptors = &worker_interceptors,
        .propagators = &propagators,
    });
    defer env.deinit();
    try env.registerWorkflow(Greeting);
    try env.registerActivity(Greet);
    _ = try env.executeWorkflow(Greeting, .{"Ziggy"}, arena.allocator(), null);

    const chain = try ancestry(&counting, arena.allocator(), "RunActivity:traced_greet");
    try testing.expectEqual(4, chain.len);
    try testing.expectEqualStrings("StartActivity:traced_greet", chain[1]);
    try testing.expectEqualStrings("RunWorkflow:traced_greeting", chain[2]);
    try testing.expectEqualStrings("StartWorkflow:traced_greeting", chain[3]);
}
// end-example
