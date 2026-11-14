//! README.md examples. The code between `// example: <id>` and `// end-example` is copied into
//! README.md by `zig build readme`; edit it here, never in README.md.

const std = @import("std");
const temporal = @import("temporal");

const testing = std.testing;
const workflow = temporal.workflow;

fn greeting(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello, {s}!", .{name});
}
const Greeting = workflow.Def("greeting", greeting);

// example: nexus-service
const nexus = temporal.nexus;

// The contract: names and types only, shared by the handling worker and its callers.
pub const Echo = nexus.OperationDef("echo", []const u8, []const u8);
pub const Greet = nexus.OperationDef("greet", []const u8, []const u8);
pub const GreetingService = nexus.ServiceDef("greeting-service", .{ Echo, Greet });

// A synchronous operation returns its output.
fn echo(ctx: *nexus.Context, message: []const u8) ![]const u8 {
    if (message.len == 0) return ctx.handlerError(.{ .type = .bad_request, .message = "the message is empty" });
    return message;
}

// A workflow-run operation returns the workflow to start, and completes with its result.
fn greet(ctx: *nexus.Context, name: []const u8) !nexus.WorkflowStart(Greeting) {
    const id = try std.fmt.allocPrint(ctx.allocator(), "greet-{s}", .{ctx.info().request_id});
    return .{ .options = .{ .id = id }, .args = .{name} };
}

pub const GreetingHandler = nexus.ServiceHandler(GreetingService, .{
    nexus.OperationHandler(Echo, echo),
    nexus.OperationHandler(Greet, greet),
});

fn registerGreetingService(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(Greeting);
    try w.registerNexusService(GreetingHandler);
}
// end-example

// example: nexus-caller
fn greetThroughNexus(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const future = ctx.executeNexusOperation(GreetingService, Greet, .{
        .endpoint = "greeting-endpoint",
        .schedule_to_close_timeout = .fromSeconds(60),
    }, name);
    // The token of the started workflow-run operation; null if the result came with the start.
    if (try future.started()) |token| ctx.logger().info("greeting started", .{ .token = token });
    return future.get();
}
pub const GreetThroughNexus = workflow.Def("greet_through_nexus", greetThroughNexus);
// end-example

// example: nexus-serve
/// A worker that serves `GreetingService` on task queue "greetings" until SIGINT or SIGTERM.
fn serveGreetings(gpa: std.mem.Allocator, io: std.Io, client: *temporal.client.Client) !void {
    var w = try temporal.worker.Worker.init(gpa, io, client, "greetings", .{
        .max_concurrent_nexus_task_executions = 20,
    });
    defer w.deinit();
    try registerGreetingService(&w);
    try w.runUntilSignal();
}
// end-example

// example: nexus-test
test "a workflow greets through the Nexus endpoint" {
    const env = try temporal.testing.WorkflowEnvironment.start(testing.allocator, testing.io);
    defer env.deinit();
    try env.registerWorkflow(GreetThroughNexus);
    try env.registerWorkflow(Greeting);
    try env.registerNexusService(GreetingHandler);
    // Routes to the environment's namespace and task queue, where its worker serves the service.
    try env.createNexusEndpoint("greeting-endpoint", .{});

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const result = try env.executeWorkflow(GreetThroughNexus, .{"Ziggy"}, arena.allocator(), null);
    try testing.expectEqualStrings("Hello, Ziggy!", result);
}
// end-example

test "nexus: a caller workflow is checked against the service contract" {
    try testing.expectEqualStrings("greet_through_nexus", GreetThroughNexus.workflow_name);
    try testing.expect(GreetThroughNexus.Result == Greet.Output);
}

test "nexus: a synchronous operation runs from the handler's dispatch table, and fails as the handler says" {
    _ = &serveGreetings;
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectEqualStrings("greeting-service", GreetingHandler.service_name);
    try testing.expect(GreetingService.hasOperation(Greet));
    const op = GreetingHandler.registered.find("echo").?;
    try testing.expectEqual(nexus.Operation.Kind.workflow_run, GreetingHandler.registered.find("greet").?.operation.kind);

    var ctx: nexus.Context = .{ .arena = a, .io_ = testing.io, .info_ = .{ .service = "greeting-service", .operation = "echo" } };
    const echoed = op.start(&ctx, try temporal.converter.toPayload(a, @as([]const u8, "hi")));
    try testing.expectEqualStrings("hi", try temporal.converter.fromPayload([]const u8, a, echoed.succeeded.result));

    var failing: nexus.Context = .{ .arena = a, .io_ = testing.io, .info_ = .{ .service = "greeting-service", .operation = "echo" } };
    const failed = op.start(&failing, try temporal.converter.toPayload(a, @as([]const u8, "")));
    try testing.expectEqual(nexus.HandlerErrorType.bad_request, failed.handler_error.type);
    try testing.expect(!failed.handler_error.retryable());
}
