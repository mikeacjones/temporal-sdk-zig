//! The caller side: a workflow in namespace `namespace` that starts the say-hello operation
//! of `service.HelloService` once per language through the Nexus endpoint `endpoint`, and
//! cancels the others as soon as one finishes.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const workflow = temporal.workflow;

pub const namespace = "my-cancelation-caller-namespace";
pub const task_queue = "nexus-cancelation-caller";
pub const endpoint = "nexus-cancelation-endpoint";

/// How long the handler workflows take, in milliseconds, for a run whose outcome doesn't
/// depend on chance. A null duration is drawn at random by the handler workflow.
pub const Timing = struct {
    /// How long each language's handler workflow works before it greets.
    work_ms: struct {
        en: ?u32 = null,
        fr: ?u32 = null,
        de: ?u32 = null,
        es: ?u32 = null,
        tr: ?u32 = null,
    } = .{},
    /// How long a canceled handler workflow cleans up before it ends.
    cleanup_ms: ?u32 = null,
};

const languages = std.enums.values(service.Language);

const Outcome = workflow.NexusOperationError!service.HelloOutput;

fn helloCallerWorkflow(ctx: *workflow.Context, name: []const u8, timing: Timing) ![]const u8 {
    const io = ctx.io();
    var outcomes: [languages.len]Outcome = undefined;
    var buffer: [languages.len]service.Language = undefined;
    var finished: std.Io.Queue(service.Language) = .init(&buffer);

    // Each operation runs on a coroutine of its own. Canceling the group cancels the
    // operations still running, and `cancel` returns once every operation has resolved: with
    // `wait_cancellation_requested`, once its handler has received the cancel request.
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (languages, &outcomes) |language, *outcome| group.async(io, sayHello, .{ ctx, name, language, timing, outcome, &finished });
    const first = try finished.getOne(io);
    ctx.logger().info("first operation finished, canceling the others", .{ .language = @tagName(first) });
    group.cancel(io);

    var greeting: ?[]const u8 = null;
    for (languages, outcomes) |language, outcome| {
        const output = outcome catch |err| switch (err) {
            error.Canceled => {
                ctx.logger().info("operation canceled", .{ .operation = service.SayHello.operation_name, .language = @tagName(language) });
                continue;
            },
            // Only a failure other than the cancellation fails the workflow.
            else => return err,
        };
        greeting = output.message;
    }
    // The first operation to finish wasn't canceled: it either completed or failed.
    return greeting orelse error.NoOperationCompleted;
}

pub const HelloCallerWorkflow = workflow.Def("HelloCallerWorkflow", helloCallerWorkflow);

/// Runs the say-hello operation for `language` to its end, writes how it ended to `outcome`,
/// and reports it on `finished`.
fn sayHello(ctx: *workflow.Context, name: []const u8, language: service.Language, timing: Timing, outcome: *Outcome, finished: *std.Io.Queue(service.Language)) void {
    const io = ctx.io();
    const operation = ctx.executeNexusOperation(service.HelloService, service.SayHello, .{
        .endpoint = endpoint,
        // Canceling resolves the operation once its handler has received the cancel request,
        // without waiting for the handler workflow to finish its cleanup.
        .cancellation_type = .wait_cancellation_requested,
    }, .{
        .name = name,
        .language = language,
        .work_ms = switch (language) {
            inline else => |l| @field(timing.work_ms, @tagName(l)),
        },
        .cleanup_ms = timing.cleanup_ms,
    });
    // The cancellation waits for the start, so every operation it cancels has a handler
    // workflow to receive it: it is blocked until `started` returns, and `get` delivers it.
    const protection = io.swapCancelProtection(.blocked);
    const started = operation.started();
    _ = io.swapCancelProtection(protection);
    outcome.* = if (started) |_| operation.get() else |err| err;
    // The queue is never closed, and holds one entry per language.
    finished.putOneUncancelable(io, language) catch {};
}

pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(HelloCallerWorkflow);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
