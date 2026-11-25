//! The caller side of the caller pattern: a workflow in namespace `namespace` that messages a
//! user's greeting workflow, which already runs in the handler's namespace, through the
//! operations of `service.NexusGreetingService` on the Nexus endpoint `endpoint`.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const workflow = temporal.workflow;

pub const namespace = "my-messaging-caller-namespace";
pub const task_queue = "nexus-messaging-callerpattern-caller";
pub const endpoint = "nexus-messaging-callerpattern-endpoint";

/// Reads the languages of `user_id`'s greeting workflow, sets its language to Arabic, checks
/// that it took, and approves it. Returns one log entry per step.
fn callerWorkflow(ctx: *workflow.Context, user_id: []const u8) ![]const []const u8 {
    const a = ctx.allocator();
    const Service = service.NexusGreetingService;
    const options: workflow.NexusOperationOptions = .{ .endpoint = endpoint };
    var entries: std.ArrayList([]const u8) = .empty;

    // 1. The languages the workflow has a greeting for.
    const languages = try ctx.executeNexusOperation(Service, service.GetLanguages, options, .{
        .include_unsupported = false,
        .user_id = user_id,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguages returned {d} languages", .{languages.languages.len}));

    // 2. Set the language to Arabic, which the workflow fetches a greeting for.
    const previous = try ctx.executeNexusOperation(Service, service.SetLanguage, options, .{
        .language = .arabic,
        .user_id = user_id,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "setLanguage(Arabic) returned previous language: {s}", .{previous.name()}));

    // 3. The current language, which must now be Arabic.
    const current = try ctx.executeNexusOperation(Service, service.GetLanguage, options, .{ .user_id = user_id }).get();
    if (current != .arabic) {
        ctx.logger().err("Expected Arabic", .{ .got = current.name() });
        return error.UnexpectedLanguage;
    }
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguage returned: {s} (confirmed Arabic)", .{current.name()}));

    // 4. Approve the workflow, so it completes.
    _ = try ctx.executeNexusOperation(Service, service.Approve, options, .{
        .name = "CallerWorkflow",
        .user_id = user_id,
    }).get();
    try entries.append(a, "approve sent successfully");

    return entries.items;
}

pub const CallerWorkflow = workflow.Def("CallerWorkflow", callerWorkflow);

pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(CallerWorkflow);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
