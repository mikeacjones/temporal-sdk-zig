//! The caller side of the on-demand pattern: a workflow in namespace `namespace` that starts
//! two greeting workflows in the handler's namespace, messages both, and waits for their
//! results, all through the operations of `service.NexusRemoteGreetingService` on the Nexus
//! endpoint `endpoint`.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const workflow = temporal.workflow;

pub const namespace = "my-messaging-caller-namespace";
pub const task_queue = "nexus-messaging-ondemandpattern-caller";
pub const endpoint = "nexus-messaging-ondemandpattern-endpoint";

pub const user_one = "nexus-messaging-greeting-one";
pub const user_two = "nexus-messaging-greeting-two";

/// Starts a greeting workflow for each of `user_one` and `user_two`, reads their languages,
/// sets one to French and the other to Spanish, checks both, approves both, and returns
/// their results. Returns one log entry per step.
fn callerRemoteWorkflow(ctx: *workflow.Context) ![]const []const u8 {
    const a = ctx.allocator();
    const Service = service.NexusRemoteGreetingService;
    const options: workflow.NexusOperationOptions = .{ .endpoint = endpoint };
    var entries: std.ArrayList([]const u8) = .empty;

    // Start both workflows; each operation completes with its workflow's result.
    const one = ctx.executeNexusOperation(Service, service.RunFromRemote, options, .{ .user_id = user_one });
    const two = ctx.executeNexusOperation(Service, service.RunFromRemote, options, .{ .user_id = user_two });

    // Wait for both to start before messaging them.
    _ = try one.started();
    try entries.append(a, try std.fmt.allocPrint(a, "started remote workflow one: {s}", .{user_one}));
    _ = try two.started();
    try entries.append(a, try std.fmt.allocPrint(a, "started remote workflow two: {s}", .{user_two}));

    const languages_one = try ctx.executeNexusOperation(Service, service.GetLanguages, options, .{
        .include_unsupported = false,
        .user_id = user_one,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguages (one) returned {d} languages", .{languages_one.languages.len}));

    const languages_two = try ctx.executeNexusOperation(Service, service.GetLanguages, options, .{
        .include_unsupported = true,
        .user_id = user_two,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguages (two) with unsupported returned {d} languages", .{languages_two.languages.len}));

    const previous_one = try ctx.executeNexusOperation(Service, service.SetLanguage, options, .{
        .language = .french,
        .user_id = user_one,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "setLanguage(French) on one returned previous: {s}", .{previous_one.name()}));

    const previous_two = try ctx.executeNexusOperation(Service, service.SetLanguage, options, .{
        .language = .spanish,
        .user_id = user_two,
    }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "setLanguage(Spanish) on two returned previous: {s}", .{previous_two.name()}));

    const current_one = try ctx.executeNexusOperation(Service, service.GetLanguage, options, .{ .user_id = user_one }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguage (one) = {s}", .{current_one.name()}));

    const current_two = try ctx.executeNexusOperation(Service, service.GetLanguage, options, .{ .user_id = user_two }).get();
    try entries.append(a, try std.fmt.allocPrint(a, "getLanguage (two) = {s}", .{current_two.name()}));

    _ = try ctx.executeNexusOperation(Service, service.Approve, options, .{
        .name = "CallerRemoteWorkflow",
        .user_id = user_one,
    }).get();
    try entries.append(a, "approved workflow one");

    _ = try ctx.executeNexusOperation(Service, service.Approve, options, .{
        .name = "CallerRemoteWorkflow",
        .user_id = user_two,
    }).get();
    try entries.append(a, "approved workflow two");

    // Approved, both workflows complete, and so do their operations.
    try entries.append(a, try std.fmt.allocPrint(a, "remote workflow one result: {s}", .{try one.get()}));
    try entries.append(a, try std.fmt.allocPrint(a, "remote workflow two result: {s}", .{try two.get()}));

    return entries.items;
}

pub const CallerRemoteWorkflow = workflow.Def("CallerRemoteWorkflow", callerRemoteWorkflow);

pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(CallerRemoteWorkflow);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
