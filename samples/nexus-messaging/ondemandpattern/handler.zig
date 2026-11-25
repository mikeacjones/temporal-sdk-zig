//! The handler side of the on-demand pattern: a long-running greeting workflow, one per user,
//! with a query, an update and a signal, and the operations of
//! `service.NexusRemoteGreetingService`, which start it and reach it from another namespace.
//! The worker serves them in namespace `namespace` on task queue `task_queue`, where the
//! Nexus endpoint routes.
//!
//! The caller starts the workflow itself, with the runFromRemote operation, and that
//! operation completes with the workflow's result. The other operations are synchronous;
//! each finds the workflow by the user ID in its input and sends the query, update or signal
//! through the worker's client.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");
const activity = temporal.activity;
const nexus = temporal.nexus;
const workflow = temporal.workflow;

pub const namespace = "my-messaging-target-namespace";
pub const task_queue = "nexus-messaging-ondemandpattern-handler";
pub const workflow_id_prefix = "GreetingWorkflow_for_";

/// The ID of the greeting workflow of `user_id`: `workflow_id_prefix` followed by the user
/// ID. Allocated from `arena`.
pub fn workflowId(arena: std.mem.Allocator, user_id: []const u8) error{OutOfMemory}![]const u8 {
    return std.mem.concat(arena, u8, &.{ workflow_id_prefix, user_id });
}

/// The languages the workflow can greet in, in enum order: all of them with
/// `include_unsupported`, otherwise those it has a greeting for.
pub const GetLanguagesQuery = workflow.QueryDef("getLanguages", fn (include_unsupported: bool) service.GetLanguagesOutput);
/// The workflow's current language.
pub const GetLanguageQuery = workflow.QueryDef("getLanguage", fn () service.Language);
/// Sets the workflow's language and returns the previous one, fetching the greetings first
/// when the workflow has none in the new language.
pub const SetLanguageUpdate = workflow.UpdateDef("setLanguage", fn (service.Language) service.Language);
/// Approves the workflow, so it completes; the argument is who approves.
pub const ApproveSignal = workflow.SignalDef("approve", fn (name: []const u8) void);

/// A greeting for each language.
pub const Greetings = std.enums.EnumFieldStruct(service.Language, []const u8, null);

/// Returns the greeting in each language, as a call to a translation service would.
fn greetingActivity(_: *activity.Context) !Greetings {
    return .{
        .arabic = "مرحبا بالعالم",
        .chinese = "你好，世界",
        .english = "Hello, world",
        .french = "Bonjour, monde",
        .hindi = "नमस्ते दुनिया",
        .portuguese = "Olá, mundo",
        .spanish = "Hola, mundo",
    };
}

pub const GreetingActivity = activity.Def("GreetingActivity", greetingActivity);

/// The workflow's state and its message handlers. Allocated with `ctx.allocator()`, so the
/// handlers can use it for the whole run.
const Greeting = struct {
    ctx: *workflow.Context,
    language: service.Language = .english,
    /// The greetings the run knows. An enum map, so walks over it go in enum order, the same
    /// on every replay.
    greetings: std.EnumMap(service.Language, []const u8) = .init(.{
        .chinese = "你好，世界",
        .english = "Hello, world",
    }),
    /// Who approved the workflow; null until it is approved.
    approved_by: ?[]const u8 = null,
    /// Held by `setLanguage` across its activity, so two updates don't interleave.
    lock: std.Io.Mutex = .init,

    fn getLanguages(g: *Greeting, include_unsupported: bool) !service.GetLanguagesOutput {
        const all = std.enums.values(service.Language);
        if (include_unsupported) return .{ .languages = all };
        var languages: std.ArrayList(service.Language) = .empty;
        for (all) |language| {
            if (g.greetings.contains(language)) try languages.append(g.ctx.allocator(), language);
        }
        return .{ .languages = languages.items };
    }

    fn getLanguage(g: *Greeting) service.Language {
        return g.language;
    }

    // There is no validator: a language outside the enum can't be decoded as a
    // `service.Language`, so such an update is rejected before this handler runs.
    fn setLanguage(g: *Greeting, language: service.Language) !service.Language {
        const io = g.ctx.io();
        try g.lock.lock(io);
        defer g.lock.unlock(io);

        const previous = g.language;
        if (!g.greetings.contains(language)) {
            const fetched = try g.ctx.executeActivity(GreetingActivity, .{
                .start_to_close_timeout = .fromSeconds(10),
                .retry_policy = .{ .maximum_attempts = 3 },
            }, .{}).get();
            inline for (comptime std.enums.values(service.Language)) |l| {
                g.greetings.put(l, @field(fetched, @tagName(l)));
            }
        }
        g.language = language;
        g.ctx.logger().info("Language updated", .{ .from = previous.name(), .to = language.name() });
        return previous;
    }

    /// The first approval counts; later ones are ignored.
    fn approve(g: *Greeting, name: []const u8) void {
        if (g.approved_by != null) return;
        g.approved_by = name;
        g.ctx.logger().info("Workflow approved", .{ .by = name });
    }

    fn approvedAndIdle(g: *Greeting) bool {
        return g.approved_by != null and g.ctx.allHandlersFinished();
    }
};

/// Greets in the current language once approved: "<greeting> (approved by <name>)". Runs
/// until the approve signal arrives and every update has finished. It takes no argument: its
/// workflow ID names the user.
fn greetingWorkflow(ctx: *workflow.Context) ![]const u8 {
    const g = try ctx.allocator().create(Greeting);
    g.* = .{ .ctx = ctx };
    try ctx.setQueryHandler(GetLanguagesQuery, g, Greeting.getLanguages);
    try ctx.setQueryHandler(GetLanguageQuery, g, Greeting.getLanguage);
    try ctx.setUpdateHandler(SetLanguageUpdate, g, Greeting.setLanguage);
    try ctx.setSignalHandler(ApproveSignal, g, Greeting.approve);
    ctx.logger().info("GreetingWorkflow started", .{ .workflow_id = ctx.info().workflow_id });

    try ctx.await(g, Greeting.approvedAndIdle);

    const greeting = g.greetings.get(g.language) orelse {
        ctx.logger().err("No greeting for the language", .{ .language = g.language.name() });
        return error.NoGreetingForLanguage;
    };
    return std.fmt.allocPrint(ctx.allocator(), "{s} (approved by {s})", .{ greeting, g.approved_by.? });
}

pub const GreetingWorkflow = workflow.Def("GreetingWorkflow", greetingWorkflow);

/// Starts `GreetingWorkflow` under the user's workflow ID. The operation's input names the
/// user, but the workflow takes no argument, so `.args` is empty. The task queue defaults to
/// the one this operation is handled on.
fn runFromRemote(ctx: *nexus.Context, input: service.RunFromRemoteInput) !nexus.WorkflowStart(GreetingWorkflow) {
    return .{ .options = .{ .id = try workflowId(ctx.allocator(), input.user_id) }, .args = .{} };
}

fn getLanguages(ctx: *nexus.Context, input: service.GetLanguagesInput) !service.GetLanguagesOutput {
    const client = try ctx.client();
    const a = ctx.allocator();
    return client.queryWorkflow(GetLanguagesQuery, a, try workflowId(a, input.user_id), "", .{input.include_unsupported});
}

fn getLanguage(ctx: *nexus.Context, input: service.GetLanguageInput) !service.Language {
    const client = try ctx.client();
    const a = ctx.allocator();
    return client.queryWorkflow(GetLanguageQuery, a, try workflowId(a, input.user_id), "", .{});
}

/// Sends the update and waits for it to complete, so the operation returns the previous
/// language.
fn setLanguage(ctx: *nexus.Context, input: service.SetLanguageInput) !service.Language {
    const client = try ctx.client();
    const a = ctx.allocator();
    var handle = try client.updateWorkflow(SetLanguageUpdate, try workflowId(a, input.user_id), "", .{input.language}, .{ .wait_for_stage = .completed });
    defer handle.deinit();
    return handle.get(a);
}

fn approve(ctx: *nexus.Context, input: service.ApproveInput) !service.ApproveOutput {
    const client = try ctx.client();
    try client.signalWorkflow(ApproveSignal, try workflowId(ctx.allocator(), input.user_id), "", .{input.name});
    return .{};
}

/// The synchronous operations return the client's errors as they are; `error.NotFound`, when the user
/// has no greeting workflow, fails the request as `not_found`.
pub const GreetingServiceHandler = nexus.ServiceHandler(service.NexusRemoteGreetingService, .{
    nexus.OperationHandler(service.RunFromRemote, runFromRemote),
    nexus.OperationHandler(service.GetLanguages, getLanguages),
    nexus.OperationHandler(service.GetLanguage, getLanguage),
    nexus.OperationHandler(service.SetLanguage, setLanguage),
    nexus.OperationHandler(service.Approve, approve),
});

/// Registers the greeting workflow, its activity, and the service.
pub fn register(w: *temporal.worker.Worker) !void {
    try w.registerWorkflow(GreetingWorkflow);
    try w.registerActivity(GreetingActivity);
    try w.registerNexusService(GreetingServiceHandler);
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !temporal.client.Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return temporal.client.Client.connect(gpa, io, options);
}
