//! Context propagation against a dev server: values set on a client call reach the workflow,
//! its activities, local activities, children, the run it continues as, and the Nexus
//! operations it calls.

const std = @import("std");
const activity = @import("activity.zig");
const nexus = @import("nexus.zig");
const workflow = @import("workflow.zig");
const propagation = @import("propagation.zig");
const WorkflowEnvironment = @import("testing.zig").WorkflowEnvironment;

const testing = std.testing;

const tenant = propagation.key([]const u8, "tenant");
const propagators = [_]propagation.ContextPropagator{propagation.HeaderPropagator(tenant).propagator};

const Fixtures = struct {
    const opts: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };

    fn whoAmI(ctx: *activity.Context) ![]const u8 {
        const header = if (ctx.headers().get("tenant") != null) "header" else "no header";
        return std.fmt.allocPrint(ctx.allocator(), "{s} ({s})", .{ ctx.propagated().get(tenant) orelse "none", header });
    }
    const WhoAmI = activity.Def("WhoAmI", whoAmI);

    fn child(ctx: *workflow.Context) ![]const u8 {
        return ctx.propagated().get(tenant) orelse "none";
    }
    const Child = workflow.Def("PropagationChild", child);

    /// Continues as new `hops` times, then reports the tenant each callee saw.
    fn relay(ctx: *workflow.Context, hops: u32) ![]const u8 {
        if (hops > 0) return ctx.continueAsNew(Relay, .{}, .{hops - 1});
        const own = ctx.propagated().get(tenant) orelse "none";
        const remote = try ctx.executeActivity(WhoAmI, opts, .{}).get();
        const local = try ctx.executeLocalActivity(WhoAmI, .{ .start_to_close_timeout = .fromSeconds(10) }, .{}).get();
        const from_child = try ctx.executeChildWorkflow(Child, .{}, .{}).get();
        return std.fmt.allocPrint(ctx.allocator(), "workflow={s} header={s} activity={s} local={s} child={s}", .{
            own, if (ctx.headers().get("tenant") != null) "yes" else "no", remote, local, from_child,
        });
    }
    const Relay = workflow.Def("PropagationRelay", relay);
};

test "a propagated value travels from a client start through the workflow, its activities, child and continue-as-new" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(Fixtures.Relay);
    try env.registerWorkflow(Fixtures.Child);
    try env.registerActivity(Fixtures.WhoAmI);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();

    var values: propagation.Values = .init(arena.allocator());
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(Fixtures.Relay, .{ .task_queue = "", .context = &values }, .{1}, arena.allocator(), null);
    try testing.expectEqualStrings("workflow=acme header=yes activity=acme (header) local=acme (header) child=acme", result);

    // Without values, nothing is propagated.
    const bare = try env.executeWorkflow(Fixtures.Relay, .{0}, arena.allocator(), null);
    try testing.expectEqualStrings("workflow=none header=no activity=none (no header) local=none (no header) child=none", bare);
}

const NexusFixtures = struct {
    const Who = nexus.OperationDef("who", void, []const u8);
    const WhoRun = nexus.OperationDef("who-run", void, []const u8);
    const Service = nexus.ServiceDef("zig-propagation-service", .{ Who, WhoRun });

    /// The tenant the handler's context reports, and the raw propagation header it received.
    fn who(ctx: *nexus.Context) ![]const u8 {
        const raw = ctx.info().header(propagation.nexus_header_name) orelse "none";
        return std.fmt.allocPrint(ctx.allocator(), "{s} (header {s})", .{
            ctx.propagated().get(tenant) orelse "none",
            if (std.mem.eql(u8, raw, "stale")) "stale" else if (std.mem.eql(u8, raw, "none")) "none" else "propagated",
        });
    }

    fn started(ctx: *workflow.Context) ![]const u8 {
        return ctx.propagated().get(tenant) orelse "none";
    }
    const Started = workflow.Def("PropagationNexusStarted", started);

    fn whoRun(ctx: *nexus.Context) !nexus.WorkflowStart(Started) {
        return .{ .options = .{ .id = try std.fmt.allocPrint(ctx.allocator(), "propagation-{s}", .{ctx.info().request_id}) }, .args = .{} };
    }

    const Handler = nexus.ServiceHandler(Service, .{
        nexus.OperationHandler(Who, who),
        nexus.OperationHandler(WhoRun, whoRun),
    });

    /// Calls both operations, the synchronous one with a caller header of the reserved name.
    fn call(ctx: *workflow.Context, endpoint: []const u8) ![]const u8 {
        const options: workflow.NexusOperationOptions = .{
            .endpoint = endpoint,
            .schedule_to_close_timeout = .fromSeconds(60),
            .headers = &.{.{ .name = "Temporal-Zig-Context", .value = "stale" }},
        };
        const sync = try ctx.executeNexusOperation(Service, Who, options, {}).get();
        const run = try ctx.executeNexusOperation(Service, WhoRun, .{ .endpoint = endpoint, .schedule_to_close_timeout = .fromSeconds(60) }, {}).get();
        return std.fmt.allocPrint(ctx.allocator(), "sync={s} workflow={s}", .{ sync, run });
    }
    const Call = workflow.Def("PropagationNexusCaller", call);
};

test "a propagated value reaches a synchronous Nexus handler and the workflow a workflow-run operation starts" {
    const env = try WorkflowEnvironment.startWithOptions(testing.allocator, testing.io, .{ .propagators = &propagators });
    defer env.deinit();
    try env.registerWorkflow(NexusFixtures.Call);
    try env.registerWorkflow(NexusFixtures.Started);
    try env.registerNexusService(NexusFixtures.Handler);
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var suffix: [4]u8 = undefined;
    testing.io.random(&suffix);
    const endpoint = try std.fmt.allocPrint(a, "zig-propagation-{x}", .{suffix});
    try env.createNexusEndpoint(endpoint, .{});

    // The propagated header replaces the caller's header of the same name.
    var values: propagation.Values = .init(a);
    try values.put(tenant, "acme");
    const result = try env.executeWorkflowWithOptions(NexusFixtures.Call, .{ .task_queue = "", .context = &values }, .{endpoint}, a, null);
    try testing.expectEqualStrings("sync=acme (header propagated) workflow=acme", result);

    // Nothing to propagate: the caller's header arrives as it was sent, and doesn't extract.
    const level = testing.log_level;
    defer testing.log_level = level;
    testing.log_level = .err;
    const bare = try env.executeWorkflow(NexusFixtures.Call, .{endpoint}, a, null);
    try testing.expectEqualStrings("sync=none (header stale) workflow=none", bare);
}
