//! Context propagation against a dev server: values set on a client call reach the workflow,
//! its activities, local activities, children, and the run it continues as.

const std = @import("std");
const activity = @import("activity.zig");
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
