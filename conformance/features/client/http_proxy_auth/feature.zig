//! A client that connects through an HTTP CONNECT proxy with basic-auth credentials runs a
//! workflow. The runner counts the tunnels its proxy opened (config.json expects one, with
//! credentials).
const temporal = @import("temporal");
const harness = @import("harness");
const workflow = temporal.workflow;

const Workflow = workflow.Def("Workflow", runWorkflow);

pub const feature = harness.define(.{
    .workflows = .{Workflow},
    .execute = execute,
});

fn runWorkflow(_: *workflow.Context) ![]const u8 {
    return "done";
}

fn execute(ctx: *harness.Context) !void {
    const proxy = ctx.http_proxy orelse return ctx.fail("the runner gave no HTTP CONNECT proxy", .{});
    // The worker stays on ctx.client, which connects directly; only this client's calls go
    // through the proxy.
    var client = temporal.client.Client.connect(ctx.gpa, ctx.io, .{
        .host_port = ctx.address,
        .namespace = ctx.namespace,
        .http_connect_proxy = .{
            .host_port = proxy,
            .username = harness.Context.http_proxy_username,
            .password = harness.Context.http_proxy_password,
        },
    }) catch |err| return ctx.fail("connect to {s} through HTTP proxy {s} returned error.{t}", .{ ctx.address, proxy, err });
    defer client.close();
    var run = try client.executeWorkflow(Workflow, ctx.startOptions(), .{});
    defer run.deinit();
    try ctx.expectEqualStrings("done", try run.get(ctx.arena));
}
