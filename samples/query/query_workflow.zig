//! Query: a workflow that reports its state through a "state" query while it waits on a
//! durable two-minute timer, and after it has finished.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;

/// The query the workflow answers with its current state.
pub const State = workflow.QueryDef("state", fn () []const u8);

/// What the "state" query reads. It lives in `ctx.allocator()` memory: queries run outside
/// the workflow's coroutine and can arrive after the workflow has returned.
const QueryState = struct {
    state: []const u8 = "started",

    fn current(s: *QueryState) []const u8 {
        return s.state;
    }
};

fn queryWorkflow(ctx: *workflow.Context) !void {
    const log = ctx.logger();
    log.info("QueryWorkflow started", .{});

    const s = try ctx.allocator().create(QueryState);
    s.* = .{};
    ctx.setQueryHandler(State, s, QueryState.current) catch |err| {
        log.err("SetQueryHandler failed", .{ .err = err });
        return err;
    };

    s.state = "waiting on timer";
    // Stands in for whatever a real workflow blocks on: an activity, a signal, or a timer.
    try ctx.io().sleep(.fromSeconds(2 * std.time.s_per_min), .awake);
    log.info("Timer fired", .{});

    s.state = "done";
    log.info("QueryWorkflow completed", .{});
}

pub const QueryWorkflow = workflow.Def("QueryWorkflow", queryWorkflow);
