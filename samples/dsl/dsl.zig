//! DSL: one workflow that interprets a workflow definition passed to it as its argument. A
//! definition has variables and a tree of statements: an activity call, a sequence of
//! statements run one after another, or a parallel block whose branches run concurrently.
//!
//! Activities are called by name with the values of the variables their arguments name; an
//! activity's result can be stored in a new variable for later activities to read. The
//! starter reads definitions from ZON files (workflow1.zon, workflow2.zon) with `std.zon`,
//! and they reach the workflow as JSON with the same field names.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;

/// A workflow definition: the initial variables and the statement to run.
pub const Definition = struct {
    variables: []const Variable = &.{},
    root: Statement,
};

pub const Variable = struct {
    name: []const u8,
    value: []const u8,
};

/// The building block of a definition.
pub const Statement = union(enum) {
    activity: ActivityInvocation,
    sequence: Sequence,
    parallel: Parallel,
};

/// Statements run one after another.
pub const Sequence = struct {
    elements: []const Statement,
};

/// Statements run concurrently. When one branch fails, the others are canceled and the block
/// fails with that branch's error.
pub const Parallel = struct {
    branches: []const Statement,
};

/// Calls the activity `name` with the values of the variables named in `arguments` (an
/// unknown variable reads as an empty string) and, if `result` is not empty, stores the
/// activity's result in the variable `result`.
pub const ActivityInvocation = struct {
    name: []const u8,
    arguments: []const []const u8 = &.{},
    result: []const u8 = "",
};

/// Variable values by name. Only looked up, never iterated, so its order doesn't matter.
const Bindings = std.StringHashMapUnmanaged([]const u8);

/// Explicit because statements execute recursively.
const ExecuteError = workflow.ActivityError;

fn simpleDsl(ctx: *workflow.Context, definition: Definition) !void {
    var bindings: Bindings = .empty;
    for (definition.variables) |v| try bindings.put(ctx.allocator(), v.name, v.value);

    const logger = ctx.logger();
    execute(ctx, &bindings, &definition.root) catch |err| {
        // Logged at warn: the sample's tests fail a run on this path, and a test that logs
        // at error level fails under the test runner.
        logger.warn("DSL Workflow failed.", .{ .err = err });
        return err;
    };
    logger.info("DSL Workflow completed.", .{});
}

pub const SimpleDslWorkflow = workflow.Def("SimpleDSLWorkflow", simpleDsl);

fn execute(ctx: *workflow.Context, bindings: *Bindings, statement: *const Statement) ExecuteError!void {
    switch (statement.*) {
        .activity => |*a| try executeActivity(ctx, bindings, a),
        .sequence => |s| for (s.elements) |*element| try execute(ctx, bindings, element),
        .parallel => |p| try executeParallel(ctx, bindings, p),
    }
}

fn executeActivity(ctx: *workflow.Context, bindings: *Bindings, a: *const ActivityInvocation) ExecuteError!void {
    const input = try ctx.allocator().alloc([]const u8, a.arguments.len);
    for (a.arguments, input) |name, *value| value.* = bindings.get(name) orelse "";
    const options: workflow.ActivityOptions = .{ .start_to_close_timeout = .fromSeconds(10) };
    const result = try ctx.executeActivityByName([]const u8, a.name, options, .{input}).get();
    if (a.result.len > 0) try bindings.put(ctx.allocator(), a.result, result);
}

const ParallelState = struct {
    pending: usize,
    err: ?ExecuteError = null,

    fn done(s: *const ParallelState) bool {
        return s.err != null or s.pending == 0;
    }
};

fn executeParallel(ctx: *workflow.Context, bindings: *Bindings, p: Parallel) ExecuteError!void {
    var state: ParallelState = .{ .pending = p.branches.len };
    var group: std.Io.Group = .init;
    // Cancels the branches still running after one failed: their activities are canceled.
    defer group.cancel(ctx.io());
    for (p.branches) |*branch| group.async(ctx.io(), runBranch, .{ ctx, bindings, branch, &state });

    try ctx.await(&state, ParallelState.done);
    if (state.err) |err| return err;
}

fn runBranch(ctx: *workflow.Context, bindings: *Bindings, branch: *const Statement, state: *ParallelState) void {
    defer state.pending -= 1;
    execute(ctx, bindings, branch) catch |err| {
        if (state.err == null) state.err = err;
    };
}

/// The activities a definition can call. Each logs its input and returns "Result_" followed by
/// its activity type name.
pub const Activities = struct {
    pub fn sampleActivity1(a: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        return a.run(ctx, input);
    }

    pub fn sampleActivity2(a: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        return a.run(ctx, input);
    }

    pub fn sampleActivity3(a: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        return a.run(ctx, input);
    }

    pub fn sampleActivity4(a: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        return a.run(ctx, input);
    }

    pub fn sampleActivity5(a: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        return a.run(ctx, input);
    }

    fn run(_: *Activities, ctx: *activity.Context, input: []const []const u8) ![]const u8 {
        const name = ctx.info().activity_type;
        const joined = try std.mem.join(ctx.allocator(), " ", input);
        ctx.logger().info("Run activity", .{ .name = name, .input = try std.fmt.allocPrint(ctx.allocator(), "[{s}]", .{joined}) });
        return std.fmt.allocPrint(ctx.allocator(), "Result_{s}", .{name});
    }
};

pub const SampleActivity1 = activity.Def("SampleActivity1", Activities.sampleActivity1);
pub const SampleActivity2 = activity.Def("SampleActivity2", Activities.sampleActivity2);
pub const SampleActivity3 = activity.Def("SampleActivity3", Activities.sampleActivity3);
pub const SampleActivity4 = activity.Def("SampleActivity4", Activities.sampleActivity4);
pub const SampleActivity5 = activity.Def("SampleActivity5", Activities.sampleActivity5);

/// Every activity, for `registerActivitiesWith`.
pub const all_activities = .{ SampleActivity1, SampleActivity2, SampleActivity3, SampleActivity4, SampleActivity5 };

/// Parses a definition from ZON source. Memory comes from `arena`; on `error.ParseZon`,
/// `diagnostics` describes the problem.
pub fn parseDefinition(arena: std.mem.Allocator, source: [:0]const u8, diagnostics: *std.zon.parse.Diagnostics) error{ OutOfMemory, ParseZon }!Definition {
    return std.zon.parse.fromSlice(Definition, .{ .gpa = arena, .arena = arena, .source = source, .diagnostics = diagnostics });
}
