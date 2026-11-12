//! Tests for the pso workflows.

const std = @import("std");
const temporal = @import("temporal");
const pso = @import("pso.zig");
const swarm_mod = pso.swarm_mod;

const testing = std.testing;
const WorkflowEnvironment = temporal.testing.WorkflowEnvironment;

fn startEnv() !*WorkflowEnvironment {
    const env = try WorkflowEnvironment.start(testing.allocator, testing.io);
    errdefer env.deinit();
    try env.registerWorkflow(pso.PsoWorkflow);
    try env.registerWorkflow(pso.PsoChildWorkflow);
    try env.registerActivity(pso.InitParticle);
    try env.registerActivity(pso.UpdateParticle);
    return env;
}

test "optimizes the sphere function at the first attempt, and names its child" {
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var run = try env.startWorkflow(pso.PsoWorkflow, .{ .task_queue = "" }, .{"sphere"});
    defer run.deinit();
    const result = try env.getResult(pso.PsoWorkflow, &run, a, null);
    try testing.expect(std.mem.startsWith(u8, result, "Optimization was successful at attempt #1. Yay! Goal was reached @ step "));

    var child_id: ?[]const u8 = null;
    for ((try env.client.fetchHistory(a, run.id, run.run_id)).events.items) |event| {
        switch (event.attributes orelse continue) {
            .start_child_workflow_execution_initiated_event_attributes => |started| {
                try testing.expectEqual(null, child_id);
                child_id = started.workflow_id;
            },
            else => {},
        }
    }
    try testing.expect(std.mem.startsWith(u8, child_id.?, "PSO_Child_"));
    try testing.expectEqualStrings(child_id.?, try env.queryWorkflow(pso.ChildQuery, a, run.id, .{}));
}

test "the child continues as new between steps and reports its progress" {
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var settings: swarm_mod.Settings = .default(.sphere);
    settings.steps = 3;
    settings.continue_as_new_every = 2;
    var prng: std.Random.DefaultPrng = .init(3);
    const particles = try a.alloc(swarm_mod.Particle, settings.size);
    for (particles) |*p| {
        p.* = .random(.sphere, prng.random());
        p.updateFitness(.sphere);
    }
    var swarm: swarm_mod.Swarm = .{ .settings = settings, .gbest = .{ .fitness = 1e20 }, .particles = particles };
    swarm.updateBest();

    // From positions this far from the minimum, three steps can't reach the goal.
    const child_id = "pso-child-test";
    var run = try env.startWorkflow(pso.PsoChildWorkflow, .{ .id = child_id, .task_queue = "" }, .{ swarm, 1 });
    defer run.deinit();
    const result = try env.getResult(pso.PsoChildWorkflow, &run, a, null);
    try testing.expect(!result.success);
    try testing.expect(std.mem.startsWith(u8, result.msg, "Goal was not reached after 3 steps (fitness="));

    const progress = try env.queryWorkflow(pso.IterationQuery, a, child_id, .{});
    try testing.expect(std.mem.startsWith(u8, progress, "Step 3 :: min err="));

    // Steps 1 and 2 ran in the first run, which continued as new; step 3 in the second.
    const last = (try env.client.fetchHistory(a, child_id, "")).events.items;
    try testing.expect(last[last.len - 1].attributes.? == .workflow_execution_completed_event_attributes);
    const first_run_id = last[0].attributes.?.workflow_execution_started_event_attributes.continued_execution_run_id;
    const first = (try env.client.fetchHistory(a, child_id, first_run_id)).events.items;
    try testing.expect(first[first.len - 1].attributes.? == .workflow_execution_continued_as_new_event_attributes);
    try testing.expectEqual(0, first[0].attributes.?.workflow_execution_started_event_attributes.continued_execution_run_id.len);
    try env.replayRun(child_id, first_run_id);
}

test "an unknown function fails the workflow" {
    const env = try startEnv();
    defer env.deinit();

    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    var failure: ?temporal.failure.Failure = null;
    try testing.expectError(error.WorkflowFailed, env.executeWorkflow(pso.PsoWorkflow, .{"ackley"}, arena.allocator(), &failure));
    try testing.expectEqualStrings("unknown function \"ackley\": use one of sphere, rosenbrock, griewank", failure.?.message);
}
