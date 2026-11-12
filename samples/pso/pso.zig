//! Particle swarm optimization: a long iterative optimization as workflows. The parent
//! workflow initializes a swarm, one activity per particle in parallel, then runs the
//! iterations in a child workflow, which updates every particle in parallel each step and
//! continues as new every few steps to keep its history small. The swarm converts itself to
//! a compact binary payload (see swarm.zig). Queries report the child's workflow ID and the
//! child's progress.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
pub const swarm_mod = @import("swarm.zig");
const Function = swarm_mod.Function;
const Particle = swarm_mod.Particle;
const Swarm = swarm_mod.Swarm;

pub const task_queue = "pso";

/// How many swarms the parent tries, each from new random positions.
pub const number_of_attempts = 5;

pub const WorkflowResult = struct {
    msg: []const u8,
    success: bool,
};

const activity_options: workflow.ActivityOptions = .{
    .start_to_close_timeout = .fromSeconds(10 * 60),
    // Short, so that a lost worker's activities move to another worker quickly.
    .heartbeat_timeout = .fromSeconds(2),
    .retry_policy = .{
        .initial_interval = .fromSeconds(1),
        .backoff_coefficient = 2.0,
        .maximum_interval = .fromSeconds(60),
        .maximum_attempts = 5,
    },
};

/// The parent's ID for its current child workflow, empty until it starts one.
pub const ChildQuery = workflow.QueryDef("child", fn () []const u8);
/// The child's last progress message, empty until a step finishes without reaching the goal.
pub const IterationQuery = workflow.QueryDef("iteration", fn () []const u8);

fn initParticle(ctx: *activity.Context, swarm: Swarm) !Particle {
    ctx.logger().info("initParticleActivity started.", .{});
    const source: std.Random.IoSource = .{ .io = ctx.io() };
    var particle: Particle = .random(swarm.settings.function, source.interface());
    particle.updateFitness(swarm.settings.function);
    return particle;
}

pub const InitParticle = activity.Def("initParticleActivityName", initParticle);

fn updateParticle(ctx: *activity.Context, swarm: Swarm, particle_idx: u32) !Particle {
    ctx.logger().info("updateParticleActivity started.", .{});
    if (particle_idx >= swarm.particles.len) {
        return ctx.fail(.{
            .message = try std.fmt.allocPrint(ctx.allocator(), "particle index {d} is out of range: the swarm has {d} particles", .{ particle_idx, swarm.particles.len }),
            .type = "InvalidParticleIndex",
            .non_retryable = true,
        });
    }
    const source: std.Random.IoSource = .{ .io = ctx.io() };
    var particle = swarm.particles[particle_idx];
    particle.updateLocation(swarm, source.interface());
    particle.updateFitness(swarm.settings.function);
    return particle;
}

pub const UpdateParticle = activity.Def("updateParticleActivityName", updateParticle);

/// Tracks a step's particle activities, which run as coroutines in one `std.Io.Group`.
const Pending = struct {
    remaining: usize,
    err: ?anyerror = null,

    fn done(p: *const Pending) bool {
        return p.err != null or p.remaining == 0;
    }
};

fn initOne(ctx: *workflow.Context, swarm: *Swarm, idx: usize, pending: *Pending) void {
    // The activity reads only the settings, and the particles aren't initialized yet.
    const settings_only: Swarm = .{ .settings = swarm.settings, .gbest = swarm.gbest, .particles = &.{} };
    swarm.particles[idx] = ctx.executeActivity(InitParticle, activity_options, .{settings_only}).get() catch |err| {
        pending.err = err;
        return;
    };
    pending.remaining -= 1;
}

fn updateOne(ctx: *workflow.Context, swarm: *Swarm, idx: usize, pending: *Pending) void {
    // Every coroutine schedules its activity before any result arrives, so each activity
    // sees the swarm as it was before the step.
    swarm.particles[idx] = ctx.executeActivity(UpdateParticle, activity_options, .{ swarm.*, @as(u32, @intCast(idx)) }).get() catch |err| {
        pending.err = err;
        return;
    };
    pending.remaining -= 1;
}

/// Runs `func` for every particle in parallel and waits for all of them, or for the first
/// failure.
fn forEachParticle(ctx: *workflow.Context, swarm: *Swarm, comptime func: fn (*workflow.Context, *Swarm, usize, *Pending) void) !void {
    var pending: Pending = .{ .remaining = swarm.particles.len };
    var group: std.Io.Group = .init;
    defer group.cancel(ctx.io());
    for (0..swarm.particles.len) |i| group.async(ctx.io(), func, .{ ctx, swarm, i, &pending });
    try ctx.await(&pending, Pending.done);
    if (pending.err) |err| return err;
}

fn newSwarm(ctx: *workflow.Context, settings: swarm_mod.Settings) !Swarm {
    var swarm: Swarm = .{
        .settings = settings,
        .gbest = .{ .fitness = 1e20 },
        .particles = try ctx.allocator().alloc(Particle, settings.size),
    };
    try forEachParticle(ctx, &swarm, initOne);
    swarm.updateBest();
    return swarm;
}

const ChildId = struct {
    allocator: std.mem.Allocator,

    /// Random on purpose: `ctx.sideEffect` records the ID, so a replay reads it from
    /// history. The workflow's own `io` draws seeded, replayable randomness, so this takes
    /// the process-wide one. Null when out of memory: a side effect returns failures as
    /// values.
    fn generate(c: ChildId) ?[]const u8 {
        const uuid = temporal.client.uuid(c.allocator, std.Io.Threaded.global_single_threaded.io()) catch return null;
        return std.fmt.allocPrint(c.allocator, "PSO_Child_{s}", .{uuid}) catch null;
    }
};

const ParentState = struct {
    child_workflow_id: []const u8 = "",

    fn child(s: *ParentState) []const u8 {
        return s.child_workflow_id;
    }
};

fn psoWorkflow(ctx: *workflow.Context, function_name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    const a = ctx.allocator();
    logger.info("Optimizing function", .{ .function = function_name });

    const state = try a.create(ParentState);
    state.* = .{};
    try ctx.setQueryHandler(ChildQuery, state, ParentState.child);

    const function = std.meta.stringToEnum(Function, function_name) orelse return ctx.fail(.{
        .message = try std.fmt.allocPrint(a, "unknown function \"{s}\": use one of sphere, rosenbrock, griewank", .{function_name}),
        .type = "UnknownFunction",
    });
    const settings: swarm_mod.Settings = .default(function);

    for (1..number_of_attempts + 1) |attempt| {
        logger.info("Attempt", .{ .attempt = attempt });

        const swarm = newSwarm(ctx, settings) catch |err| {
            logger.err("Optimization failed.", .{ .err = err });
            return err;
        };

        // The child's ID is unique per attempt.
        state.child_workflow_id = try ctx.sideEffect(?[]const u8, ChildId{ .allocator = a }, ChildId.generate) orelse return error.OutOfMemory;
        const child = ctx.executeChildWorkflow(PsoChildWorkflow, .{
            .id = state.child_workflow_id,
            .run_timeout = .fromSeconds(60),
            .task_timeout = .fromSeconds(60),
        }, .{ swarm, 1 });
        const result = child.get() catch |err| {
            logger.err("Parent execution received child execution failure.", .{ .err = err });
            return err;
        };
        if (result.success) {
            const msg = try std.fmt.allocPrint(a, "Optimization was successful at attempt #{d}. {s}", .{ attempt, result.msg });
            logger.info("Optimization was successful.", .{ .result = msg });
            return msg;
        }
    }

    const msg = try std.fmt.allocPrint(a, "Unable to reach goal after {d} attempts", .{number_of_attempts});
    logger.info("Optimization did not reach the goal.", .{ .result = msg });
    return msg;
}

pub const PsoWorkflow = workflow.Def("PSOWorkflow", psoWorkflow);

const ChildState = struct {
    iteration_message: []const u8 = "",

    fn iteration(s: *ChildState) []const u8 {
        return s.iteration_message;
    }
};

/// Runs the optimization from `starting_step` until the swarm reaches the goal or the last
/// step, continuing as new every `continue_as_new_every` steps with the swarm so far.
fn psoChildWorkflow(ctx: *workflow.Context, initial: Swarm, starting_step: u32) !WorkflowResult {
    const logger = ctx.logger();
    const a = ctx.allocator();
    logger.info("Child workflow execution started.", .{});

    const state = try a.create(ChildState);
    state.* = .{};
    try ctx.setQueryHandler(IterationQuery, state, ChildState.iteration);

    var swarm = initial;
    const settings = swarm.settings;
    var step = starting_step;
    while (step <= settings.steps) : (step += 1) {
        logger.info("Iteration", .{ .step = step });
        forEachParticle(ctx, &swarm, updateOne) catch |err| {
            logger.err("Error in swarm loop.", .{ .err = err });
            return err;
        };
        swarm.updateBest();
        if (swarm.gbest.fitness < Function.goal) break;

        state.iteration_message = try std.fmt.allocPrint(a, "Step {d} :: min err={e:.5}", .{ step, swarm.gbest.fitness });
        if (settings.print_every != 0 and step % settings.print_every == 0) {
            logger.info("Iteration progress", .{ .message = state.iteration_message });
        }
        if (step == settings.steps) break;
        if (settings.continue_as_new_every != 0 and step % settings.continue_as_new_every == 0) {
            return ctx.continueAsNew(PsoChildWorkflow, .{}, .{ swarm, step + 1 });
        }
    }

    const fitness = swarm.gbest.fitness;
    if (fitness < Function.goal) {
        const msg = try std.fmt.allocPrint(a, "Yay! Goal was reached @ step {d} (fitness={e:.2}) :-)", .{ step, fitness });
        logger.info("Goal reached.", .{ .result = msg });
        return .{ .msg = msg, .success = true };
    }
    const msg = try std.fmt.allocPrint(a, "Goal was not reached after {d} steps (fitness={e:.2}) :-)", .{ step, fitness });
    logger.info("Goal not reached.", .{ .result = msg });
    return .{ .msg = msg, .success = false };
}

pub const PsoChildWorkflow = workflow.Def("PSOChildWorkflow", psoChildWorkflow);
