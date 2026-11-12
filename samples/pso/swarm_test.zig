//! Tests for the swarm state, its arithmetic and its payloads.

const std = @import("std");
const temporal = @import("temporal");
const swarm = @import("swarm.zig");

const testing = std.testing;
const converter = temporal.converter;

test "each function's minimum is 0 inside its range" {
    try testing.expectEqual(0, swarm.Function.sphere.evaluate(@splat(0)));
    try testing.expectEqual(0, swarm.Function.rosenbrock.evaluate(@splat(1)));
    try testing.expectEqual(0, swarm.Function.griewank.evaluate(@splat(0)));
    try testing.expectEqual(12, swarm.Function.sphere.evaluate(.{ 2, -2, 2 }));
}

test "the swarm size grows with the dimensions up to the maximum" {
    try testing.expectEqual(13, swarm.swarmSize(3, 100));
    try testing.expectEqual(12, swarm.swarmSize(1, 100));
    try testing.expectEqual(5, swarm.swarmSize(3, 5));
}

test "a particle's best position follows its fitness" {
    var prng: std.Random.DefaultPrng = .init(1);
    var p: swarm.Particle = .random(.sphere, prng.random());
    for (p.position.location) |x| try testing.expect(x >= -100 and x <= 100);
    p.updateFitness(.sphere);
    try testing.expectEqual(p.position, p.pbest);

    // A worse position leaves the best one alone.
    const best = p.pbest;
    p.position.location = .{ 100, 100, 100 };
    p.updateFitness(.sphere);
    try testing.expectEqual(best, p.pbest);
}

test "the swarm's best position is its particles' best" {
    var particles = [_]swarm.Particle{
        .{ .position = .{}, .pbest = .{ .location = .{ 1, 1, 1 }, .fitness = 3 }, .velocity = @splat(0) },
        .{ .position = .{}, .pbest = .{ .location = .{ 1, 0, 0 }, .fitness = 1 }, .velocity = @splat(0) },
    };
    var s: swarm.Swarm = .{ .settings = .default(.sphere), .gbest = .{ .fitness = 1e20 }, .particles = &particles };
    s.updateBest();
    try testing.expectEqual(particles[1].pbest, s.gbest);
}

test "a swarm round-trips through its binary payload" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var prng: std.Random.DefaultPrng = .init(7);
    var settings: swarm.Settings = .default(.griewank);
    settings.steps = 42;
    settings.continue_as_new_every = 3;
    var particles: [13]swarm.Particle = undefined;
    for (&particles) |*p| {
        p.* = .random(.griewank, prng.random());
        p.updateFitness(.griewank);
    }
    var original: swarm.Swarm = .{ .settings = settings, .gbest = .{ .fitness = 1e20 }, .particles = &particles };
    original.updateBest();

    const payload = try converter.toPayload(a, original);
    try testing.expectEqualStrings(swarm.Swarm.encoding, converter.encodingOf(payload).?);
    const decoded = try converter.fromPayload(swarm.Swarm, a, payload);
    try testing.expectEqual(original.settings, decoded.settings);
    try testing.expectEqual(original.gbest, decoded.gbest);
    try testing.expectEqualSlices(swarm.Particle, original.particles, decoded.particles);

    const particle = try converter.fromPayload(swarm.Particle, a, try converter.toPayload(a, particles[4]));
    try testing.expectEqual(particles[4], particle);
}

test "a truncated or foreign payload doesn't decode as a swarm" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var particles = [_]swarm.Particle{.{ .position = .{}, .pbest = .{}, .velocity = @splat(0) }};
    var payload = try converter.toPayload(a, swarm.Swarm{ .settings = .default(.sphere), .gbest = .{}, .particles = &particles });
    payload.data = payload.data[0 .. payload.data.len - 1];
    try testing.expectError(error.InvalidPayload, converter.fromPayload(swarm.Swarm, a, payload));

    const json = try converter.toPayload(a, "a swarm");
    try testing.expectError(error.UnknownEncoding, converter.fromPayload(swarm.Swarm, a, json));
    try testing.expectError(error.UnknownEncoding, converter.fromPayload(swarm.Particle, a, json));
}
