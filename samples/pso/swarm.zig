//! The state of a particle swarm optimization: objective functions, settings, particles and
//! the swarm, with the arithmetic the activities and workflows run on them.
//!
//! The swarm is passed to every activity and from run to run of the child workflow, so
//! `Swarm` and `Particle` convert themselves to compact binary payloads instead of JSON. All
//! integers and floats are little-endian; a float is its IEEE 754 bits as a u64.
//!
//!     binary/pso-particle: position (dim floats, fitness), best position (dim floats,
//!                          fitness), velocity (dim floats)
//!     binary/pso-swarm:    function (u8), size, print_every, continue_as_new_every,
//!                          steps (u32 each), c1, c2, inertia (floats), best position
//!                          (dim floats, fitness), particle count (u32), then each
//!                          particle as above

const std = @import("std");
const temporal = @import("temporal");
const converter = temporal.converter;
const Allocator = std.mem.Allocator;

/// The problem's dimensionality, the same for every function.
pub const dim = 3;
pub const Vector = [dim]f64;

/// The functions the swarm can minimize. Each has its minimum, 0, inside its range.
pub const Function = enum(u8) {
    sphere,
    rosenbrock,
    griewank,

    /// The optimization stops once the best fitness is below this.
    pub const goal = 1e-5;

    pub fn lo(f: Function) f64 {
        return switch (f) {
            .sphere => -100,
            .rosenbrock => -2.048,
            .griewank => -600,
        };
    }

    pub fn hi(f: Function) f64 {
        return -f.lo();
    }

    pub fn evaluate(f: Function, vec: Vector) f64 {
        var sum: f64 = 0;
        switch (f) {
            .sphere => for (vec) |x| {
                sum += x * x;
            },
            .rosenbrock => for (0..dim - 1) |i| {
                const a = vec[i + 1] - vec[i] * vec[i];
                const b = 1 - vec[i];
                sum += 100 * a * a + b * b;
            },
            .griewank => {
                var prod: f64 = 1;
                for (vec, 1..) |x, i| {
                    sum += x * x;
                    prod *= @cos(x / @sqrt(@as(f64, @floatFromInt(i))));
                }
                return sum / 4000 - prod + 1;
            },
        }
        return sum;
    }
};

const max_size = 100;
/// The default inertia weight (Clerc, 2002).
const default_inertia = 0.7298;

/// The swarm size for a problem of `dimensions`: 10 + 2·√dimensions, rounded, at most
/// `max`.
pub fn swarmSize(dimensions: u32, max: u32) u32 {
    const s = 10 + 2 * @sqrt(@as(f64, @floatFromInt(dimensions)));
    return @min(@as(u32, @intFromFloat(@floor(s + 0.5))), max);
}

pub const Settings = struct {
    function: Function,
    /// The number of particles.
    size: u32,
    /// Log progress every this many steps; 0 logs none.
    print_every: u32 = 10,
    /// Continue the child workflow as new every this many steps, to keep its history small;
    /// 0 never does.
    continue_as_new_every: u32 = 10,
    /// The maximum number of steps.
    steps: u32 = 100_000,
    /// Cognitive coefficient: the pull towards a particle's own best position.
    c1: f64 = 1.496,
    /// Social coefficient: the pull towards the swarm's best position.
    c2: f64 = 1.496,
    /// Inertia weight: how much of its velocity a particle keeps from step to step.
    inertia: f64 = default_inertia,

    pub fn default(function: Function) Settings {
        return .{ .function = function, .size = swarmSize(dim, max_size) };
    }
};

pub const Position = struct {
    location: Vector = @splat(0),
    fitness: f64 = 0,

    pub fn random(function: Function, rng: std.Random) Position {
        var p: Position = .{};
        for (&p.location) |*x| x.* = function.lo() + (function.hi() - function.lo()) * rng.float(f64);
        return p;
    }

    pub fn isBetterThan(p: Position, other: Position) bool {
        return p.fitness < other.fitness;
    }
};

pub const Particle = struct {
    position: Position,
    /// The best position this particle has visited.
    pbest: Position,
    velocity: Vector,

    pub const encoding = "binary/pso-particle";
    const encoded_len = (3 * dim + 2) * 8;

    /// A particle at a random position with a random velocity. Its fitness isn't evaluated
    /// yet; its best position starts out worse than any.
    pub fn random(function: Function, rng: std.Random) Particle {
        const position: Position = .random(function, rng);
        var p: Particle = .{
            .position = position,
            .pbest = .{ .location = position.location, .fitness = 1e20 },
            .velocity = undefined,
        };
        const width = function.hi() - function.lo();
        for (&p.velocity) |*v| {
            const a = function.lo() + width * rng.float(f64);
            const b = function.lo() + width * rng.float(f64);
            v.* = (a - b) / 2;
        }
        return p;
    }

    /// Moves the particle by its velocity, which is pulled at random strengths towards its
    /// own best position and the swarm's.
    pub fn updateLocation(p: *Particle, swarm: Swarm, rng: std.Random) void {
        const s = swarm.settings;
        for (&p.velocity, &p.position.location, p.pbest.location, swarm.gbest.location) |*v, *x, best, gbest| {
            const rho1 = s.c1 * rng.float(f64);
            const rho2 = s.c2 * rng.float(f64);
            v.* = s.inertia * v.* + rho1 * (best - x.*) + rho2 * (gbest - x.*);
            x.* += v.*;
        }
    }

    pub fn updateFitness(p: *Particle, function: Function) void {
        p.position.fitness = function.evaluate(p.position.location);
        if (p.position.isBetterThan(p.pbest)) p.pbest = p.position;
    }

    pub fn toPayload(p: Particle, arena: Allocator) error{OutOfMemory}!converter.Payload {
        var aw: std.Io.Writer.Allocating = try .initCapacity(arena, encoded_len);
        writeParticle(&aw.writer, p) catch return error.OutOfMemory;
        return converter.payloadWithEncoding(arena, encoding, try aw.toOwnedSlice());
    }

    pub fn fromPayload(_: Allocator, payload: converter.Payload) converter.DecodeError!Particle {
        try expectEncoding(payload, encoding);
        var r: std.Io.Reader = .fixed(payload.data);
        const p = readParticle(&r) catch return error.InvalidPayload;
        if (r.bufferedLen() != 0) return error.InvalidPayload;
        return p;
    }
};

pub const Swarm = struct {
    settings: Settings,
    /// The best position any particle has visited.
    gbest: Position,
    particles: []Particle,

    pub const encoding = "binary/pso-swarm";

    /// Moves the swarm's best position to the best of its particles' best positions.
    pub fn updateBest(s: *Swarm) void {
        for (s.particles) |p| {
            if (p.pbest.isBetterThan(s.gbest)) s.gbest = p.pbest;
        }
    }

    pub fn toPayload(s: Swarm, arena: Allocator) error{OutOfMemory}!converter.Payload {
        var aw: std.Io.Writer.Allocating = try .initCapacity(arena, 128 + s.particles.len * Particle.encoded_len);
        writeSwarm(&aw.writer, s) catch return error.OutOfMemory;
        return converter.payloadWithEncoding(arena, encoding, try aw.toOwnedSlice());
    }

    /// The particles are allocated from `arena`.
    pub fn fromPayload(arena: Allocator, payload: converter.Payload) converter.DecodeError!Swarm {
        try expectEncoding(payload, encoding);
        var r: std.Io.Reader = .fixed(payload.data);
        const s = readSwarm(arena, &r) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return error.InvalidPayload,
        };
        if (r.bufferedLen() != 0) return error.InvalidPayload;
        return s;
    }
};

fn expectEncoding(payload: converter.Payload, expected: []const u8) converter.DecodeError!void {
    const enc = converter.encodingOf(payload) orelse return error.UnknownEncoding;
    if (!std.mem.eql(u8, enc, expected)) return error.UnknownEncoding;
}

fn writeFloat(w: *std.Io.Writer, x: f64) std.Io.Writer.Error!void {
    try w.writeInt(u64, @bitCast(x), .little);
}

fn readFloat(r: *std.Io.Reader) std.Io.Reader.Error!f64 {
    return @bitCast(try r.takeInt(u64, .little));
}

fn writePosition(w: *std.Io.Writer, p: Position) std.Io.Writer.Error!void {
    for (p.location) |x| try writeFloat(w, x);
    try writeFloat(w, p.fitness);
}

fn readPosition(r: *std.Io.Reader) std.Io.Reader.Error!Position {
    var p: Position = .{};
    for (&p.location) |*x| x.* = try readFloat(r);
    p.fitness = try readFloat(r);
    return p;
}

fn writeParticle(w: *std.Io.Writer, p: Particle) std.Io.Writer.Error!void {
    try writePosition(w, p.position);
    try writePosition(w, p.pbest);
    for (p.velocity) |v| try writeFloat(w, v);
}

fn readParticle(r: *std.Io.Reader) std.Io.Reader.Error!Particle {
    var p: Particle = .{ .position = try readPosition(r), .pbest = try readPosition(r), .velocity = undefined };
    for (&p.velocity) |*v| v.* = try readFloat(r);
    return p;
}

fn writeSwarm(w: *std.Io.Writer, s: Swarm) std.Io.Writer.Error!void {
    const st = s.settings;
    try w.writeByte(@backingInt(st.function));
    for ([_]u32{ st.size, st.print_every, st.continue_as_new_every, st.steps }) |n| try w.writeInt(u32, n, .little);
    for ([_]f64{ st.c1, st.c2, st.inertia }) |x| try writeFloat(w, x);
    try writePosition(w, s.gbest);
    try w.writeInt(u32, @intCast(s.particles.len), .little);
    for (s.particles) |p| try writeParticle(w, p);
}

fn readSwarm(arena: Allocator, r: *std.Io.Reader) (std.Io.Reader.Error || error{ OutOfMemory, InvalidPayload })!Swarm {
    const function = std.enums.fromInt(Function, try r.takeByte()) orelse return error.InvalidPayload;
    const settings: Settings = .{
        .function = function,
        .size = try r.takeInt(u32, .little),
        .print_every = try r.takeInt(u32, .little),
        .continue_as_new_every = try r.takeInt(u32, .little),
        .steps = try r.takeInt(u32, .little),
        .c1 = try readFloat(r),
        .c2 = try readFloat(r),
        .inertia = try readFloat(r),
    };
    const gbest = try readPosition(r);
    const count = try r.takeInt(u32, .little);
    // Check the count against the bytes left before allocating for it.
    if (r.bufferedLen() != @as(usize, count) * Particle.encoded_len) return error.InvalidPayload;
    const particles = try arena.alloc(Particle, count);
    for (particles) |*p| p.* = try readParticle(r);
    return .{ .settings = settings, .gbest = gbest, .particles = particles };
}
