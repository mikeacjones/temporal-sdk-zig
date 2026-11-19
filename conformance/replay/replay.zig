//! Replays every history stored under conformance/histories against the workflows of the
//! feature it belongs to, so a change that breaks replay of an earlier recording fails
//! `zig build test`. Everything it reads is compiled in: it opens and writes no file, and
//! needs no server.
const std = @import("std");
const temporal = @import("temporal");
const harness = @import("harness");
const features = @import("features");
const stored_histories = @import("stored_histories");
const Allocator = std.mem.Allocator;
const WorkflowReplayer = temporal.testing.WorkflowReplayer;

const exclusions_path = "conformance/replay/exclusions.zon";

/// A feature left out of replay and of the missing-history rule because of an SDK defect.
const Exclusion = struct { feature: []const u8, bead: []const u8, reason: []const u8 };

const Input = struct {
    files: []const harness.stored.File,
    features: []const features.Entry,
    exclusions: []const Exclusion,
    version: []const u8,
};

/// Checks the store against the features and replays it; one message per defect.
fn check(gpa: Allocator, io: std.Io, arena: Allocator, input: Input, problems: *std.ArrayList([]const u8)) error{OutOfMemory}!void {
    const paths = try arena.alloc([]const u8, input.features.len);
    for (input.features, paths) |entry, *path| path.* = entry.path;

    // 1. The exclusion list. A malformed entry that names a written feature still exempts it.
    var excluded: std.StringArrayHashMapUnmanaged(Exclusion) = .empty;
    for (input.exclusions) |exclusion| {
        if (findFeature(input.features, exclusion.feature) == null) {
            try problems.append(arena, try std.fmt.allocPrint(arena, exclusions_path ++ ": {s} is not a written feature", .{exclusion.feature}));
            continue;
        }
        const slot = try excluded.getOrPut(arena, exclusion.feature);
        if (slot.found_existing) {
            try problems.append(arena, try std.fmt.allocPrint(arena, exclusions_path ++ ": {s} is listed twice", .{exclusion.feature}));
            continue;
        }
        slot.value_ptr.* = exclusion;
        if (isBlank(exclusion.bead) or isBlank(exclusion.reason))
            try problems.append(arena, try std.fmt.allocPrint(arena, exclusions_path ++ ": {s} has an empty bead or reason", .{exclusion.feature}));
    }

    // 2. The store itself, excluded features included.
    const directories = try harness.stored.load(arena, input.files, problems);

    // 3. Every file belongs to a written feature; note which features have the current version.
    var current: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (input.files) |file| {
        // A file outside any directory is already a problem from `load`.
        const slash = std.mem.lastIndexOfScalar(u8, file.path, '/') orelse continue;
        const location = harness.stored.locate(paths, file.path[0..slash]) orelse {
            try problems.append(arena, try std.fmt.allocPrint(arena, "conformance/histories/{s}: no feature under conformance/features owns this path", .{file.path}));
            continue;
        };
        if (std.mem.eql(u8, location.version, input.version) and harness.stored.parseFileName(file.path[slash + 1 ..]) != null)
            try current.put(arena, location.feature, {});
    }

    // 4. Every stored workflow type is declared by its feature.
    const declared = try arena.alloc([]const bool, directories.len);
    for (directories, declared) |directory, *flags| {
        flags.* = &.{};
        const location = harness.stored.locate(paths, directory.path) orelse continue;
        const feature = findFeature(input.features, location.feature).?;
        const ok = try arena.alloc(bool, directory.histories.len);
        for (directory.histories, ok) |history, *is_declared| {
            is_declared.* = contains(feature.workflow_types, history.workflow_type);
            if (!is_declared.*) try problems.append(arena, try std.fmt.allocPrint(
                arena,
                "conformance/histories/{s}: workflow type \"{s}\" is not in .workflows of feature {s}",
                .{ history.path, history.workflow_type, location.feature },
            ));
        }
        flags.* = ok;
    }

    // 5. The missing-history rule.
    for (input.features) |entry| {
        if (entry.feature.workflow_types.len == 0 or excluded.contains(entry.path) or current.contains(entry.path)) continue;
        try problems.append(arena, try std.fmt.allocPrint(
            arena,
            "feature {s} declares workflows but has no stored history for version {s}; record it with: zig build conformance -Drecord-histories -Dfeature={s}",
            .{ entry.path, input.version, entry.path },
        ));
    }

    // 6. Replay, one replayer per leaf directory.
    for (directories, declared) |directory, flags| {
        const location = harness.stored.locate(paths, directory.path) orelse continue;
        if (excluded.contains(location.feature)) continue;
        try replayDirectory(gpa, io, arena, findFeature(input.features, location.feature).?, directory, flags, problems);
    }

    // 7. The standing reminder of every feature left uncovered.
    for (excluded.values()) |exclusion| {
        std.debug.print("conformance replay: skipping {s} ({s}): {s}\n", .{ exclusion.feature, exclusion.bead, exclusion.reason });
    }
}

fn replayDirectory(
    gpa: Allocator,
    io: std.Io,
    arena: Allocator,
    feature: *const harness.Feature,
    directory: harness.stored.Directory,
    declared: []const bool,
    problems: *std.ArrayList([]const u8),
) error{OutOfMemory}!void {
    var batch: std.ArrayList(temporal.history.WorkflowHistory) = .empty;
    var batch_paths: std.ArrayList([]const u8) = .empty;
    for (directory.histories, declared) |history, is_declared| {
        if (!is_declared) continue;
        try batch.append(arena, .{ .workflow_id = history.workflow_id, .history = history.history });
        try batch_paths.append(arena, history.path);
    }
    if (batch.items.len == 0) return;

    var replayer: WorkflowReplayer = .initWithOptions(gpa, io, feature.replayerOptions(directory.namespace, directory.task_queue));
    defer replayer.deinit();
    feature.registerReplayer(&replayer) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            try problems.append(arena, try std.fmt.allocPrint(arena, "conformance/histories/{s}: registering the workflows of the feature failed with error.{t}", .{ directory.path, err }));
            return;
        },
    };
    const results = replayer.replayHistories(arena, batch.items) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            for (batch_paths.items) |path| try problems.append(arena, try std.fmt.allocPrint(arena, "conformance/histories/{s}: replay failed with error.{t}: the replay worker could not run", .{ path, err }));
            return;
        },
    };
    for (results, batch_paths.items) |result, path| {
        const err = result.err orelse continue;
        try problems.append(arena, try std.fmt.allocPrint(arena, "conformance/histories/{s}: replay failed with error.{t}: {s}", .{ path, err, result.message }));
    }
}

fn findFeature(entries: []const features.Entry, path: []const u8) ?*const harness.Feature {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.path, path)) return entry.feature;
    }
    return null;
}

fn contains(names: []const []const u8, name: []const u8) bool {
    for (names) |candidate| {
        if (std.mem.eql(u8, candidate, name)) return true;
    }
    return false;
}

fn isBlank(text: []const u8) bool {
    return std.mem.trim(u8, text, " \t\r\n").len == 0;
}

fn expectNoProblems(problems: []const []const u8) !void {
    for (problems) |message| std.debug.print("{s}\n", .{message});
    if (problems.len != 0) return error.TestUnexpectedResult;
}

test "conformance stored histories replay against their features" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    var problems: std.ArrayList([]const u8) = .empty;

    var diag: std.zon.parse.Diagnostics = undefined;
    const exclusions = std.zon.parse.fromSlice([]const Exclusion, .{
        .gpa = std.testing.allocator,
        .arena = arena,
        .source = @embedFile("exclusions.zon"),
        .diagnostics = &diag,
    }) catch |err| {
        diag.log(exclusions_path);
        return err;
    };
    try check(std.testing.allocator, std.testing.io, arena, .{
        .files = &stored_histories.files,
        .features = &features.all,
        .exclusions = exclusions,
        .version = temporal.version,
    }, &problems);
    try expectNoProblems(problems.items);
}

// ---------------------------------------------------------------- tests over hand-made inputs

const Fixture = struct {
    fn greet(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
        return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
    }
    const Greet = temporal.activity.Def("Greet", greet);

    /// The workflow that produced testdata/1-HelloWorld.json.
    fn hello(ctx: *temporal.workflow.Context, name: []const u8) ![]const u8 {
        const greeting = ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
        return greeting.get();
    }
    /// The same type name, completing without the activity the history records.
    fn other(_: *temporal.workflow.Context, _: []const u8) ![]const u8 {
        return "no activity here";
    }
    fn execute(_: *harness.Context) !void {}

    const matching = harness.define(.{ .workflows = .{temporal.workflow.Def("HelloWorld", hello)}, .execute = execute });
    const changed = harness.define(.{ .workflows = .{temporal.workflow.Def("HelloWorld", other)}, .execute = execute });
    const undeclared = harness.define(.{ .workflows = .{temporal.workflow.Def("Other", other)}, .execute = execute });
    const client_only = harness.define(.{ .execute = execute });

    const files = [_]harness.stored.File{
        .{ .path = "fixture/hello/0.0.0/1-HelloWorld.json", .bytes = @embedFile("testdata/1-HelloWorld.json") },
        .{ .path = "fixture/hello/0.0.0/runs.json", .bytes = @embedFile("testdata/runs.json") },
    };
};

const TestCheck = struct {
    state: std.heap.ArenaAllocator,
    problems: std.ArrayList([]const u8),

    const init: TestCheck = .{ .state = .init(std.testing.allocator), .problems = .empty };

    fn run(t: *TestCheck, input: Input) ![]const []const u8 {
        try check(std.testing.allocator, std.testing.io, t.state.allocator(), input, &t.problems);
        return t.problems.items;
    }

    fn deinit(t: *TestCheck) void {
        t.state.deinit();
    }
};

fn expectProblems(expected: []const []const u8, actual: []const []const u8) !void {
    errdefer for (actual) |message| std.debug.print("{s}\n", .{message});
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| try std.testing.expectEqualStrings(e, a);
}

test "conformance replay passes a fixture against the workflow that produced it" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{}, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.matching }},
        .exclusions = &.{},
        .version = "0.0.0",
    }));
}

test "conformance replay reports a workflow whose commands differ from its history as nondeterminism" {
    var t: TestCheck = .init;
    defer t.deinit();
    const problems = try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.changed }},
        .exclusions = &.{},
        .version = "0.0.0",
    });
    try std.testing.expectEqual(1, problems.len);
    try std.testing.expectStringStartsWith(problems[0], "conformance/histories/fixture/hello/0.0.0/1-HelloWorld.json: replay failed with error.Nondeterminism: ");
}

test "conformance replay names a feature with workflows and no history for the current version" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{
        "feature fixture/hello declares workflows but has no stored history for version 0.1.0; record it with: zig build conformance -Drecord-histories -Dfeature=fixture/hello",
    }, try t.run(.{
        .files = &Fixture.files,
        .features = &.{
            .{ .path = "fixture/hello", .feature = &Fixture.matching },
            .{ .path = "fixture/client", .feature = &Fixture.client_only },
        },
        .exclusions = &.{},
        .version = "0.1.0",
    }));
}

test "conformance replay exempts an excluded feature from the missing-history rule" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{}, try t.run(.{
        .files = &.{},
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.matching }},
        .exclusions = &.{.{ .feature = "fixture/hello", .bead = "temporal-sdk-zig-test", .reason = "example defect" }},
        .version = "0.0.0",
    }));
}

test "conformance replay names a stored file no feature owns" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{
        "conformance/histories/fixture/hello/0.0.0/1-HelloWorld.json: no feature under conformance/features owns this path",
        "conformance/histories/fixture/hello/0.0.0/runs.json: no feature under conformance/features owns this path",
    }, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/other", .feature = &Fixture.client_only }},
        .exclusions = &.{},
        .version = "0.0.0",
    }));
}

test "conformance replay names a stored workflow type its feature does not declare" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{
        "conformance/histories/fixture/hello/0.0.0/1-HelloWorld.json: workflow type \"HelloWorld\" is not in .workflows of feature fixture/hello",
    }, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.undeclared }},
        .exclusions = &.{},
        .version = "0.0.0",
    }));
}

test "conformance replay names an undeclared workflow type in an excluded feature's files" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{
        "conformance/histories/fixture/hello/0.0.0/1-HelloWorld.json: workflow type \"HelloWorld\" is not in .workflows of feature fixture/hello",
    }, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.undeclared }},
        .exclusions = &.{.{ .feature = "fixture/hello", .bead = "temporal-sdk-zig-test", .reason = "example defect" }},
        .version = "0.0.0",
    }));
}

test "conformance replay does not replay an excluded feature's histories" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{}, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.changed }},
        .exclusions = &.{.{ .feature = "fixture/hello", .bead = "temporal-sdk-zig-test", .reason = "example defect" }},
        .version = "0.0.0",
    }));
}

test "conformance replay rejects an exclusion of a feature that is not written" {
    var t: TestCheck = .init;
    defer t.deinit();
    try expectProblems(&.{
        "conformance/replay/exclusions.zon: fixture/gone is not a written feature",
    }, try t.run(.{
        .files = &Fixture.files,
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.matching }},
        .exclusions = &.{.{ .feature = "fixture/gone", .bead = "temporal-sdk-zig-test", .reason = "example defect" }},
        .version = "0.0.0",
    }));
}

test "conformance replay rejects an exclusion listed twice" {
    var t: TestCheck = .init;
    defer t.deinit();
    const exclusion: Exclusion = .{ .feature = "fixture/hello", .bead = "temporal-sdk-zig-test", .reason = "example defect" };
    try expectProblems(&.{
        "conformance/replay/exclusions.zon: fixture/hello is listed twice",
    }, try t.run(.{
        .files = &.{},
        .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.matching }},
        .exclusions = &.{ exclusion, exclusion },
        .version = "0.0.0",
    }));
}

test "conformance replay rejects an exclusion with an empty bead or reason, and it still exempts the feature" {
    for ([_]Exclusion{
        .{ .feature = "fixture/hello", .bead = "", .reason = "example defect" },
        .{ .feature = "fixture/hello", .bead = "temporal-sdk-zig-test", .reason = " " },
    }) |exclusion| {
        var t: TestCheck = .init;
        defer t.deinit();
        // An empty store and a changed workflow: neither a missing-history nor a replay message.
        try expectProblems(&.{
            "conformance/replay/exclusions.zon: fixture/hello has an empty bead or reason",
        }, try t.run(.{
            .files = &.{},
            .features = &.{.{ .path = "fixture/hello", .feature = &Fixture.changed }},
            .exclusions = &.{exclusion},
            .version = "0.0.0",
        }));
    }
}
