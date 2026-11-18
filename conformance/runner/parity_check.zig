const std = @import("std");
const parity_map = @import("parity_map.zig");
const parity_table = @import("parity_table.zig");
const report = @import("report.zig");
const Allocator = std.mem.Allocator;

pub const Finding = union(enum) {
    failing: struct { row: []const u8, feature: []const u8, status: report.Status },
    uncovered: struct { row: []const u8 },
    unknown_row: struct { row: []const u8, feature: []const u8 },
};
pub const Counts = struct { failing: usize = 0, uncovered: usize = 0, unknown_row: usize = 0 };
pub const Findings = struct { lines: []const Finding, counts: Counts };
pub const Verdict = union(enum) { refused: []const []const u8, findings: Findings };
pub const Sources = struct {
    report_path: []const u8,
    report_json: []const u8,
    map_path: []const u8,
    map_source: [:0]const u8,
    parity_source: []const u8,
};

/// Compares parsed inputs, refusing a filtered report or a mapping with file errors.
/// The returned verdict and its strings belong to the arena.
pub fn compare(arena: Allocator, result: report.Report, entries: []const parity_map.Entry, rows: []const parity_table.Row) error{OutOfMemory}!Verdict {
    if (try filteredReportProblem(arena, result)) |problem| return refuse(arena, problem);
    const problems = try mappingProblems(arena, result, entries);
    if (problems.len != 0) return .{ .refused = problems };
    return find(arena, result, entries, rows);
}

/// Parses the sources and returns the first stage's problems or a comparison.
/// The returned verdict and its strings belong to the arena.
pub fn checkSources(gpa: Allocator, arena: Allocator, sources: Sources) error{OutOfMemory}!Verdict {
    const result = switch (try report.parse(arena, sources.report_json)) {
        .report => |result| result,
        .problem => |problem| return refuse(arena, try std.fmt.allocPrint(arena, "cannot read report {s}: {s}", .{ sources.report_path, problem })),
    };
    if (try filteredReportProblem(arena, result)) |problem| return refuse(arena, problem);
    const entries = switch (try parity_map.parse(gpa, arena, sources.map_source, sources.map_path)) {
        .entries => |entries| entries,
        .problem => |problem| return refuse(arena, problem),
    };
    const problems = try mappingProblems(arena, result, entries);
    if (problems.len != 0) return .{ .refused = problems };
    const rows = switch (try parity_table.read(arena, sources.parity_source)) {
        .rows => |rows| rows,
        .problem => |problem| return refuse(arena, problem),
    };
    return find(arena, result, entries, rows);
}

fn refuse(arena: Allocator, problem: []const u8) error{OutOfMemory}!Verdict {
    return .{ .refused = try arena.dupe([]const u8, &.{problem}) };
}

fn filteredReportProblem(arena: Allocator, result: report.Report) error{OutOfMemory}!?[]const u8 {
    if (result.filter.len == 0) return null;
    const patterns = try std.mem.join(arena, ", ", result.filter);
    return try std.fmt.allocPrint(arena, "the report was made with -Dfeature ({s}); conformance-check compares a report of the whole suite", .{patterns});
}

fn mappingProblems(arena: Allocator, result: report.Report, entries: []const parity_map.Entry) error{OutOfMemory}![]const []const u8 {
    const suite = try arena.alloc([]const u8, result.features.len);
    var not_applicable: std.ArrayList([]const u8) = .empty;
    for (result.features, suite) |feature, *path| {
        path.* = feature.path;
        if (feature.status == .not_applicable) try not_applicable.append(arena, feature.path);
    }
    return parity_map.validate(arena, entries, suite, not_applicable.items);
}

fn find(arena: Allocator, result: report.Report, entries: []const parity_map.Entry, rows: []const parity_table.Row) error{OutOfMemory}!Verdict {
    var statuses: std.StringHashMapUnmanaged(report.Status) = .empty;
    for (result.features) |feature| try statuses.put(arena, feature.path, feature.status);
    const sorted = try arena.dupe(parity_map.Entry, entries);
    std.mem.sort(parity_map.Entry, sorted, {}, struct {
        fn lessThan(_: void, a: parity_map.Entry, b: parity_map.Entry) bool {
            return std.mem.lessThan(u8, a.feature, b.feature);
        }
    }.lessThan);

    var lines: std.ArrayList(Finding) = .empty;
    var counts: Counts = .{};
    for (rows) |row| {
        if (row.status != .supported) continue;
        var covered = false;
        for (sorted) |entry| {
            if (entry.parity != .row or !std.mem.eql(u8, entry.parity.row, row.feature)) continue;
            covered = true;
            const status = statuses.get(entry.feature).?;
            if (status == .pass) continue;
            try lines.append(arena, .{ .failing = .{
                .row = try arena.dupe(u8, row.feature),
                .feature = try arena.dupe(u8, entry.feature),
                .status = status,
            } });
            counts.failing += 1;
        }
        if (!covered) {
            try lines.append(arena, .{ .uncovered = .{ .row = try arena.dupe(u8, row.feature) } });
            counts.uncovered += 1;
        }
    }
    for (try parity_map.unknownRows(arena, entries, rows)) |entry| {
        try lines.append(arena, .{ .unknown_row = .{
            .row = try arena.dupe(u8, entry.parity.row),
            .feature = try arena.dupe(u8, entry.feature),
        } });
        counts.unknown_row += 1;
    }
    return .{ .findings = .{ .lines = lines.items, .counts = counts } };
}

pub fn write(findings: Findings, out: *std.Io.Writer) std.Io.Writer.Error!void {
    for (findings.lines) |line| switch (line) {
        .failing => |item| try out.print("failing\t{s}\t{s}\t{t}\n", .{ item.row, item.feature, item.status }),
        .uncovered => |item| try out.print("uncovered\t{s}\n", .{item.row}),
        .unknown_row => |item| try out.print("unknown-row\t{s}\t{s}\n", .{ item.row, item.feature }),
    };
    const counts = findings.counts;
    try out.print("counts\tfailing={d}\tuncovered={d}\tunknown-row={d}\n", .{ counts.failing, counts.uncovered, counts.unknown_row });
}

pub fn exitCode(verdict: Verdict) u8 {
    return switch (verdict) {
        .refused => 1,
        .findings => |findings| if (findings.counts.failing != 0 or findings.counts.unknown_row != 0) 1 else 0,
    };
}

const fixture_report: report.Report = .{
    .suite_commit = "commit",
    .sdk_version = "0.0.0",
    .filter = &.{},
    .readme_only_directories = 0,
    .features = &.{.{ .path = "a/one", .status = .pass }},
};
const fixture_map =
    \\.{ .{ .feature = "a/one", .parity = .{ .row = "One" } } }
;
const fixture_table =
    \\## Feature table
    \\| Feature | SDKs | Stage | Zig | Where |
    \\|---------|------|-------|-----|-------|
    \\| One | all | GA | supported | docs |
    \\
;

fn fixtureSources(arena: Allocator, result: report.Report, map_source: [:0]const u8, parity_source: []const u8) !Sources {
    var buffer: std.Io.Writer.Allocating = .init(arena);
    try report.write(result, &buffer.writer);
    return .{
        .report_path = "fixture.json",
        .report_json = buffer.written(),
        .map_path = "fixture.zon",
        .map_source = map_source,
        .parity_source = parity_source,
    };
}

fn expectOutput(verdict: Verdict, code: u8, expected: []const u8) !void {
    try std.testing.expect(verdict == .findings);
    try std.testing.expectEqual(code, exitCode(verdict));
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try write(verdict.findings, &output.writer);
    try std.testing.expectEqualStrings(expected, output.written());
}

fn expectRefusal(verdict: Verdict, expected: []const []const u8) !void {
    try std.testing.expect(verdict == .refused);
    try std.testing.expectEqual(@as(u8, 1), exitCode(verdict));
    try std.testing.expectEqualDeep(expected, verdict.refused);
}

test "conformance check orders TSV findings by table row then feature path" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var result = fixture_report;
    result.features = &.{
        .{ .path = "z/unknown", .status = .pass },
        .{ .path = "z/fail", .status = .fail, .message = "failed" },
        .{ .path = "z/pass", .status = .pass },
        .{ .path = "a/one", .status = .not_run },
        .{ .path = "a/unknown", .status = .unwritten },
        .{ .path = "a/fail", .status = .unwritten },
        .{ .path = "a/partial", .status = .unwritten },
        .{ .path = "a/missing", .status = .fail },
        .{ .path = "a/na-row", .status = .fail },
        .{ .path = "a/na-feature", .status = .not_applicable },
    };
    const sources = try fixtureSources(arena, result,
        \\.{
        \\    .{ .feature = "z/unknown", .parity = .{ .row = "Unknown A" } },
        \\    .{ .feature = "z/fail", .parity = .{ .row = "Z | Row" } },
        \\    .{ .feature = "z/pass", .parity = .{ .row = "Z | Row" } },
        \\    .{ .feature = "a/one", .parity = .{ .row = "One" } },
        \\    .{ .feature = "a/unknown", .parity = .{ .row = "Unknown Z" } },
        \\    .{ .feature = "a/fail", .parity = .{ .row = "Z | Row" } },
        \\    .{ .feature = "a/partial", .parity = .{ .row = "Partial" } },
        \\    .{ .feature = "a/missing", .parity = .{ .row = "Missing" } },
        \\    .{ .feature = "a/na-row", .parity = .{ .row = "Not applicable" } },
        \\    .{ .feature = "a/na-feature", .parity = .{ .none = "Out of scope." } },
        \\}
    ,
        \\## Feature table
        \\| Feature | SDKs | Stage | Zig | Where |
        \\|---------|------|-------|-----|-------|
        \\| Z \| Row | all | GA | supported | docs |
        \\| Uncovered | all | GA | supported | docs |
        \\| One | all | GA | supported | docs |
        \\| Partial | all | GA | partial | docs |
        \\| Missing | all | GA | missing | docs |
        \\| Not applicable | all | GA | not applicable | docs |
        \\| Unmapped missing | all | GA | missing | docs |
    );
    try expectOutput(try checkSources(gpa, arena, sources), 1, "failing\tZ | Row\ta/fail\tunwritten\n" ++
        "failing\tZ | Row\tz/fail\tfail\n" ++
        "uncovered\tUncovered\n" ++
        "failing\tOne\ta/one\tnot_run\n" ++
        "unknown-row\tUnknown Z\ta/unknown\n" ++
        "unknown-row\tUnknown A\tz/unknown\n" ++
        "counts\tfailing=3\tuncovered=1\tunknown-row=2\n");
}

test "conformance check exit codes distinguish failing, unknown, uncovered and passing rows" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var result = fixture_report;
    const passing = try fixtureSources(arena, result, fixture_map, fixture_table);
    try expectOutput(try checkSources(gpa, arena, passing), 0, "counts\tfailing=0\tuncovered=0\tunknown-row=0\n");

    const uncovered = try fixtureSources(arena, result, fixture_map, fixture_table ++ "| Two | all | GA | supported | docs |\n");
    try expectOutput(try checkSources(gpa, arena, uncovered), 0, "uncovered\tTwo\ncounts\tfailing=0\tuncovered=1\tunknown-row=0\n");

    result.features = &.{.{ .path = "a/one", .status = .fail }};
    const failing = try fixtureSources(arena, result, fixture_map, fixture_table);
    try expectOutput(try checkSources(gpa, arena, failing), 1, "failing\tOne\ta/one\tfail\ncounts\tfailing=1\tuncovered=0\tunknown-row=0\n");

    result.features = &.{ .{ .path = "a/one", .status = .pass }, .{ .path = "a/two", .status = .pass } };
    const unknown_only = try fixtureSources(arena, result,
        \\.{
        \\    .{ .feature = "a/one", .parity = .{ .row = "One" } },
        \\    .{ .feature = "a/two", .parity = .{ .row = "Typo" } },
        \\}
    , fixture_table);
    try expectOutput(try checkSources(gpa, arena, unknown_only), 1, "unknown-row\tTypo\ta/two\ncounts\tfailing=0\tuncovered=0\tunknown-row=1\n");
}

test "conformance check refuses invalid mappings before reading the table or making findings" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var result = fixture_report;
    result.features = &.{
        .{ .path = "a/one", .status = .unwritten },
        .{ .path = "a/out-of-scope", .status = .not_applicable },
        .{ .path = "a/omitted", .status = .pass },
    };
    const sources = try fixtureSources(arena, result,
        \\.{
        \\    .{ .feature = "a/one", .parity = .{ .row = "One" } },
        \\    .{ .feature = "a/one", .parity = .{ .row = "Unknown" } },
        \\    .{ .feature = "a/extra", .parity = .{ .none = " \t\r\n" } },
        \\    .{ .feature = "a/out-of-scope", .parity = .{ .row = "One" } },
        \\}
    , "invalid table");
    const expected: []const []const u8 = &.{
        "parity_map.zon has two entries for a/one",
        "parity_map.zon has an entry for a/extra, which is absent from the suite",
        "parity_map.zon maps a/extra to none with an empty reason",
        "parity_map.zon maps a/out-of-scope to the row \"One\", but not_applicable.zon lists it: map it to none",
        "parity_map.zon has no entry for a/omitted",
    };
    try expectRefusal(try checkSources(gpa, arena, sources), expected);
    const parsed = try parity_map.parse(gpa, arena, sources.map_source, sources.map_path);
    const table = try parity_table.read(arena, fixture_table);
    try expectRefusal(try compare(arena, result, parsed.entries, table.rows), expected);
}

test "conformance check returns only the first unreadable or filtered input stage" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    var result = fixture_report;
    result.filter = &.{ "a/*", "b/foo" };
    var sources = try fixtureSources(arena, result, ".{", "invalid table");
    const filtered: []const []const u8 = &.{"the report was made with -Dfeature (a/*, b/foo); conformance-check compares a report of the whole suite"};
    try expectRefusal(try checkSources(gpa, arena, sources), filtered);
    try expectRefusal(try compare(arena, result, &.{}, &.{}), filtered);

    sources.report_json = "{";
    try expectRefusal(try checkSources(gpa, arena, sources), &.{"cannot read report fixture.json: UnexpectedEndOfInput"});

    sources = try fixtureSources(arena, fixture_report, ".{", "invalid table");
    const map_verdict = try checkSources(gpa, arena, sources);
    try std.testing.expect(map_verdict == .refused);
    try std.testing.expectEqual(@as(u8, 1), exitCode(map_verdict));
    try std.testing.expectEqual(@as(usize, 1), map_verdict.refused.len);
    try std.testing.expect(std.mem.startsWith(u8, map_verdict.refused[0], "fixture.zon:1:"));

    sources.map_source = fixture_map;
    try expectRefusal(try checkSources(gpa, arena, sources), &.{"PARITY.md:1: no rows under \"## Feature table\""});
}
