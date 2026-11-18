const std = @import("std");
const parity_table = @import("parity_table.zig");
const Allocator = std.mem.Allocator;

pub const Target = union(enum) { row: []const u8, none: []const u8 };
pub const Entry = struct { feature: []const u8, parity: Target };
pub const Parsed = union(enum) { entries: []const Entry, problem: []const u8 };
pub const Summary = struct { entries: usize, mapped: usize, rows: usize, none: usize };

/// Parses the ZON into the arena. The path only prefixes diagnostic messages.
pub fn parse(gpa: Allocator, arena: Allocator, source: [:0]const u8, path: []const u8) error{OutOfMemory}!Parsed {
    var diagnostics: std.zon.parse.Diagnostics = undefined;
    const entries = std.zon.parse.fromSlice([]const Entry, .{
        .gpa = gpa,
        .arena = arena,
        .source = source,
        .diagnostics = &diagnostics,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ParseZon => return .{ .problem = try std.fmt.allocPrint(arena, "{f}", .{diagnostics.fmt(path)}) },
    };
    for (entries) |entry| {
        if (hasControl(entry.feature) or (entry.parity == .row and hasControl(entry.parity.row)))
            return .{ .problem = try std.fmt.allocPrint(arena, "{s}: the entry for {s} holds a control character", .{ path, entry.feature }) };
    }
    return .{ .entries = entries };
}

fn hasControl(text: []const u8) bool {
    for (text) |byte| if (std.ascii.isControl(byte)) return true;
    return false;
}

/// Returns a message for each file error in entry order, then for each omitted suite path.
/// The messages and their slice belong to the arena.
pub fn validate(arena: Allocator, entries: []const Entry, suite: []const []const u8, not_applicable: []const []const u8) error{OutOfMemory}![]const []const u8 {
    var problems: std.ArrayList([]const u8) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    for (entries) |entry| {
        if ((try seen.getOrPut(arena, entry.feature)).found_existing)
            try problems.append(arena, try std.fmt.allocPrint(arena, "parity_map.zon has two entries for {s}", .{entry.feature}));
        if (!contains(suite, entry.feature))
            try problems.append(arena, try std.fmt.allocPrint(arena, "parity_map.zon has an entry for {s}, which is absent from the suite", .{entry.feature}));
        switch (entry.parity) {
            .row => |row| if (contains(not_applicable, entry.feature)) {
                try problems.append(arena, try std.fmt.allocPrint(arena, "parity_map.zon maps {s} to the row \"{s}\", but not_applicable.zon lists it: map it to none", .{ entry.feature, row }));
            },
            .none => |reason| if (std.mem.trim(u8, reason, " \t\r\n").len == 0) {
                try problems.append(arena, try std.fmt.allocPrint(arena, "parity_map.zon maps {s} to none with an empty reason", .{entry.feature}));
            },
        }
    }
    for (suite) |path| {
        if (!seen.contains(path))
            try problems.append(arena, try std.fmt.allocPrint(arena, "parity_map.zon has no entry for {s}", .{path}));
    }
    return problems.items;
}

fn contains(paths: []const []const u8, path: []const u8) bool {
    for (paths) |candidate| if (std.mem.eql(u8, candidate, path)) return true;
    return false;
}

/// Returns entries whose row text is absent, sorted by feature path in byte order.
/// The entries borrow their strings from the input.
pub fn unknownRows(arena: Allocator, entries: []const Entry, rows: []const parity_table.Row) error{OutOfMemory}![]const Entry {
    var unknown: std.ArrayList(Entry) = .empty;
    for (entries) |entry| {
        const text = switch (entry.parity) {
            .row => |value| value,
            .none => continue,
        };
        for (rows) |row| {
            if (std.mem.eql(u8, text, row.feature)) break;
        } else try unknown.append(arena, entry);
    }
    std.mem.sort(Entry, unknown.items, {}, struct {
        fn lessThan(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.feature, b.feature);
        }
    }.lessThan);
    return unknown.items;
}

/// Counts entries and distinct row texts for the map summary line.
pub fn summarize(arena: Allocator, entries: []const Entry) error{OutOfMemory}!Summary {
    var distinct: std.StringHashMapUnmanaged(void) = .empty;
    var mapped: usize = 0;
    for (entries) |entry| switch (entry.parity) {
        .row => |row| {
            try distinct.put(arena, row, {});
            mapped += 1;
        },
        .none => {},
    };
    return .{ .entries = entries.len, .mapped = mapped, .rows = distinct.count(), .none = entries.len - mapped };
}

const unknown_row_message = "conformance/parity_map.zon maps {s} to the row \"{s}\", which PARITY.md's feature table lacks";

test "conformance parity map names only rows PARITY.md's feature table has" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    const entries = switch (try parse(gpa, arena, @embedFile("parity_map.zon"), "conformance/parity_map.zon")) {
        .entries => |entries| entries,
        .problem => |message| {
            std.debug.print("{s}\n", .{message});
            return error.InvalidParityMap;
        },
    };
    const rows = switch (try parity_table.read(arena, @embedFile("PARITY.md"))) {
        .rows => |rows| rows,
        .problem => |message| {
            std.debug.print("{s}\n", .{message});
            return error.InvalidParityTable;
        },
    };
    const unknown = try unknownRows(arena, entries, rows);
    for (unknown) |entry| std.debug.print(unknown_row_message ++ "\n", .{ entry.feature, entry.parity.row });
    if (unknown.len != 0) return error.UnknownParityRow;
}

test "conformance parity map parses targets and counts distinct rows" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    const parsed = try parse(gpa, arena,
        \\.{
        \\    .{ .feature = "a/one", .parity = .{ .row = "Name | Detail" } },
        \\    .{ .feature = "a/two", .parity = .{ .row = "Name | Detail" } },
        \\    .{ .feature = "bugs/go/three", .parity = .{ .none = "Language-specific regression." } },
        \\}
    , "map.zon");
    const entries = parsed.entries;
    try std.testing.expectEqualDeep(@as([]const Entry, &.{
        .{ .feature = "a/one", .parity = .{ .row = "Name | Detail" } },
        .{ .feature = "a/two", .parity = .{ .row = "Name | Detail" } },
        .{ .feature = "bugs/go/three", .parity = .{ .none = "Language-specific regression." } },
    }), entries);
    try std.testing.expectEqual(Summary{ .entries = 3, .mapped = 2, .rows = 1, .none = 1 }, try summarize(arena, entries));
    const problems = try validate(arena, entries, &.{ "a/one", "a/two", "bugs/go/three" }, &.{"bugs/go/three"});
    try std.testing.expectEqual(@as(usize, 0), problems.len);
}

test "conformance parity map returns ZON syntax and shape diagnostics" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    for ([_][:0]const u8{
        ".{",
        \\.{ .{ .feature = "a/one", .parity = .{ .row = "Row", .none = "Reason." } } }
        ,
        \\.{ .{ .feature = "a/one" } }
        ,
    }) |source| {
        const parsed = try parse(gpa, arena, source, "fixture.zon");
        try std.testing.expect(std.mem.startsWith(u8, parsed.problem, "fixture.zon:1:"));
        try std.testing.expect(std.mem.find(u8, parsed.problem, ": error: ") != null);
    }
}

test "conformance parity map rejects control characters in features and row names" {
    const gpa = std.testing.allocator;
    var state: std.heap.ArenaAllocator = .init(gpa);
    defer state.deinit();
    const arena = state.allocator();
    const bad_feature = try parse(gpa, arena,
        \\.{ .{ .feature = "a/bad\tfeature", .parity = .{ .none = "Some reason." } } }
    , "fixture.zon");
    try std.testing.expectEqualStrings("fixture.zon: the entry for a/bad\tfeature holds a control character", bad_feature.problem);
    const bad_row = try parse(gpa, arena,
        \\.{ .{ .feature = "a/one", .parity = .{ .row = "Bad\x7frow" } } }
    , "fixture.zon");
    try std.testing.expectEqualStrings("fixture.zon: the entry for a/one holds a control character", bad_row.problem);
}

fn expectProblems(entries: []const Entry, suite: []const []const u8, not_applicable: []const []const u8, expected: []const []const u8) !void {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    try std.testing.expectEqualDeep(expected, try validate(state.allocator(), entries, suite, not_applicable));
}

const fixture_entry: Entry = .{ .feature = "a/one", .parity = .{ .row = "One" } };

test "conformance parity map reports a missing suite feature" {
    try expectProblems(&.{fixture_entry}, &.{ "a/one", "a/two" }, &.{}, &.{
        "parity_map.zon has no entry for a/two",
    });
}

test "conformance parity map reports an entry absent from the suite" {
    try expectProblems(&.{fixture_entry}, &.{}, &.{}, &.{
        "parity_map.zon has an entry for a/one, which is absent from the suite",
    });
}

test "conformance parity map reports a duplicate entry" {
    try expectProblems(&.{ fixture_entry, fixture_entry, fixture_entry }, &.{"a/one"}, &.{}, &.{
        "parity_map.zon has two entries for a/one",
        "parity_map.zon has two entries for a/one",
    });
}

test "conformance parity map reports a row assigned to a not-applicable feature" {
    try expectProblems(&.{fixture_entry}, &.{"a/one"}, &.{"a/one"}, &.{
        "parity_map.zon maps a/one to the row \"One\", but not_applicable.zon lists it: map it to none",
    });
}

test "conformance parity map reports none with an empty reason" {
    const entries: []const Entry = &.{.{ .feature = "a/one", .parity = .{ .none = " \t\r\n" } }};
    try expectProblems(entries, &.{"a/one"}, &.{}, &.{
        "parity_map.zon maps a/one to none with an empty reason",
    });
}

test "conformance parity map accumulates every problem in entry and suite order" {
    const entries: []const Entry = &.{
        fixture_entry,
        .{ .feature = "a/one", .parity = .{ .none = "" } },
    };
    try expectProblems(entries, &.{ "a/z", "a/b" }, &.{"a/one"}, &.{
        "parity_map.zon has an entry for a/one, which is absent from the suite",
        "parity_map.zon maps a/one to the row \"One\", but not_applicable.zon lists it: map it to none",
        "parity_map.zon has two entries for a/one",
        "parity_map.zon has an entry for a/one, which is absent from the suite",
        "parity_map.zon maps a/one to none with an empty reason",
        "parity_map.zon has no entry for a/z",
        "parity_map.zon has no entry for a/b",
    });
}

test "conformance parity map identifies unknown rows by feature with the missing row text" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const table = try parity_table.read(arena,
        \\## Feature table
        \\| Feature | SDKs | Stage | Zig | Where |
        \\|---------|------|-------|-----|-------|
        \\| A \| B | all | GA | supported | docs |
        \\## Features only one SDK offers
        \\| Feature | SDKs | Stage | Zig | Where |
        \\|---------|------|-------|-----|-------|
        \\| Two | all | GA | supported | docs |
    );
    const entries: []const Entry = &.{
        .{ .feature = "z/two", .parity = .{ .row = "Two" } },
        .{ .feature = "a/known", .parity = .{ .row = "A | B" } },
        .{ .feature = "a/none", .parity = .{ .none = "Some reason." } },
        fixture_entry,
    };
    const unknown = try unknownRows(arena, entries, table.rows);
    try std.testing.expectEqualDeep(@as([]const Entry, &.{ fixture_entry, entries[0] }), unknown);
    const message = try std.fmt.allocPrint(arena, unknown_row_message, .{ unknown[0].feature, unknown[0].parity.row });
    try std.testing.expectEqualStrings("conformance/parity_map.zon maps a/one to the row \"One\", which PARITY.md's feature table lacks", message);
}
