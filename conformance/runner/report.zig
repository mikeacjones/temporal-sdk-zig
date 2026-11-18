const std = @import("std");
const inventory = @import("inventory.zig");
const Allocator = std.mem.Allocator;

pub const Status = enum { pass, fail, unwritten, not_applicable, not_run };
pub const Outcome = union(enum) { pass, fail: []const u8 };
pub const Entry = struct { path: []const u8, status: Status, message: ?[]const u8 = null };
pub const Report = struct {
    suite_commit: []const u8,
    sdk_version: []const u8,
    filter: []const []const u8,
    readme_only_directories: usize,
    features: []const Entry,
};

/// Outcomes are indexed by written path. Unselected features have no outcome.
pub fn assemble(
    arena: Allocator,
    suite_commit: []const u8,
    sdk_version: []const u8,
    suite: inventory.Inventory,
    not_applicable: []const inventory.NotApplicable,
    written: []const []const u8,
    patterns: []const []const u8,
    outcomes: []const ?Outcome,
) error{OutOfMemory}!Report {
    const entries = try arena.alloc(Entry, suite.features.len);
    for (suite.features, entries) |path, *entry| {
        entry.* = .{ .path = path, .status = .unwritten };
        for (not_applicable) |item| {
            if (std.mem.eql(u8, path, item.feature)) {
                entry.status = .not_applicable;
                entry.message = item.reason;
                break;
            }
        } else for (written, outcomes) |candidate, outcome| {
            if (!std.mem.eql(u8, path, candidate)) continue;
            if (inventory.selected(patterns, path)) {
                switch (outcome orelse Outcome{ .fail = "feature produced no outcome" }) {
                    .pass => entry.status = .pass,
                    .fail => |msg| {
                        entry.status = .fail;
                        entry.message = if (msg.len != 0) msg else "feature failed without a message";
                    },
                }
            } else {
                entry.status = .not_run;
            }
            break;
        }
    }
    return .{
        .suite_commit = suite_commit,
        .sdk_version = sdk_version,
        .filter = patterns,
        .readme_only_directories = suite.readme_only_directories,
        .features = entries,
    };
}

pub fn write(report: Report, writer: *std.Io.Writer) std.Io.Writer.Error!void {
    try std.json.Stringify.value(report, .{ .whitespace = .indent_2, .emit_null_optional_fields = false }, writer);
    try writer.writeByte('\n');
}

test "conformance report preserves inventory order and writes optional messages only where required" {
    const gpa = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    const suite: inventory.Inventory = .{ .features = &.{ "a/fail", "a/missing", "a/pass", "a/skipped" }, .readme_only_directories = 2 };
    const na: []const inventory.NotApplicable = &.{.{ .feature = "a/skipped", .reason = "out of scope" }};
    const written: []const []const u8 = &.{ "a/pass", "a/fail" };
    const outcomes: []const ?Outcome = &.{ .pass, .{ .fail = "bad \"result\"" } };
    const result = try assemble(a, "commit", "0.0.0", suite, na, written, &.{}, outcomes);
    var buffer: std.Io.Writer.Allocating = .init(gpa);
    defer buffer.deinit();
    try write(result, &buffer.writer);
    try std.testing.expectEqualStrings(
        \\{
        \\  "suite_commit": "commit",
        \\  "sdk_version": "0.0.0",
        \\  "filter": [],
        \\  "readme_only_directories": 2,
        \\  "features": [
        \\    {
        \\      "path": "a/fail",
        \\      "status": "fail",
        \\      "message": "bad \"result\""
        \\    },
        \\    {
        \\      "path": "a/missing",
        \\      "status": "unwritten"
        \\    },
        \\    {
        \\      "path": "a/pass",
        \\      "status": "pass"
        \\    },
        \\    {
        \\      "path": "a/skipped",
        \\      "status": "not_applicable",
        \\      "message": "out of scope"
        \\    }
        \\  ]
        \\}
        \\
    , buffer.written());

    const filtered = try assemble(a, "commit", "0.0.0", suite, na, written, &.{"a/p*"}, &.{ .pass, null });
    try std.testing.expectEqualDeep(&[_][]const u8{"a/p*"}, filtered.filter);
    try std.testing.expectEqualDeep(&[_]Entry{
        .{ .path = "a/fail", .status = .not_run },
        .{ .path = "a/missing", .status = .unwritten },
        .{ .path = "a/pass", .status = .pass },
        .{ .path = "a/skipped", .status = .not_applicable, .message = "out of scope" },
    }, filtered.features);
}
