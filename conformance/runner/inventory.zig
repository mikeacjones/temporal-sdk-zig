const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const feature_file_extensions = [_][]const u8{ "go", "py", "ts", "java", "cs", "php", "rb", "rs" };

pub const Inventory = struct {
    features: []const []const u8,
    readme_only_directories: usize,
};

pub fn scan(arena: Allocator, io: Io, features_dir: Io.Dir) !Inventory {
    var features: std.ArrayList([]const u8) = .empty;
    var readme_only: usize = 0;
    try scanDirectory(arena, io, features_dir, "", &features, &readme_only);
    std.mem.sort([]const u8, features.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    return .{ .features = features.items, .readme_only_directories = readme_only };
}

fn scanDirectory(
    arena: Allocator,
    io: Io,
    dir: Io.Dir,
    path: []const u8,
    features: *std.ArrayList([]const u8),
    readme_only: *usize,
) !void {
    var entries: usize = 0;
    var has_readme = false;
    var has_feature = false;
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        entries += 1;
        switch (entry.kind) {
            .directory => {
                const subpath = if (path.len == 0)
                    try arena.dupe(u8, entry.name)
                else
                    try std.fmt.allocPrint(arena, "{s}/{s}", .{ path, entry.name });
                const subdir = try dir.openDir(io, entry.name, .{ .iterate = true });
                defer subdir.close(io);
                try scanDirectory(arena, io, subdir, subpath, features, readme_only);
            },
            .file => {
                if (std.mem.eql(u8, entry.name, "README.md")) has_readme = true;
                inline for (feature_file_extensions) |ext| {
                    if (std.mem.eql(u8, entry.name, "feature." ++ ext)) has_feature = true;
                }
            },
            else => {},
        }
    }
    if (path.len == 0) return;
    if (has_feature) try features.append(arena, path);
    if (has_readme and entries == 1) readme_only.* += 1;
}

pub const NotApplicable = struct {
    feature: []const u8,
    reason: []const u8,
};

pub fn parseNotApplicable(gpa: Allocator, arena: Allocator, source: [:0]const u8, path: []const u8) ?[]const NotApplicable {
    var diag: std.zon.parse.Diagnostics = undefined;
    return std.zon.parse.fromSlice([]const NotApplicable, .{
        .gpa = gpa,
        .arena = arena,
        .source = source,
        .diagnostics = &diag,
    }) catch {
        diag.log(path);
        return null;
    };
}

pub fn check(
    arena: Allocator,
    inventory: Inventory,
    not_applicable: []const NotApplicable,
    written: []const []const u8,
    patterns: []const []const u8,
) error{OutOfMemory}!?[]const u8 {
    for (written) |path| {
        if (!contains(inventory.features, path))
            return try std.fmt.allocPrint(arena, "conformance/features/{s}/feature.zig has no feature in the suite", .{path});
    }
    for (not_applicable, 0..) |item, i| {
        if (std.mem.trim(u8, item.reason, " \t\r\n").len == 0)
            return try std.fmt.allocPrint(arena, "not_applicable.zon lists {s} with an empty reason", .{item.feature});
        if (!std.unicode.utf8ValidateSlice(item.reason))
            return try std.fmt.allocPrint(arena, "not_applicable.zon lists {s} with a reason that is not valid UTF-8", .{item.feature});
        if (!contains(inventory.features, item.feature))
            return try std.fmt.allocPrint(arena, "not_applicable.zon lists {s}, which is absent from the suite", .{item.feature});
        if (contains(written, item.feature))
            return try std.fmt.allocPrint(arena, "not_applicable.zon lists {s}, which has a Zig implementation", .{item.feature});
        for (not_applicable[0..i]) |previous| {
            if (std.mem.eql(u8, previous.feature, item.feature))
                return try std.fmt.allocPrint(arena, "not_applicable.zon lists {s} twice", .{item.feature});
        }
    }
    for (patterns) |pattern| {
        for (inventory.features) |path| {
            if (matches(pattern, path)) break;
        } else return try std.fmt.allocPrint(arena, "-Dfeature={s} matches no feature in the suite", .{pattern});
    }
    return null;
}

fn contains(paths: []const []const u8, path: []const u8) bool {
    for (paths) |candidate| if (std.mem.eql(u8, candidate, path)) return true;
    return false;
}

pub fn selected(patterns: []const []const u8, path: []const u8) bool {
    for (patterns) |pattern| if (matches(pattern, path)) return true;
    return patterns.len == 0;
}

/// A star can absorb additional bytes until the rest of the pattern matches.
pub fn matches(pattern: []const u8, path: []const u8) bool {
    var i: usize = 0;
    var j: usize = 0;
    var star: ?usize = null;
    var star_end: usize = 0;
    while (j < path.len) {
        if (i < pattern.len and pattern[i] == '*') {
            star = i;
            star_end = j;
            i += 1;
        } else if (i < pattern.len and pattern[i] == path[j]) {
            i += 1;
            j += 1;
        } else if (star) |s| {
            star_end += 1;
            j = star_end;
            i = s + 1;
        } else return false;
    }
    while (i < pattern.len and pattern[i] == '*') : (i += 1) {}
    return i == pattern.len;
}

test "conformance inventory finds exact feature files and counts only README-only directories" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const files = [_][]const u8{
        "features.go",                      "z/feature.rs",                        "z/feature.ts",
        "a/README.md",                      "a/implemented/feature.py",            "a/empty/README.md",
        "a/mixed/README.md",                "a/mixed/file.txt",                    "a/old/feature_pre1.go",
        "bugs/go/race/feature.go",          "helpers/util.go",                     "snippets/demo.ts",
        "directory/feature.java/README.md", "directory/suffix/feature.javascript",
    };
    for (files) |path| {
        if (std.fs.path.dirname(path)) |dir| try tmp.dir.createDirPath(io, dir);
        try tmp.dir.writeFile(io, .{ .sub_path = path, .data = "" });
    }
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const result = try scan(arena.allocator(), io, tmp.dir);
    try std.testing.expectEqual(@as(usize, 2), result.readme_only_directories);
    try std.testing.expectEqualDeep(&[_][]const u8{ "a/implemented", "bugs/go/race", "z" }, result.features);
}

test "conformance patterns match whole paths with only star as a wildcard" {
    try std.testing.expect(matches("bugs/*race", "bugs/go/activity_start_race"));
    try std.testing.expect(matches("*/*/race**", "bugs/go/race"));
    try std.testing.expect(matches("a*b*bc", "abbbc"));
    try std.testing.expect(matches("a/*b", "a/b"));
    try std.testing.expect(matches("a/?[b]", "a/?[b]"));
    try std.testing.expect(!matches("a/?[b]", "a/ab"));
    try std.testing.expect(!matches("race", "bugs/go/race"));
    try std.testing.expect(!matches("bugs/*race", "bugs/go/race/other"));
    try std.testing.expect(!matches("", "bugs/go/race"));
}

test "conformance inventory validates written paths, reasons, duplicates and filters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const suite: Inventory = .{ .features = &.{ "a/one", "a/two" }, .readme_only_directories = 0 };
    const na = parseNotApplicable(std.testing.allocator, a,
        \\.{ .{ .feature = "a/two", .reason = "example" } }
    , "not_applicable.zon").?;
    try std.testing.expectEqualStrings("example", na[0].reason);
    try std.testing.expectEqual(null, try check(a, suite, na, &.{"a/one"}, &.{ "a/two", "a/*" }));
    try std.testing.expectEqualStrings("conformance/features/a/missing/feature.zig has no feature in the suite", (try check(a, suite, na, &.{"a/missing"}, &.{})).?);
    try std.testing.expectEqualStrings("not_applicable.zon lists a/two, which has a Zig implementation", (try check(a, suite, na, &.{"a/two"}, &.{})).?);
    try std.testing.expectEqualStrings("not_applicable.zon lists a/two twice", (try check(a, suite, &.{ na[0], na[0] }, &.{}, &.{})).?);
    try std.testing.expectEqualStrings("not_applicable.zon lists a/one with an empty reason", (try check(a, suite, &.{.{ .feature = "a/one", .reason = " " }}, &.{}, &.{})).?);
    try std.testing.expectEqualStrings("not_applicable.zon lists a/one with a reason that is not valid UTF-8", (try check(a, suite, &.{.{ .feature = "a/one", .reason = "\xff" }}, &.{}, &.{})).?);
    try std.testing.expectEqualStrings("not_applicable.zon lists a/three, which is absent from the suite", (try check(a, suite, &.{.{ .feature = "a/three", .reason = "example" }}, &.{}, &.{})).?);
    try std.testing.expectEqualStrings("-Dfeature=a/nope* matches no feature in the suite", (try check(a, suite, na, &.{}, &.{ "a/*", "a/nope*" })).?);
}
