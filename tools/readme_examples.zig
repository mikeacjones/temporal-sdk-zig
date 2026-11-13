//! Keeps README.md's Zig code blocks equal to the example regions they copy.
//!
//!     readme_examples check|update <root dir> <README path> <example file>...
//!
//! Paths after the root are relative to it. An example file marks a region with a line
//! `// example: <id>` and ends it with `// end-example`. In README.md, a line
//! `<!-- example: <id> -->` followed by a zig code block marks a block that must equal that
//! region, with the region's common indentation removed. A zig code block is any fenced block
//! whose info string starts with `zig`, in any case: a fence of three or more backticks or
//! tildes, indented or inside a block quote, closed by a fence of the same character at least
//! as long. `check` reports every block that differs, every zig block with no marker, every
//! marker that names no region, every region no block uses, and every id defined twice, and
//! exits 1 if there are any. `update` rewrites the blocks from the regions and still fails on
//! the other problems.
//!
//! The marker `<!-- example: unchecked -->` exempts a block, at most `max_unchecked` times:
//! for the snippets of a user's build files, which can't be compiled here.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const max_unchecked = 2;
const unchecked_id = "unchecked";
const region_start = "// example: ";
const region_end = "// end-example";
const marker_start = "<!-- example: ";
const marker_end = " -->";
const zig_info = "zig";

/// The opening line of a fenced code block.
const Fence = struct {
    /// What precedes the fence characters: indentation and block quote markers. Body lines
    /// carry it too, and it is removed before they are compared.
    prefix: []const u8,
    char: u8,
    len: usize,
    info: []const u8,

    /// Reads `line` as an opening fence: a run of at least three backticks or tildes after
    /// any indentation and block quote markers, then an info string, which after backticks
    /// may not contain a backtick.
    fn parse(line: []const u8) ?Fence {
        const l = std.mem.trimEnd(u8, line, " \t\r");
        const start = std.mem.findNone(u8, l, " \t>") orelse return null;
        const c = l[start];
        if (c != '`' and c != '~') return null;
        const end = std.mem.findNonePos(u8, l, start, &.{c}) orelse l.len;
        if (end - start < 3) return null;
        const info = std.mem.trim(u8, l[end..], " \t");
        if (c == '`' and std.mem.findScalar(u8, info, '`') != null) return null;
        return .{ .prefix = l[0..start], .char = c, .len = end - start, .info = info };
    }

    fn isZig(f: Fence) bool {
        return f.info.len >= zig_info.len and std.ascii.eqlIgnoreCase(f.info[0..zig_info.len], zig_info);
    }

    /// Whether `line` closes the block: only the fence's character, at least as many, after
    /// indentation (and block quote markers, if the fence is quoted).
    fn closedBy(f: Fence, line: []const u8) bool {
        const skip: []const u8 = if (std.mem.findScalar(u8, f.prefix, '>') != null) " \t>" else " \t";
        const rest = std.mem.trim(u8, std.mem.trimStart(u8, line, skip), " \t\r");
        return rest.len >= f.len and std.mem.findNone(u8, rest, &.{f.char}) == null;
    }

    /// `line` without the part of the fence's prefix it starts with.
    fn strip(f: Fence, line: []const u8) []const u8 {
        var k: usize = 0;
        while (k < f.prefix.len and k < line.len and line[k] == f.prefix[k]) k += 1;
        return line[k..];
    }
};

pub const Region = struct {
    id: []const u8,
    file: []const u8,
    /// The line of the `// example:` marker, from 1.
    line: usize,
    /// The region's lines with their common indentation removed, joined by newlines, without
    /// trailing blank lines.
    text: []const u8,
    used: bool = false,
};

pub const Mode = enum { check, update };

/// Problems, as messages ready to print, in the order found.
pub const Problems = std.ArrayList([]const u8);

/// Adds the regions of `source` (the contents of `file`) to `regions`.
pub fn collectRegions(arena: Allocator, file: []const u8, source: []const u8, regions: *std.ArrayList(Region), problems: *Problems) Allocator.Error!void {
    var open: ?struct { id: []const u8, line: usize, body: std.ArrayList([]const u8) } = null;
    var it = std.mem.splitScalar(u8, source, '\n');
    var line_no: usize = 0;
    while (it.next()) |line| {
        line_no += 1;
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (std.mem.startsWith(u8, trimmed, region_start)) {
            const id = std.mem.trim(u8, trimmed[region_start.len..], " \t");
            if (open) |o| {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" starts inside example \"{s}\" (line {d}); end it with {s} first", .{ file, line_no, id, o.id, o.line, region_end }));
                continue;
            }
            if (id.len == 0) {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example marker has no id", .{ file, line_no }));
                continue;
            }
            open = .{ .id = id, .line = line_no, .body = .empty };
        } else if (std.mem.eql(u8, trimmed, region_end)) {
            const o = open orelse {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: {s} with no example open", .{ file, line_no, region_end }));
                continue;
            };
            open = null;
            if (findRegion(regions.items, o.id)) |r| {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" is already defined at {s}:{d}", .{ file, o.line, o.id, r.file, r.line }));
            } else try regions.append(arena, .{ .id = o.id, .file = file, .line = o.line, .text = try dedent(arena, o.body.items) });
        } else if (open) |*o| {
            try o.body.append(arena, std.mem.trimEnd(u8, line, "\r"));
        }
    }
    if (open) |o| try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" has no {s}", .{ file, o.line, o.id, region_end }));
}

/// `lines` without their common leading whitespace or trailing blank lines, joined by
/// newlines. Blank lines don't count toward the indentation and come out empty.
fn dedent(arena: Allocator, lines: []const []const u8) Allocator.Error![]const u8 {
    var end = lines.len;
    while (end > 0 and isBlank(lines[end - 1])) end -= 1;
    var indent: usize = std.math.maxInt(usize);
    for (lines[0..end]) |line| {
        if (isBlank(line)) continue;
        indent = @min(indent, line.len - std.mem.trimStart(u8, line, " \t").len);
    }
    var out: std.ArrayList(u8) = .empty;
    for (lines[0..end], 0..) |line, i| {
        if (i > 0) try out.append(arena, '\n');
        if (!isBlank(line)) try out.appendSlice(arena, std.mem.trimEnd(u8, line[indent..], " \t"));
    }
    return out.items;
}

fn isBlank(line: []const u8) bool {
    return std.mem.trim(u8, line, " \t\r").len == 0;
}

/// Walks `readme` (named `readme_name` in messages) against `regions`, and returns it with
/// every marked block replaced by its region's text. In `check` mode, a block that differs is
/// a problem; in `update` mode it is rewritten. Regions no block used are problems either way.
pub fn process(arena: Allocator, mode: Mode, readme_name: []const u8, readme: []const u8, regions: []Region, problems: *Problems) Allocator.Error![]const u8 {
    var lines: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, readme, '\n');
    while (it.next()) |line| try lines.append(arena, line);

    var out: std.ArrayList(u8) = .empty;
    var unchecked: usize = 0;
    var i: usize = 0;
    while (i < lines.items.len) {
        const line = lines.items[i];
        const trimmed = std.mem.trimEnd(u8, line, " \t\r");
        if (markerId(trimmed)) |id| {
            const marker_line = i + 1;
            try appendLine(arena, &out, line);
            i += 1;
            while (i < lines.items.len and isBlank(lines.items[i])) : (i += 1) try appendLine(arena, &out, lines.items[i]);
            const open = if (i < lines.items.len) Fence.parse(lines.items[i]) else null;
            if (open == null or !open.?.isZig()) {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example marker \"{s}\" is not followed by a zig code block", .{ readme_name, marker_line, id }));
                continue;
            }
            const fence = open.?;
            const fence_line = i;
            try appendLine(arena, &out, lines.items[i]);
            i += 1;
            const body_start = i;
            while (i < lines.items.len and !fence.closedBy(lines.items[i])) i += 1;
            if (i == lines.items.len) {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: the zig code block of example \"{s}\" is never closed", .{ readme_name, fence_line + 1, id }));
                for (lines.items[body_start..]) |l| try appendLine(arena, &out, l);
                break;
            }
            const body = lines.items[body_start..i];
            if (std.mem.eql(u8, id, unchecked_id)) {
                unchecked += 1;
                for (body) |l| try appendLine(arena, &out, l);
            } else if (findRegion(regions, id)) |region| {
                region.used = true;
                if (mode == .check) {
                    if (firstDifference(fence, body, region.text)) |n| try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" differs from {s}:{d} at line {d} of the example; run zig build readme", .{ readme_name, body_start + 1 + n, id, region.file, region.line + 1 + n, n + 1 }));
                    for (body) |l| try appendLine(arena, &out, l);
                } else if (region.text.len > 0) {
                    var region_lines = std.mem.splitScalar(u8, region.text, '\n');
                    while (region_lines.next()) |l| {
                        if (l.len == 0) {
                            try appendLine(arena, &out, std.mem.trimEnd(u8, fence.prefix, " \t"));
                        } else {
                            try out.appendSlice(arena, fence.prefix);
                            try appendLine(arena, &out, l);
                        }
                    }
                }
            } else {
                try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" names no region in the example files", .{ readme_name, marker_line, id }));
                for (body) |l| try appendLine(arena, &out, l);
            }
            try appendLine(arena, &out, lines.items[i]);
            i += 1;
            continue;
        }
        if (Fence.parse(line)) |fence| {
            if (fence.isZig()) try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: zig code block has no {s}<id>{s} marker before it", .{ readme_name, i + 1, marker_start, marker_end }));
            // Skip any fenced block whole, so marker-like lines inside it aren't read.
            try appendLine(arena, &out, line);
            i += 1;
            while (i < lines.items.len and !fence.closedBy(lines.items[i])) : (i += 1) try appendLine(arena, &out, lines.items[i]);
            if (i < lines.items.len) {
                try appendLine(arena, &out, lines.items[i]);
                i += 1;
            }
            continue;
        }
        try appendLine(arena, &out, line);
        i += 1;
    }
    if (unchecked > max_unchecked) try problems.append(arena, try std.fmt.allocPrint(arena, "{s}: {d} blocks are marked {s}{s}{s}; at most {d} may be", .{ readme_name, unchecked, marker_start, unchecked_id, marker_end, max_unchecked }));
    for (regions) |r| if (!r.used) try problems.append(arena, try std.fmt.allocPrint(arena, "{s}:{d}: example \"{s}\" is used by no block in {s}", .{ r.file, r.line, r.id, readme_name }));
    // splitScalar yields a last empty line for text ending in a newline; appendLine added a
    // newline after every line, so drop the one that line added.
    if (out.items.len > 0) out.items.len -= 1;
    return out.items;
}

fn appendLine(arena: Allocator, out: *std.ArrayList(u8), line: []const u8) Allocator.Error!void {
    try out.appendSlice(arena, line);
    try out.append(arena, '\n');
}

fn markerId(line: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, line, marker_start) or !std.mem.endsWith(u8, line, marker_end)) return null;
    if (line.len < marker_start.len + marker_end.len) return null;
    return std.mem.trim(u8, line[marker_start.len .. line.len - marker_end.len], " ");
}

fn findRegion(regions: []Region, id: []const u8) ?*Region {
    for (regions) |*r| if (std.mem.eql(u8, r.id, id)) return r;
    return null;
}

/// The index of the first line where `body`, without `fence`'s prefix, and `text` differ, or
/// null if they are equal.
fn firstDifference(fence: Fence, body: []const []const u8, text: []const u8) ?usize {
    if (text.len == 0) return if (body.len == 0) null else 0;
    var expected = std.mem.splitScalar(u8, text, '\n');
    for (body, 0..) |line, n| {
        const want = expected.next() orelse return n;
        if (!std.mem.eql(u8, fence.strip(std.mem.trimEnd(u8, line, "\r")), want)) return n;
    }
    return if (expected.next() != null) body.len else null;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const mode_arg = args.next() orelse return usage();
    const mode = std.meta.stringToEnum(Mode, mode_arg) orelse return usage();
    const root_path = args.next() orelse return usage();
    const readme_name = args.next() orelse return usage();

    var root = try std.Io.Dir.cwd().openDir(io, root_path, .{});
    defer root.close(io);
    const limit: std.Io.Limit = .limited(4 * 1024 * 1024);

    var problems: Problems = .empty;
    var regions: std.ArrayList(Region) = .empty;
    while (args.next()) |file| {
        const source = root.readFileAlloc(io, file, arena, limit) catch |err| {
            std.debug.print("cannot read example file {s}: {t}\n", .{ file, err });
            std.process.exit(1);
        };
        try collectRegions(arena, file, source, &regions, &problems);
    }
    const readme = root.readFileAlloc(io, readme_name, arena, limit) catch |err| {
        std.debug.print("cannot read {s}: {t}\n", .{ readme_name, err });
        std.process.exit(1);
    };
    const updated = try process(arena, mode, readme_name, readme, regions.items, &problems);
    if (mode == .update and !std.mem.eql(u8, updated, readme)) {
        try root.writeFile(io, .{ .sub_path = readme_name, .data = updated });
        std.debug.print("{s}: rewrote its examples from {d} regions\n", .{ readme_name, regions.items.len });
    }
    for (problems.items) |p| std.debug.print("{s}\n", .{p});
    if (problems.items.len > 0) std.process.exit(1);
}

fn usage() noreturn {
    std.debug.print("usage: readme_examples check|update <root dir> <README path> <example file>...\n", .{});
    std.process.exit(2);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    regions: std.ArrayList(Region) = .empty,
    problems: Problems = .empty,

    fn add(f: *Fixture, sources: []const [2][]const u8) !void {
        for (sources) |s| try collectRegions(f.arena.allocator(), s[0], s[1], &f.regions, &f.problems);
    }

    fn run(f: *Fixture, mode: Mode, readme: []const u8) ![]const u8 {
        return process(f.arena.allocator(), mode, "README.md", readme, f.regions.items, &f.problems);
    }

    fn expectProblem(f: *Fixture, needle: []const u8) !void {
        for (f.problems.items) |p| if (std.mem.indexOf(u8, p, needle) != null) return;
        std.debug.print("no problem contains \"{s}\"; problems:\n", .{needle});
        for (f.problems.items) |p| std.debug.print("  {s}\n", .{p});
        return error.TestExpectedProblem;
    }

    fn deinit(f: *Fixture) void {
        f.arena.deinit();
    }
};

const example_file =
    \\const std = @import("std");
    \\test {
    \\    // example: greet
    \\    const name = "Ann";
    \\
    \\    _ = name;
    \\
    \\    // end-example
    \\}
    \\
;

test "a block equal to its region passes, with the region's indentation removed" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    const readme = "Intro.\n\n<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n\n_ = name;\n```\n";
    try testing.expectEqualStrings(readme, try f.run(.check, readme));
    try testing.expectEqual(@as(usize, 0), f.problems.items.len);
}

test "a block that differs is reported with the first differing line" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    _ = try f.run(.check, "<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n\n_ = nom;\n```\n");
    try testing.expectEqual(@as(usize, 1), f.problems.items.len);
    try testing.expectEqualStrings("README.md:5: example \"greet\" differs from ex.zig:6 at line 3 of the example; run zig build readme", f.problems.items[0]);
}

test "a shorter or longer block differs" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    _ = try f.run(.check, "<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n```\n");
    try f.expectProblem("example \"greet\" differs");

    var g: Fixture = .{ .arena = .init(testing.allocator) };
    defer g.deinit();
    try g.add(&.{.{ "ex.zig", example_file }});
    _ = try g.run(.check, "<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n\n_ = name;\nextra\n```\n");
    try g.expectProblem("example \"greet\" differs");
}

test "a zig block without a marker is reported; other fences are not" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    _ = try f.run(.check, "```bash\nzig build\n```\n\n```zig\nconst x = 1;\n```\n<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n\n_ = name;\n```\n");
    try testing.expectEqual(@as(usize, 1), f.problems.items.len);
    try f.expectProblem("README.md:5: zig code block has no");
}

test "a marker naming no region, and a region no block uses, are reported" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    _ = try f.run(.check, "<!-- example: missing -->\n```zig\nx\n```\n");
    try f.expectProblem("example \"missing\" names no region");
    try f.expectProblem("ex.zig:3: example \"greet\" is used by no block");
}

test "a marker not followed by a zig block is reported" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    _ = try f.run(.check, "<!-- example: greet -->\nSome text.\n");
    try f.expectProblem("is not followed by a zig code block");
}

test "an id defined twice, a nested region, and an unterminated region are reported" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{
        .{ "a.zig", "// example: one\nx\n// end-example\n" },
        .{ "b.zig", "// example: one\ny\n// end-example\n// example: two\n// example: three\n// end-example\n// example: four\nz\n" },
    });
    try f.expectProblem("b.zig:1: example \"one\" is already defined at a.zig:1");
    try f.expectProblem("b.zig:5: example \"three\" starts inside example \"two\"");
    try f.expectProblem("b.zig:7: example \"four\" has no // end-example");
}

test "update rewrites blocks from their regions and keeps the rest of the file" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    const readme = "# Title\n\n<!-- example: greet -->\n\n```zig\nold\n```\n\nAfter.\n";
    try testing.expectEqualStrings(
        "# Title\n\n<!-- example: greet -->\n\n```zig\nconst name = \"Ann\";\n\n_ = name;\n```\n\nAfter.\n",
        try f.run(.update, readme),
    );
    try testing.expectEqual(@as(usize, 0), f.problems.items.len);
}

test "unchecked blocks are kept as written, up to the limit" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{});
    const two = "<!-- example: unchecked -->\n```zig\na\n```\n<!-- example: unchecked -->\n```zig\nb\n```\n";
    try testing.expectEqualStrings(two, try f.run(.update, two));
    try testing.expectEqual(@as(usize, 0), f.problems.items.len);
    _ = try f.run(.check, two ++ "<!-- example: unchecked -->\n```zig\nc\n```\n");
    try f.expectProblem("3 blocks are marked <!-- example: unchecked -->; at most 2 may be");
}

test "every fence spelling with a zig info string is a zig block" {
    const spellings = [_][2][]const u8{
        .{ "   ```zig", "   ```" },
        .{ "``` zig", "```" },
        .{ "```Zig", "```" },
        .{ "```ZIG", "```" },
        .{ "~~~zig", "~~~" },
        .{ "````zig", "````" },
        .{ "```zig title=a", "```" },
        .{ "~~~~ zig {.numberLines}", "~~~~~" },
        .{ "> ```zig", "> ```" },
    };
    for (spellings) |s| {
        var f: Fixture = .{ .arena = .init(testing.allocator) };
        defer f.deinit();
        try f.add(&.{.{ "ex.zig", example_file }});
        const a = f.arena.allocator();
        const block = try std.fmt.allocPrint(a, "{s}\nconst x = 1;\n{s}\n", .{ s[0], s[1] });
        _ = try f.run(.check, try std.mem.concat(a, u8, &.{ block, "<!-- example: greet -->\n```zig\nconst name = \"Ann\";\n\n_ = name;\n```\n" }));
        f.expectProblem("README.md:1: zig code block has no") catch |err| {
            std.debug.print("fence: {s}\n", .{s[0]});
            return err;
        };
        try testing.expectEqual(@as(usize, 1), f.problems.items.len);

        var g: Fixture = .{ .arena = .init(testing.allocator) };
        defer g.deinit();
        try g.add(&.{.{ "ex.zig", example_file }});
        _ = try g.run(.check, try std.fmt.allocPrint(g.arena.allocator(), "<!-- example: greet -->\n{s}\nconst name = \"Ann\";\n_ = nom;\n{s}\n", .{ s[0], s[1] }));
        g.expectProblem("example \"greet\" differs") catch |err| {
            std.debug.print("fence: {s}\n", .{s[0]});
            return err;
        };
    }
}

test "fences that aren't zig blocks are skipped whole" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    // A longer fence holds shorter ones, and a tilde fence holds backtick lines.
    const readme =
        \\````markdown
        \\```zig
        \\x
        \\```
        \\````
        \\~~~
        \\```zig
        \\~~~
        \\```zigx`
        \\not a fence: backticks in a backtick info string
        \\``zig
        \\not a fence: two backticks
        \\<!-- example: greet -->
        \\```zig
        \\const name = "Ann";
        \\
        \\_ = name;
        \\```
        \\
    ;
    try testing.expectEqualStrings(readme, try f.run(.check, readme));
    try testing.expectEqual(@as(usize, 0), f.problems.items.len);
}

test "an indented or quoted block is compared and rewritten without its prefix" {
    var f: Fixture = .{ .arena = .init(testing.allocator) };
    defer f.deinit();
    try f.add(&.{.{ "ex.zig", example_file }});
    const indented = "<!-- example: greet -->\n  ~~~Zig\n  const name = \"Ann\";\n\n  _ = name;\n  ~~~\n";
    try testing.expectEqualStrings(indented, try f.run(.check, indented));
    const quoted = "<!-- example: greet -->\n> ```zig\n> const name = \"Ann\";\n>\n> _ = name;\n> ```\n";
    try testing.expectEqualStrings(quoted, try f.run(.check, quoted));
    try testing.expectEqual(@as(usize, 0), f.problems.items.len);
    try testing.expectEqualStrings(quoted, try f.run(.update, "<!-- example: greet -->\n> ```zig\n> old\n> ```\n"));
}
