const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Status = enum { supported, partial, missing, not_applicable };
pub const Row = struct { line: usize, feature: []const u8, status: Status };
pub const Table = union(enum) { rows: []const Row, problem: []const u8 };

/// Reads the rows under "## Feature table" in file order, with 1-based lines.
/// The source must outlive the rows, which may borrow their Feature text from it.
pub fn read(arena: Allocator, source: []const u8) error{OutOfMemory}!Table {
    var lines = std.mem.splitScalar(u8, source, '\n');
    var rows: std.ArrayList(Row) = .empty;
    var line: usize = 0;
    var section_line: ?usize = null;
    var in_section = false;
    var in_table = false;
    while (lines.next()) |raw| {
        line += 1;
        const text = dropCarriageReturn(raw);
        const content = stripIndent(text);
        if (std.mem.startsWith(u8, content, "## ")) {
            in_section = std.mem.eql(u8, std.mem.trimEnd(u8, content, " \t"), "## Feature table");
            if (in_section and section_line == null) section_line = line;
            in_table = false;
            continue;
        }
        if (!in_section) continue;
        const after_hashes = std.mem.trimStart(u8, content, "#");
        const heading = after_hashes.len != content.len and
            (after_hashes.len == 0 or isSpace(after_hashes[0]));
        if (std.mem.trim(u8, text, " \t").len == 0 or heading) {
            in_table = false;
            continue;
        }

        if (!in_table) {
            if (std.mem.findScalar(u8, text, '|') == null) continue;
            if (lines.peek()) |next| {
                if (isDelimiter(dropCarriageReturn(next))) {
                    _ = lines.next();
                    line += 1;
                    in_table = true;
                    continue;
                }
            }
            return problem(arena, line, "a | outside a table needs a header and delimiter above the feature rows", .{});
        }
        if (startsBlock(content))
            return problem(arena, line, "this line starts another block inside a table; put a blank line before it", .{});

        var row = std.mem.trim(u8, text, " \t");
        if (row[0] == '|') row = row[1..];
        if (row.len != 0 and row[row.len - 1] == '|' and !escapedPipe(row, row.len - 1)) {
            row = row[0 .. row.len - 1];
        }
        var cells: [5][]const u8 = undefined;
        var count: usize = 0;
        var start: usize = 0;
        for (row, 0..) |byte, i| {
            if (byte != '|' or escapedPipe(row, i)) continue;
            if (count < cells.len) cells[count] = std.mem.trim(u8, row[start..i], " \t");
            count += 1;
            start = i + 1;
        }
        if (row.len != 0) {
            if (count < cells.len) cells[count] = std.mem.trim(u8, row[start..], " \t");
            count += 1;
        }
        if (count != cells.len)
            return problem(arena, line, "a row needs 5 columns (Feature, SDKs, Stage, Zig, Where), not {d}", .{count});

        const feature = cells[0];
        if (feature.len == 0) return problem(arena, line, "a row needs a feature name", .{});
        for (feature) |byte| {
            if (std.ascii.isControl(byte)) return problem(arena, line, "the Feature cell holds a control character", .{});
        }
        const name = if (std.mem.find(u8, feature, "\\|") != null)
            try std.mem.replaceOwned(u8, arena, feature, "\\|", "|")
        else
            feature;
        const status = std.StaticStringMap(Status).initComptime(.{
            .{ "supported", .supported },
            .{ "partial", .partial },
            .{ "missing", .missing },
            .{ "not applicable", .not_applicable },
        }).get(cells[3]) orelse
            return problem(arena, line, "unknown Zig status \"{s}\" (supported, partial, missing, or not applicable)", .{cells[3]});
        for (rows.items) |previous| {
            if (std.mem.eql(u8, previous.feature, name))
                return problem(arena, line, "duplicate Feature text \"{s}\" (first on line {d})", .{ name, previous.line });
        }
        try rows.append(arena, .{ .line = line, .feature = name, .status = status });
    }
    if (rows.items.len == 0)
        return problem(arena, section_line orelse 1, "no rows under \"## Feature table\"", .{});
    return .{ .rows = rows.items };
}

fn problem(arena: Allocator, line: usize, comptime format: []const u8, args: anytype) error{OutOfMemory}!Table {
    return .{ .problem = try std.fmt.allocPrint(arena, "PARITY.md:{d}: " ++ format, .{line} ++ args) };
}

fn dropCarriageReturn(line: []const u8) []const u8 {
    return if (line.len != 0 and line[line.len - 1] == '\r') line[0 .. line.len - 1] else line;
}

fn stripIndent(line: []const u8) []const u8 {
    var i: usize = 0;
    while (i < @min(3, line.len) and line[i] == ' ') : (i += 1) {}
    return line[i..];
}

fn isSpace(byte: u8) bool {
    return byte == ' ' or byte == '\t';
}

fn escapedPipe(row: []const u8, i: usize) bool {
    return i > 0 and row[i - 1] == '\\';
}

fn isDelimiter(line: []const u8) bool {
    var text = std.mem.trimEnd(u8, stripIndent(line), " \t");
    if (text.len != 0 and text[0] == '|') text = text[1..];
    if (text.len != 0 and text[text.len - 1] == '|') text = text[0 .. text.len - 1];
    if (text.len == 0) return false;

    var cells = std.mem.splitScalar(u8, text, '|');
    while (cells.next()) |cell| {
        var marks = std.mem.trim(u8, cell, " \t");
        if (marks.len != 0 and marks[0] == ':') marks = marks[1..];
        if (marks.len != 0 and marks[marks.len - 1] == ':') marks = marks[0 .. marks.len - 1];
        if (marks.len == 0) return false;
        for (marks) |byte| if (byte != '-') return false;
    }
    return true;
}

fn startsBlock(content: []const u8) bool {
    if (content.len == 0) return false;
    switch (content[0]) {
        '>', '<' => return true,
        '-', '+', '*' => if (content.len > 1 and isSpace(content[1])) return true,
        '0'...'9' => {
            var end: usize = 1;
            while (end < content.len and std.ascii.isDigit(content[end])) : (end += 1) {}
            if (end + 1 < content.len and (content[end] == '.' or content[end] == ')') and isSpace(content[end + 1]))
                return true;
        },
        '`' => if (std.mem.startsWith(u8, content, "```")) return true,
        '~' => if (std.mem.startsWith(u8, content, "~~~")) return true,
        else => {},
    }
    if (content[0] != '-' and content[0] != '*' and content[0] != '_') return false;
    var marks: usize = 0;
    for (content) |byte| switch (byte) {
        '-', '*', '_' => marks += 1,
        ' ', '\t' => {},
        else => return false,
    };
    return marks >= 3;
}

test "conformance parity table reads multiple tables in order with CRLF and escaped pipes" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const source =
        "Outside | the section\r\n" ++
        "  ## Feature table \t\r\n" ++
        "Prose without a pipe\r\n" ++
        " Feature | SDKs | Stage | Zig | Where\r\n" ++
        "   | :--- | ---: | :---: | - | --- | \r\n" ++
        "   | A \\| B | all | GA | supported | docs \\| link |\r\n" ++
        "  C | all | GA | partial | details\\|\r\n" ++
        "  \t\r\n" ++
        "More prose after a blank line\r\n" ++
        "  ### Next table\r\n" ++
        "| Feature | SDKs | Stage | Zig | Where |\r\n" ++
        "--- | --- | --- | --- | ---\r\n" ++
        "| D | all | GA | missing | unfiled |\r\n" ++
        "| E | all | GA | not applicable | reason |\r\n" ++
        " ## Features only one SDK offers\r\n" ++
        "| ignored | all | GA | supported | docs |\r\n";
    const table = try read(state.allocator(), source);
    try std.testing.expectEqualDeep(@as([]const Row, &.{
        .{ .line = 6, .feature = "A | B", .status = .supported },
        .{ .line = 7, .feature = "C", .status = .partial },
        .{ .line = 13, .feature = "D", .status = .missing },
        .{ .line = 14, .feature = "E", .status = .not_applicable },
    }), table.rows);
}

test "conformance parity table requires a delimiter for a table header" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    for ([_][]const u8{ "", "| --- | : : |", "    | --- |", "-||-" }) |delimiter| {
        const source = try std.fmt.allocPrint(arena, "## Feature table\nFeature | SDKs\n{s}", .{delimiter});
        const table = try read(arena, source);
        try std.testing.expectEqualStrings("PARITY.md:2: a | outside a table needs a header and delimiter above the feature rows", table.problem);
    }
}

const fixture_header = "## Feature table\n| Feature | SDKs | Stage | Zig | Where |\n|-|-|-|-|-|\n";

test "conformance parity table ends tables at headings and blank lines" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    for ([_][]const u8{ "\t ", "   ###\tHeading", "  #" }) |boundary| {
        const source = try std.fmt.allocPrint(arena, fixture_header ++ "| A | all | GA | supported | docs |\n{s}\n| orphan |", .{boundary});
        const table = try read(arena, source);
        try std.testing.expectEqualStrings("PARITY.md:6: a | outside a table needs a header and delimiter above the feature rows", table.problem);
    }
}

test "conformance parity table rejects blocks inside a table" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    for ([_][]const u8{
        "  > quote", " - item", "+\titem",            "* item", "32. item", "1)\titem",
        "   ```zig", "~~~",     "  <!-- comment -->", "---",    "* -\t_",
    }) |block| {
        const source = try std.fmt.allocPrint(arena, fixture_header ++ "{s}", .{block});
        const table = try read(arena, source);
        try std.testing.expectEqualStrings("PARITY.md:4: this line starts another block inside a table; put a blank line before it", table.problem);
    }
}

test "conformance parity table reports malformed rows, duplicate names and control characters" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const problems = [_]struct { source: []const u8, message: []const u8 }{
        .{ .source = "prose", .message = "PARITY.md:4: a row needs 5 columns (Feature, SDKs, Stage, Zig, Where), not 1" },
        .{ .source = "| Name | all | GA | supported | docs | extra |", .message = "PARITY.md:4: a row needs 5 columns (Feature, SDKs, Stage, Zig, Where), not 6" },
        .{ .source = "|   | all | GA | supported | docs |", .message = "PARITY.md:4: a row needs a feature name" },
        .{ .source = "| Name | all | GA | Supported | docs |", .message = "PARITY.md:4: unknown Zig status \"Supported\" (supported, partial, missing, or not applicable)" },
        .{ .source = "| A\\|B | all | GA | partial | info |\n| A\\|B  | all | GA | missing | info |", .message = "PARITY.md:5: duplicate Feature text \"A|B\" (first on line 4)" },
        .{ .source = "| Bad\tname | all | GA | supported | docs |", .message = "PARITY.md:4: the Feature cell holds a control character" },
        .{ .source = "| Bad\x7fname | all | GA | supported | docs |", .message = "PARITY.md:4: the Feature cell holds a control character" },
    };
    for (problems) |case| {
        const source = try std.mem.concat(arena, u8, &.{ fixture_header, case.source });
        const table = try read(arena, source);
        try std.testing.expectEqualStrings(case.message, table.problem);
    }
}

test "conformance parity table requires rows under the exact section heading" {
    var state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer state.deinit();
    const arena = state.allocator();
    const empty = try read(arena, "\n" ++ fixture_header);
    try std.testing.expectEqualStrings("PARITY.md:2: no rows under \"## Feature table\"", empty.problem);
    const absent = try read(arena, "## Feature table extra\n| outside |\n|-|\n| row |");
    try std.testing.expectEqualStrings("PARITY.md:1: no rows under \"## Feature table\"", absent.problem);
}
