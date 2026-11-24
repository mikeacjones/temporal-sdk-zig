//! The call sites in workflow code where a blocked coroutine waits, for the `__stack_trace`
//! query: its stack is unwound from the registers it saved when it blocked
//! (`Scheduler.blockedTrace`), and each return address is resolved to a source location with
//! the executable's debug info. Frames in the SDK's own source files and in the standard
//! library are left out, so what remains is workflow code, innermost first.
//!
//! Where optimization inlined calls, ELF debug info lists the inlined functions at each
//! address and every workflow one among them is written. Mach-O debug info, as std reads it,
//! gives only the innermost: an address then resolves to the inlined SDK or standard library
//! line, and the workflow frames inlined with it are left out, so the entry starts at the
//! nearest workflow caller that wasn't inlined. When no frame remains, the answer says
//! `unknown_location` instead.
//!
//! Every string written that the SDK didn't write itself goes through `text`, so the answer
//! is valid UTF-8 and only the SDK starts its lines.

const std = @import("std");
const Scheduler = @import("Scheduler.zig");
const Allocator = std.mem.Allocator;

/// The line written for a coroutine whose call site in workflow code can't be read: the
/// build has no debug info (or the target can't unwind a stack), or optimization inlined the
/// workflow frames away.
pub const unknown_location = "at an unknown location: no debug information covers this call site in workflow code (a build without debug info, or optimization inlined it)";

/// Return addresses read from one coroutine's stack. Deeper frames are left out.
const max_frames = 64;
/// Workflow frames written for one coroutine, innermost first. Deeper ones are left out.
const max_lines = 16;

const enabled = std.options.allow_stack_tracing and std.debug.SelfInfo != void and
    std.debug.cpu_context.Native != noreturn;

/// Reads source locations. Create one per answer with `init`, from a thread's own stack:
/// reading debug info takes more stack than a coroutine has, and file I/O the first time.
pub const Reader = struct {
    di: if (enabled) *std.debug.SelfInfo else void,
    /// The directory holding this SDK's source files, and the standard library's, as debug
    /// info spells them.
    sdk_root: []const u8,
    std_root: []const u8,
    /// Scratch memory for one address's symbols, not std's shared debug-info allocator (see
    /// `failure.addressesText`).
    scratch: std.heap.ArenaAllocator,
    prev_protection: std.Io.CancelProtection,

    /// Null when the build or target gives no source locations. `out` holds the roots.
    pub fn init(out: Allocator) error{OutOfMemory}!?Reader {
        if (comptime !enabled) return null;
        const di = std.debug.getSelfDebugInfo() catch return null;
        const io = std.Options.debug_io;
        // As `failure.addressesText`: a cancel pending on this thread would fail the debug-info
        // file open, and std caches that failure.
        var r: Reader = .{
            .di = di,
            .sdk_root = "",
            .std_root = "",
            .scratch = .init(std.heap.page_allocator),
            .prev_protection = io.swapCancelProtection(.blocked),
        };
        errdefer r.deinit();
        r.sdk_root = try r.root(out, @intFromPtr(&anchor) + 1, @src().file) orelse {
            r.deinit();
            return null;
        };
        r.std_root = try r.root(out, @intFromPtr(&std.debug.captureCurrentStackTrace) + 1, "debug.zig") orelse {
            r.deinit();
            return null;
        };
        return r;
    }

    pub fn deinit(r: *Reader) void {
        r.scratch.deinit();
        _ = std.Options.debug_io.swapCancelProtection(r.prev_protection);
    }

    /// The directory that the file holding `address` is in, as debug info names it, given
    /// that file's path `suffix` below that directory; null when it doesn't resolve.
    fn root(r: *Reader, out: Allocator, address: usize, suffix: []const u8) error{OutOfMemory}!?[]const u8 {
        for (try r.symbols(address)) |sym| {
            const loc = sym.source_location orelse continue;
            if (!std.mem.endsWith(u8, loc.file_name, suffix)) return null;
            return try out.dupe(u8, loc.file_name[0 .. loc.file_name.len - suffix.len]);
        }
        return null;
    }

    /// The symbols of the instruction before return address `address`: the function it is
    /// in, and, where the debug info records them (ELF builds), the functions inlined at it,
    /// innermost first. Empty when nothing resolves. Memory is `scratch`, valid until the
    /// next call.
    fn symbols(r: *Reader, address: usize) error{OutOfMemory}![]const std.debug.Symbol {
        _ = r.scratch.reset(.retain_capacity);
        const a = r.scratch.allocator();
        var list: std.ArrayList(std.debug.Symbol) = .empty;
        r.di.getSymbols(std.Options.debug_io, a, a, address -| 1, true, &list) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return &.{},
        };
        return list.items;
    }

    /// Whether `file` is workflow code: not the standard library's, and not one of this
    /// SDK's own files other than its tests (`*_test.zig`), whose workflows count as
    /// workflow code.
    fn isWorkflowCode(r: *const Reader, file: []const u8) bool {
        if (std.mem.startsWith(u8, file, r.std_root)) return false;
        if (std.mem.startsWith(u8, file, r.sdk_root)) return std.mem.endsWith(u8, file, "_test.zig");
        return true;
    }
};

/// Gives `Reader.init` an address in this file.
noinline fn anchor() void {
    std.mem.doNotOptimizeAway(@as(u8, 0));
}

/// Calls `visitor.frame(file, line, column, function)` for each workflow frame of blocked
/// `fiber`, innermost first, at most `max_lines` of them; the strings are valid only during the
/// call. Returns whether deeper workflow frames were left out. `write` and `locations` both
/// walk the frames here, so the text and the structured answer list the same call sites.
fn eachFrame(r: *Reader, fiber: *const Scheduler.Fiber, visitor: anytype) !bool {
    var count: usize = 0;
    var buf: [max_frames]usize = undefined;
    for (Scheduler.blockedTrace(fiber, &buf)) |addr| {
        for (try r.symbols(addr)) |sym| {
            const loc = sym.source_location orelse continue;
            if (loc.file_name.len == 0 or !r.isWorkflowCode(loc.file_name)) continue;
            if (count == max_lines) return true;
            try visitor.frame(loc.file_name, loc.line, loc.column, sym.name orelse "???");
            count += 1;
        }
    }
    return false;
}

/// Writes the workflow frames of blocked `fiber`, one line each ("at path/to/file.zig:12:5 in
/// function", the path and name through `text`), each starting with `indent`, or one
/// `unknown_location` line. Past `max_lines` call sites, a last line says the deeper ones
/// were left out.
pub fn write(reader: ?*Reader, w: *std.Io.Writer, fiber: *const Scheduler.Fiber, indent: []const u8) (error{OutOfMemory} || std.Io.Writer.Error)!void {
    const Lines = struct {
        w: *std.Io.Writer,
        indent: []const u8,
        count: usize = 0,

        fn frame(l: *@This(), file: []const u8, line: u64, column: u64, function: []const u8) std.Io.Writer.Error!void {
            try l.w.print("{s}at {f}:{d}:{d} in {f}\n", .{ l.indent, text(file), line, column, text(function) });
            l.count += 1;
        }
    };
    var lines: Lines = .{ .w = w, .indent = indent };
    if (reader) |r| {
        if (try eachFrame(r, fiber, &lines)) try w.print("{s}... deeper call sites left out\n", .{indent});
    }
    if (lines.count == 0) try w.print("{s}{s}\n", .{ indent, unknown_location });
}

/// One call site in workflow code. The strings are valid UTF-8 (see `validUtf8`).
pub const Location = struct {
    file_path: []const u8,
    line: u32,
    column: u32,
    function_name: []const u8,
};

/// The workflow frames of blocked `fiber` that `write` writes, innermost first, at most
/// `max_lines`, as locations in `out`. Empty when `reader` is null or no frame resolves.
pub fn locations(reader: ?*Reader, out: Allocator, fiber: *const Scheduler.Fiber) error{OutOfMemory}![]Location {
    const r = reader orelse return &.{};
    const Collect = struct {
        out: Allocator,
        list: std.ArrayList(Location) = .empty,

        fn frame(c: *@This(), file: []const u8, line: u64, column: u64, function: []const u8) error{OutOfMemory}!void {
            try c.list.append(c.out, .{
                .file_path = try validUtf8(c.out, file),
                .line = std.math.cast(u32, line) orelse std.math.maxInt(u32),
                .column = std.math.cast(u32, column) orelse std.math.maxInt(u32),
                .function_name = try validUtf8(c.out, function),
            });
        }
    };
    var collect: Collect = .{ .out = out };
    _ = try eachFrame(r, fiber, &collect);
    return collect.list.items;
}

/// Lines of source around each location in an excerpt, before and after.
pub const excerpt_context_lines = 5;
/// A source file larger than this is left out of an answer's sources.
pub const max_source_file_bytes = 4 * 1024 * 1024;
/// The excerpts of one answer stop once their content would pass this many bytes.
pub const max_excerpt_bytes = 256 * 1024;

/// Part of a source file: `content` is its lines from the one after the first
/// `line_offset` lines, each with its line feed, valid UTF-8.
pub const Excerpt = struct {
    line_offset: u32,
    content: []const u8,
};

/// The excerpts of the source file at `path` around `lines` (1-based, in any order, repeats
/// allowed): `excerpt_context_lines` lines before and after each, windows that overlap or
/// touch merged, in ascending order. Null when the file is missing, unreadable, or larger
/// than `max_source_file_bytes`, or no excerpt fits the bytes `budget` has left; each
/// excerpt kept takes its length from `budget`, and once one doesn't fit, none after it is
/// kept. The file is read with `std.Options.debug_io` into `scratch`, which is reset first;
/// the excerpts are copied into `out`.
pub fn excerpts(out: Allocator, scratch: *std.heap.ArenaAllocator, path: []const u8, lines: []const u32, budget: *usize) error{OutOfMemory}!?[]Excerpt {
    if (lines.len == 0 or budget.* == 0) return null;
    _ = scratch.reset(.retain_capacity);
    const a = scratch.allocator();
    // A file of more than `max_source_file_bytes` reaches the limit: error.StreamTooLong.
    const file = std.Io.Dir.cwd().readFileAlloc(std.Options.debug_io, path, a, .limited(max_source_file_bytes + 1)) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };

    const sorted = try a.dupe(u32, lines);
    std.mem.sortUnstable(u32, sorted, {}, std.sort.asc(u32));

    var result: std.ArrayList(Excerpt) = .empty;
    // One pass over the file: `pos` is where line `line` starts.
    var line: u32 = 1;
    var pos: usize = 0;
    var i: usize = 0;
    windows: while (i < sorted.len) {
        const first = @max(sorted[i], 1 + excerpt_context_lines) - excerpt_context_lines;
        var last = sorted[i] +| excerpt_context_lines;
        i += 1;
        while (i < sorted.len and (@max(sorted[i], 1 + excerpt_context_lines) - excerpt_context_lines) <= last +| 1) : (i += 1)
            last = @max(last, sorted[i] +| excerpt_context_lines);
        while (line < first) : (line += 1) {
            pos = (std.mem.indexOfScalarPos(u8, file, pos, '\n') orelse break :windows) + 1;
        }
        // Past the last line: a file that ends with a line feed has no line after it.
        if (pos == file.len) break;
        const start = pos;
        while (line <= last and pos < file.len) : (line += 1) {
            pos = if (std.mem.indexOfScalarPos(u8, file, pos, '\n')) |nl| nl + 1 else file.len;
        }
        const bytes = file[start..pos];
        if (bytes.len > budget.*) {
            budget.* = 0;
            break;
        }
        budget.* -= bytes.len;
        try result.append(out, .{ .line_offset = first - 1, .content = try validUtf8(out, bytes) });
    }
    if (result.items.len == 0) return null;
    return result.items;
}

/// `bytes`, a string the SDK didn't write (a name from workflow code, the server, or debug
/// info), as `__stack_trace` answer text: valid UTF-8 kept as is; each byte that isn't part
/// of a valid UTF-8 sequence written as U+FFFD; and control characters, `"` and `\`
/// escaped as in Zig string literals (`\n`, `\"`, `\x01`). So the answer is always valid
/// UTF-8, which the JSON converter encodes as a string, and a name can't start a line or
/// end a quote of its own.
pub fn text(bytes: []const u8) Text {
    return .{ .bytes = bytes };
}

pub const Text = struct {
    bytes: []const u8,

    pub fn format(t: Text, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const bytes = t.bytes;
        var i: usize = 0;
        while (i < bytes.len) {
            const b = bytes[i];
            if (b < 0x80) {
                switch (b) {
                    '\n' => try w.writeAll("\\n"),
                    '\r' => try w.writeAll("\\r"),
                    '\t' => try w.writeAll("\\t"),
                    '"' => try w.writeAll("\\\""),
                    '\\' => try w.writeAll("\\\\"),
                    0...8, 11, 12, 14...0x1f, 0x7f => try w.print("\\x{x:0>2}", .{b}),
                    else => try w.writeByte(b),
                }
                i += 1;
                continue;
            }
            const len = sequenceLength(bytes, i);
            if (len == 0) {
                try w.writeAll(&std.unicode.replacement_character_utf8);
                i += 1;
                continue;
            }
            try w.writeAll(bytes[i..][0..len]);
            i += len;
        }
    }
};

/// The length of the valid UTF-8 sequence that starts at `bytes[i]`, a byte of 0x80 or more,
/// or 0 if none does.
fn sequenceLength(bytes: []const u8, i: usize) usize {
    const len = std.unicode.utf8ByteSequenceLength(bytes[i]) catch return 0;
    if (i + len > bytes.len) return 0;
    _ = std.unicode.utf8Decode(bytes[i..][0..len]) catch return 0;
    return len;
}

/// A copy of `bytes` in `out` with each byte that isn't part of a valid UTF-8 sequence
/// replaced by U+FFFD, as `text` does, but with nothing escaped: the JSON encoder escapes.
pub fn validUtf8(out: Allocator, bytes: []const u8) error{OutOfMemory}![]const u8 {
    if (std.unicode.utf8ValidateSlice(bytes)) return out.dupe(u8, bytes);
    var copy: std.ArrayList(u8) = try .initCapacity(out, bytes.len + 8);
    var i: usize = 0;
    while (i < bytes.len) {
        if (bytes[i] < 0x80) {
            try copy.append(out, bytes[i]);
            i += 1;
            continue;
        }
        const len = sequenceLength(bytes, i);
        if (len == 0) {
            try copy.appendSlice(out, &std.unicode.replacement_character_utf8);
            i += 1;
            continue;
        }
        try copy.appendSlice(out, bytes[i..][0..len]);
        i += len;
    }
    return copy.items;
}

test "text keeps valid UTF-8, replaces invalid bytes, and escapes what could change the answer's lines" {
    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.print("{f}", .{text("订单 ok\n\"q\"\\\x01\x7f" ++ "\xe8\xae" ++ "x\xff\xc0\xaf\xed\xa0\x80")});
    const out = w.buffered();
    try std.testing.expectEqualStrings("订单 ok\\n\\\"q\\\"\\\\\\x01\\x7f\u{fffd}\u{fffd}x\u{fffd}\u{fffd}\u{fffd}\u{fffd}\u{fffd}\u{fffd}", out);
    try std.testing.expect(std.unicode.utf8ValidateSlice(out));
}

/// A file of `n` lines "line 1" to "line n" in `tmp`, the last without a line feed, and the
/// path to it from the working directory.
fn numberedFile(a: Allocator, tmp: *std.testing.TmpDir, name: []const u8, n: u32) ![]const u8 {
    var data: std.Io.Writer.Allocating = .init(a);
    for (1..n + 1) |i| try data.writer.print("line {d}{s}", .{ i, if (i == n) "" else "\n" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = data.written() });
    return std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, name });
}

/// "line first\n" to "line last\n", without the last line feed when `last` ends the file.
fn numberedLines(a: Allocator, first: u32, last: u32, ends_file: bool) ![]const u8 {
    var data: std.Io.Writer.Allocating = .init(a);
    for (first..last + 1) |i| try data.writer.print("line {d}{s}", .{ i, if (i == last and ends_file) "" else "\n" });
    return data.written();
}

test "excerpts take five lines around each location, merge windows that overlap or touch, and give the lines before each" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var scratch: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer scratch.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try numberedFile(a, &tmp, "source.zig", 60);

    // 3 and 14 give windows 1-8 and 9-19, which touch; 32 (twice) gives 27-37; 58 gives
    // 53-63, cut at the file's end; 100 is past it.
    var budget: usize = max_excerpt_bytes;
    const got = (try excerpts(a, &scratch, path, &.{ 32, 100, 3, 58, 14, 32 }, &budget)).?;
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqual(@as(u32, 0), got[0].line_offset);
    try std.testing.expectEqualStrings(try numberedLines(a, 1, 19, false), got[0].content);
    try std.testing.expectEqual(@as(u32, 26), got[1].line_offset);
    try std.testing.expectEqualStrings(try numberedLines(a, 27, 37, false), got[1].content);
    try std.testing.expectEqual(@as(u32, 52), got[2].line_offset);
    try std.testing.expectEqualStrings(try numberedLines(a, 53, 60, true), got[2].content);
    try std.testing.expectEqual(max_excerpt_bytes - got[0].content.len - got[1].content.len - got[2].content.len, budget);

    // 10 gives 5-15 and 22 gives 17-27, with line 16 between them; 21 gives 16-26, which
    // touches 5-15.
    budget = max_excerpt_bytes;
    const apart = (try excerpts(a, &scratch, path, &.{ 10, 22 }, &budget)).?;
    try std.testing.expectEqual(@as(usize, 2), apart.len);
    try std.testing.expectEqual(@as(u32, 4), apart[0].line_offset);
    try std.testing.expectEqual(@as(u32, 16), apart[1].line_offset);
    budget = max_excerpt_bytes;
    try std.testing.expectEqual(@as(usize, 1), (try excerpts(a, &scratch, path, &.{ 10, 21 }, &budget)).?.len);
}

test "excerpts stop at the byte budget, and a missing or too large file, or one with no location in it, gives none" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var scratch: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer scratch.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try numberedFile(a, &tmp, "source.zig", 60);

    // Room for the first excerpt only: the second doesn't fit, and neither does any after it.
    const first_len = (try numberedLines(a, 1, 8, false)).len;
    var budget: usize = first_len + 5;
    const cut = (try excerpts(a, &scratch, path, &.{ 3, 30, 50 }, &budget)).?;
    try std.testing.expectEqual(@as(usize, 1), cut.len);
    try std.testing.expectEqual(@as(usize, 0), budget);
    try std.testing.expect(try excerpts(a, &scratch, path, &.{3}, &budget) == null);
    budget = first_len - 1;
    try std.testing.expect(try excerpts(a, &scratch, path, &.{3}, &budget) == null);

    budget = max_excerpt_bytes;
    try std.testing.expect(try excerpts(a, &scratch, path, &.{}, &budget) == null);
    // A location past the end of the file (one changed since the build) still shows the
    // file's last lines when its window reaches them.
    try std.testing.expect(try excerpts(a, &scratch, path, &.{66}, &budget) == null);
    try std.testing.expectEqual(@as(u32, 59), (try excerpts(a, &scratch, path, &.{65}, &budget)).?[0].line_offset);
    budget = max_excerpt_bytes;
    const missing = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, "missing.zig" });
    try std.testing.expect(try excerpts(a, &scratch, missing, &.{1}, &budget) == null);

    const big = try a.alloc(u8, max_source_file_bytes + 1);
    @memset(big, '\n');
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "big.zig", .data = big });
    const big_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, "big.zig" });
    try std.testing.expect(try excerpts(a, &scratch, big_path, &.{1}, &budget) == null);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "big.zig", .data = big[1..] });
    try std.testing.expectEqual(@as(usize, 1), (try excerpts(a, &scratch, big_path, &.{1}, &budget)).?.len);
    try std.testing.expectEqual(max_excerpt_bytes, budget + 6);
}

test "excerpts and validUtf8 replace bytes that aren't UTF-8 and escape nothing" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var scratch: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer scratch.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "bad.zig", .data = "const s = \"\xff订单\";\n\t\"quoted\\\"\n" });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path, "bad.zig" });
    var budget: usize = max_excerpt_bytes;
    const got = (try excerpts(a, &scratch, path, &.{1}, &budget)).?;
    try std.testing.expectEqualStrings("const s = \"\u{fffd}订单\";\n\t\"quoted\\\"\n", got[0].content);
    try std.testing.expectEqualStrings("a\u{fffd}\u{fffd}b\u{fffd}", try validUtf8(a, "a\xe8\xaeb\xc0"));
    try std.testing.expectEqualStrings("订单\n\"", try validUtf8(a, "订单\n\""));
}
