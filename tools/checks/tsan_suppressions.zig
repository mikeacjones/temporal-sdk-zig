//! Thread Sanitizer suppressions from a known-findings file such as tools/checks/tsan.zon,
//! for `zig build tsan`.
//!
//!   tsan-suppressions render <file.zon>
//!     Prints one `<kind>:<site>` line per entry. A malformed file fails, naming the entry.
//!   tsan-suppressions exec [--symbolizer <path>] <suppressions file> <program> [args...]
//!     Runs the program in place of this process with `abort_on_error=0` and the file
//!     added to `TSAN_OPTIONS`, so a report ends it with exit 66 and no crash report on
//!     macOS. Replacing the process keeps the build runner's test protocol on stdin and
//!     stdout intact. `--symbolizer` adds `external_symbolizer_path` too, for the
//!     symbolizer build.zig builds (tools/checks/symbolizer.zig).
//!
//! A `__tsan_default_suppressions` in the sanitized binary would need neither, but on macOS
//! the runtime is a dylib that calls its own weak default directly, so a definition in the
//! binary is never called. The build cannot pass a generated file's path in an environment
//! variable either, which is why `exec` exists, and why it takes the symbolizer's path.

const std = @import("std");
const Allocator = std.mem.Allocator;

const usage_text = "usage: tsan-suppressions render <file.zon> | exec [--symbolizer <path>] <suppressions file> <program> [args...]\n";

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);
    if (args.len < 2) usage();
    if (std.mem.eql(u8, args[1], "render")) {
        if (args.len != 3) usage();
        const source = std.Io.Dir.cwd().readFileAllocOptions(init.io, args[2], arena, .limited(1024 * 1024), .of(u8), 0) catch |err|
            fatal("cannot read {s}: {t}", .{ args[2], err });
        switch (try render(init.gpa, arena, source, args[2])) {
            .text => |text| std.Io.File.stdout().writeStreamingAll(init.io, text) catch |err| fatal("cannot write the suppressions: {t}", .{err}),
            .invalid => |msg| fatal("{s}", .{msg}),
        }
    } else if (std.mem.eql(u8, args[1], "exec")) {
        var rest = args[2..];
        var symbolizer: ?[]const u8 = null;
        if (rest.len > 0 and std.mem.eql(u8, rest[0], "--symbolizer")) {
            if (rest.len < 2) usage();
            symbolizer = rest[1];
            rest = rest[2..];
        }
        if (rest.len < 2) usage();
        const options = tsanOptions(arena, init.environ_map.get("TSAN_OPTIONS") orelse "", rest[0], symbolizer) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.QuoteInPath => fatal("the suppressions path {s} or the symbolizer path contains '\"', which TSAN_OPTIONS cannot quote", .{rest[0]}),
        };
        try init.environ_map.put("TSAN_OPTIONS", options);
        const err = std.process.replace(init.io, .{ .argv = rest[1..], .environ_map = init.environ_map });
        fatal("cannot run {s}: {t}", .{ rest[1], err });
    } else usage();
}

/// The caller's options first, so that these win.
fn tsanOptions(arena: Allocator, inherited: []const u8, suppressions_path: []const u8, symbolizer_path: ?[]const u8) error{ OutOfMemory, QuoteInPath }![]const u8 {
    if (std.mem.indexOfScalar(u8, suppressions_path, '"') != null) return error.QuoteInPath;
    const sep = if (inherited.len == 0) "" else " ";
    const options = try std.fmt.allocPrint(arena, "{s}{s}abort_on_error=0 suppressions=\"{s}\"", .{ inherited, sep, suppressions_path });
    const path = symbolizer_path orelse return options;
    if (std.mem.indexOfScalar(u8, path, '"') != null) return error.QuoteInPath;
    return std.fmt.allocPrint(arena, "{s} external_symbolizer_path=\"{s}\"", .{ options, path });
}

fn usage() noreturn {
    std.debug.print(usage_text, .{});
    std.process.exit(2);
}

fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.debug.print("tsan-suppressions: " ++ format ++ "\n", args);
    std.process.exit(1);
}

pub const Kind = enum { race, race_top, mutex, thread, signal, deadlock, called_from_lib };

pub const Entry = struct {
    /// A function, file or module name as the report prints it. The runtime matches it
    /// against every frame of the report.
    site: []const u8,
    /// The bug filed for the finding.
    bead: []const u8,
    reason: []const u8,
    kind: Kind = .race,
};

pub const Rendered = union(enum) {
    /// One `<kind>:<site>` line per entry, in file order.
    text: []const u8,
    /// Why the file is malformed, naming the file and the entry.
    invalid: []const u8,
};

/// Parses `source`, a ZON list of `Entry`, and renders it as suppressions. Results and
/// messages are allocated with `arena`; `gpa` is scratch.
pub fn render(gpa: Allocator, arena: Allocator, source: [:0]const u8, path: []const u8) Allocator.Error!Rendered {
    var diag: std.zon.parse.Diagnostics = undefined;
    const entries = std.zon.parse.fromSlice([]const Entry, .{
        .gpa = gpa,
        .arena = arena,
        .source = source,
        .diagnostics = &diag,
    }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.ParseZon => return .{ .invalid = try std.fmt.allocPrint(arena, "{f}", .{diag.fmt(path)}) },
    };
    var text: std.Io.Writer.Allocating = .init(arena);
    for (entries, 0..) |entry, i| {
        // A blank pattern would match every report.
        const problem: ?[]const u8 = if (std.mem.trim(u8, entry.site, " \t\r\n").len == 0)
            "an empty site"
        else if (entry.bead.len == 0)
            "an empty bead"
        else if (entry.reason.len == 0)
            "an empty reason"
        else if (std.mem.indexOfAny(u8, entry.site, "\r\n#") != null)
            "a site containing a line break or '#'"
        else
            null;
        if (problem) |what| return .{ .invalid = try std.fmt.allocPrint(arena, "{s}: entry {d} (site \"{s}\") has {s}", .{ path, i, entry.site, what }) };
        text.writer.print("{t}:{s}\n", .{ entry.kind, entry.site }) catch return error.OutOfMemory;
    }
    return .{ .text = text.written() };
}

const testing = std.testing;

fn renderForTest(arena: *std.heap.ArenaAllocator, source: [:0]const u8) !Rendered {
    return render(testing.allocator, arena.allocator(), source, "known.zon");
}

test "an empty list renders no suppressions" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    try testing.expectEqualStrings("", (try renderForTest(&arena, ".{}")).text);
}

test "entries render as kind and site lines in file order, race by default" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const rendered = try renderForTest(&arena,
        \\.{
        \\    .{ .site = "worker.Poller.run", .bead = "temporal-sdk-zig-a", .reason = "r" },
        \\    .{ .site = "bridge.x", .bead = "temporal-sdk-zig-b", .reason = "r", .kind = .race_top },
        \\    .{ .site = "lock.y", .bead = "temporal-sdk-zig-c", .reason = "r", .kind = .mutex },
        \\}
    );
    try testing.expectEqualStrings("race:worker.Poller.run\nrace_top:bridge.x\nmutex:lock.y\n", rendered.text);
}

test "an entry with an empty field or a site that would break the format is rejected by index and site" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const cases = [_]struct { source: [:0]const u8, message: []const u8 }{
        .{ .source = ".{ .{ .site = \"\", .bead = \"b\", .reason = \"r\" } }", .message = "known.zon: entry 0 (site \"\") has an empty site" },
        .{ .source = ".{ .{ .site = \" \\t \", .bead = \"b\", .reason = \"r\" } }", .message = "known.zon: entry 0 (site \" \t \") has an empty site" },
        .{ .source = ".{ .{ .site = \"a\", .bead = \"b\", .reason = \"r\" }, .{ .site = \"s\", .bead = \"\", .reason = \"r\" } }", .message = "known.zon: entry 1 (site \"s\") has an empty bead" },
        .{ .source = ".{ .{ .site = \"s\", .bead = \"b\", .reason = \"\" } }", .message = "known.zon: entry 0 (site \"s\") has an empty reason" },
        .{ .source = ".{ .{ .site = \"s#t\", .bead = \"b\", .reason = \"r\" } }", .message = "known.zon: entry 0 (site \"s#t\") has a site containing a line break or '#'" },
        .{ .source = ".{ .{ .site = \"s\\nrace:all\", .bead = \"b\", .reason = \"r\" } }", .message = "known.zon: entry 0 (site \"s\nrace:all\") has a site containing a line break or '#'" },
    };
    for (cases) |case| try testing.expectEqualStrings(case.message, (try renderForTest(&arena, case.source)).invalid);
}

test "a file that is not a list of entries is rejected with the parser's message" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const unknown_kind = (try renderForTest(&arena, ".{ .{ .site = \"s\", .bead = \"b\", .reason = \"r\", .kind = .leak } }")).invalid;
    try testing.expect(std.mem.startsWith(u8, unknown_kind, "known.zon:1:"));
    const missing_reason = (try renderForTest(&arena, ".{ .{ .site = \"s\", .bead = \"b\" } }")).invalid;
    try testing.expect(std.mem.indexOf(u8, missing_reason, "reason") != null);
}

test "exec adds abort_on_error and the quoted suppressions path after the caller's options" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings("abort_on_error=0 suppressions=\"/c/x y.supp\"", try tsanOptions(a, "", "/c/x y.supp", null));
    try testing.expectEqualStrings("verbosity=1 abort_on_error=0 suppressions=\"s\"", try tsanOptions(a, "verbosity=1", "s", null));
    try testing.expectError(error.QuoteInPath, tsanOptions(a, "", "a\"b", null));
}

test "exec --symbolizer adds the quoted symbolizer path last, so it wins over the caller's" {
    var arena: std.heap.ArenaAllocator = .init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings(
        "external_symbolizer_path=\"/x/atos\" abort_on_error=0 suppressions=\"s\" external_symbolizer_path=\"/c/d e/llvm-symbolizer\"",
        try tsanOptions(a, "external_symbolizer_path=\"/x/atos\"", "s", "/c/d e/llvm-symbolizer"),
    );
    try testing.expectError(error.QuoteInPath, tsanOptions(a, "", "s", "a\"b"));
}
