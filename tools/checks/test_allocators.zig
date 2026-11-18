const std = @import("std");
const sources = @import("sources.zig");
const Ast = std.zig.Ast;

const Entry = struct {
    file: []const u8,
    test_name: []const u8,
    reason: []const u8,

    fn lessThan(_: void, a: Entry, b: Entry) bool {
        return switch (std.mem.order(u8, a.file, b.file)) {
            .lt => true,
            .eq => std.mem.lessThan(u8, a.test_name, b.test_name),
            .gt => false,
        };
    }
};

const AllocatorName = enum {
    page_allocator,
    c_allocator,
    raw_c_allocator,
    smp_allocator,
    wasm_allocator,
    brk_allocator,
    FixedBufferAllocator,
    BufferFirstAllocator,
    DebugAllocator,
    SmpAllocator,
    SafeAllocator,
    PageAllocator,
    WasmAllocator,
    BrkAllocator,
};

const TestBody = struct {
    first: Ast.TokenIndex,
    end: Ast.TokenIndex,
    name: []const u8,

    fn lessThan(_: void, a: TestBody, b: TestBody) bool {
        return a.first < b.first;
    }
};

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const root_path = args.next() orelse usage();
    const allow_path = args.next() orelse usage();
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    scan(init.io, arena, root_path, allow_path, &args, &stdout.interface) catch |err| {
        std.debug.print("test-allocators: scan failed: {t}\n", .{err});
        std.process.exit(1);
    };
}

fn scan(io: std.Io, arena: std.mem.Allocator, root_path: []const u8, allow_path: []const u8, args: *std.process.Args.Iterator, out: *std.Io.Writer) !void {
    const root = std.Io.Dir.cwd().openDir(io, root_path, .{}) catch |err| cannotRead(".", err);
    defer root.close(io);
    const entries = try readAllowList(io, arena, root, allow_path);
    const matched = try arena.alloc(bool, entries.len);
    @memset(matched, false);
    var paths: std.ArrayList([]const u8) = .empty;
    while (args.next()) |path| try paths.append(arena, path);
    var failed: []const u8 = ".";
    const files = sources.collect(io, arena, root, paths.items, &failed) catch |err| cannotRead(failed, err);

    var found = false;
    for (files) |file| {
        const source = readFile(io, arena, root, file);
        if (try scanFile(arena, file, source, entries, matched, out)) found = true;
    }
    for (entries, matched) |entry, used| {
        if (used) continue;
        found = true;
        try out.print("stale: {s} test \"{f}\" (no such use; remove the entry)\n", .{ entry.file, std.zig.fmtString(entry.test_name) });
    }
    try out.flush();
    if (found) std.process.exit(1);
}

fn readFile(io: std.Io, arena: std.mem.Allocator, root: std.Io.Dir, path: []const u8) [:0]const u8 {
    return root.readFileAllocOptions(io, path, arena, .limited(std.zig.max_src_size), .@"1", 0) catch |err| cannotRead(path, err);
}

fn readAllowList(io: std.Io, arena: std.mem.Allocator, root: std.Io.Dir, path: []const u8) ![]const Entry {
    const source = readFile(io, arena, root, path);
    var diagnostics: std.zon.parse.Diagnostics = undefined;
    const parsed = std.zon.parse.fromSlice([]const Entry, .{
        .gpa = arena,
        .arena = arena,
        .source = source,
        .diagnostics = &diagnostics,
    }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.ParseZon => {
            std.debug.print("test-allocators: invalid allow-list: {f}\n", .{diagnostics.fmt(path)});
            std.process.exit(1);
        },
    };
    const entries = try arena.dupe(Entry, parsed);
    std.mem.sortUnstable(Entry, entries, {}, Entry.lessThan);
    for (entries, 0..) |entry, i| {
        if (std.mem.trim(u8, entry.reason, " \t\r\n").len == 0) {
            std.debug.print("test-allocators: {s}: empty reason for {s} test \"{f}\"\n", .{ path, entry.file, std.zig.fmtString(entry.test_name) });
            std.process.exit(1);
        }
        if (i != 0 and !Entry.lessThan({}, entries[i - 1], entry)) {
            std.debug.print("test-allocators: {s}: duplicate entry for {s} test \"{f}\"\n", .{ path, entry.file, std.zig.fmtString(entry.test_name) });
            std.process.exit(1);
        }
    }
    return entries;
}

fn scanFile(arena: std.mem.Allocator, file: []const u8, source: [:0]const u8, entries: []const Entry, matched: []bool, out: *std.Io.Writer) !bool {
    const tree = try Ast.parse(arena, source, .{});
    if (tree.errors.len != 0) {
        const err = tree.errors[0];
        const loc = tree.tokenLocation(0, err.token);
        var message: std.Io.Writer.Allocating = .init(arena);
        try tree.renderError(err, &message.writer);
        std.debug.print("test-allocators: cannot parse {s}:{d}:{d}: {s}\n", .{ file, loc.line + 1, loc.column + 1, message.written() });
        return true;
    }

    var tests: std.ArrayList(TestBody) = .empty;
    for (tree.nodes.items(.tag), 0..) |tag, index| {
        if (tag != .test_decl) continue;
        const name, const body = tree.nodeData(@fromBackingInt(@intCast(index))).opt_token_and_node;
        try tests.append(arena, .{
            .first = tree.firstToken(body),
            .end = tree.lastToken(body),
            .name = if (name.unwrap()) |token| try testName(arena, tree.tokenSlice(token)) else "",
        });
    }
    if (tests.items.len == 0) return false;
    std.mem.sortUnstable(TestBody, tests.items, {}, TestBody.lessThan);

    var active: std.ArrayList(TestBody) = .empty;
    var next: usize = 0;
    var line: usize = 1;
    var offset: usize = 0;
    var reported: std.enums.EnumSet(AllocatorName) = .empty;
    var found = false;
    // Attribute nested tests to the innermost body and visit findings in source order.
    for (tree.tokens.items(.tag), 0..) |tag, index| {
        while (active.getLastOrNull()) |body| {
            if (index < body.end) break;
            _ = active.pop();
        }
        if (next < tests.items.len and tests.items[next].first == index) {
            try active.append(arena, tests.items[next]);
            next += 1;
        }
        const body = active.getLastOrNull() orelse continue;
        if (tag != .identifier) continue;
        const token: Ast.TokenIndex = @intCast(index);
        const identifier = std.meta.stringToEnum(AllocatorName, tree.tokenSlice(token)) orelse continue;
        for (entries, matched) |entry, *used| {
            if (!std.mem.eql(u8, entry.file, file) or !std.mem.eql(u8, entry.test_name, body.name)) continue;
            used.* = true;
            break;
        } else {
            // Deduplicate unallowed uses only, so an allowed use cannot hide another test's use.
            const start = tree.tokenStart(token);
            const lines = std.mem.countScalar(u8, source[offset..start], '\n');
            offset = start;
            line += lines;
            if (lines != 0) reported = .empty;
            if (reported.contains(identifier)) continue;
            reported.insert(identifier);
            found = true;
            try out.print("{s}:{d}: test \"{f}\" uses {t}\n", .{ file, line, std.zig.fmtString(body.name), identifier });
        }
    }
    return found;
}

fn testName(arena: std.mem.Allocator, token: []const u8) ![]const u8 {
    if (token[0] == '"') return std.zig.string_literal.parseAlloc(arena, token);
    if (token[0] == '@') return std.zig.string_literal.parseAlloc(arena, token[1..]);
    return token;
}

fn cannotRead(path: []const u8, err: anyerror) noreturn {
    std.debug.print("test-allocators: cannot read {s}: {t}\n", .{ path, err });
    std.process.exit(1);
}

fn usage() noreturn {
    std.debug.print("test-allocators: usage: test_allocators <root dir> <allow-list file> [<path>...]\n", .{});
    std.process.exit(2);
}
