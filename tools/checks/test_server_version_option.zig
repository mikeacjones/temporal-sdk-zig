//! The `b.dependency` argument that README.md, `testing.TestServer.Options.version` and the
//! changelog give for choosing the time-skipping test server release must name an option
//! build.zig declares. Zig only logs an argument a dependency doesn't declare and goes on, so
//! a misspelled one is silently ignored: the dependent gets the pinned release, not the one it
//! asked for. Run from the repository root (build.zig sets the working directory).

const std = @import("std");
const Io = std.Io;

const doc_files = [_][]const u8{
    "README.md",
    "src/testing.zig",
    "changelog/breaking-changes/test-server-version-pin.md",
};

/// How a `b.dependency` argument is spelled, and the build option it sets.
const spellings = [_]struct { arg: []const u8, option: []const u8 }{
    .{ .arg = ".test_server_version = ", .option = "test_server_version" },
    .{ .arg = ".@\"test-server-version\" = ", .option = "test-server-version" },
};

fn declaresOption(build_zig: []const u8, name: []const u8) bool {
    var rest = build_zig;
    while (std.mem.indexOf(u8, rest, "b.option(")) |at| {
        rest = rest[at + "b.option(".len ..];
        const open = std.mem.indexOfScalar(u8, rest, '"') orelse return false;
        const close = std.mem.indexOfScalarPos(u8, rest, open + 1, '"') orelse return false;
        if (std.mem.eql(u8, rest[open + 1 .. close], name)) return true;
    }
    return false;
}

test "the b.dependency argument the docs give for the test server release is an option build.zig declares" {
    const io = std.testing.io;
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cwd = Io.Dir.cwd();

    const build_zig = try cwd.readFileAlloc(io, "build.zig", arena, .unlimited);
    try std.testing.expect(declaresOption(build_zig, "test-server-version"));

    var readme_mentions: usize = 0;
    for (doc_files) |path| {
        const text = cwd.readFileAlloc(io, path, arena, .unlimited) catch |err| switch (err) {
            // The changelog fragment goes when a release is assembled.
            error.FileNotFound => continue,
            else => |e| return e,
        };
        for (spellings) |s| {
            const count = std.mem.count(u8, text, s.arg);
            if (std.mem.eql(u8, path, "README.md")) readme_mentions += count;
            if (count > 0 and !declaresOption(build_zig, s.option)) {
                std.debug.print("{s} passes `{s}...` to b.dependency, but build.zig declares no option \"{s}\", so Zig ignores it\n", .{ path, s.arg, s.option });
                return error.UndeclaredDependencyArgument;
            }
        }
    }
    // README.md must still say how to choose another release.
    try std.testing.expect(readme_mentions > 0);
}

test "every changelog fragment that gives the test server b.dependency argument is one the check reads" {
    const io = std.testing.io;
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const cwd = Io.Dir.cwd();

    const changelog = try cwd.openDir(io, "changelog", .{ .iterate = true });
    defer changelog.close(io);
    var categories = changelog.iterate();
    while (try categories.next(io)) |category| {
        if (category.kind != .directory) continue;
        const dir = try changelog.openDir(io, category.name, .{ .iterate = true });
        defer dir.close(io);
        var fragments = dir.iterate();
        while (try fragments.next(io)) |fragment| {
            if (fragment.kind != .file or !std.mem.endsWith(u8, fragment.name, ".md")) continue;
            const path = try std.mem.concat(arena, u8, &.{ "changelog/", category.name, "/", fragment.name });
            const text = try cwd.readFileAlloc(io, path, arena, .unlimited);
            var gives_argument = false;
            for (spellings) |s| {
                if (std.mem.indexOf(u8, text, s.arg) != null) gives_argument = true;
            }
            if (!gives_argument) continue;
            var read = false;
            for (doc_files) |doc| {
                if (std.mem.eql(u8, doc, path)) read = true;
            }
            if (!read) {
                std.debug.print("{s} gives a b.dependency argument for the test server release, but doc_files doesn't list it, so a misspelled argument there goes unchecked\n", .{path});
                return error.UncheckedChangelogFragment;
            }
        }
    }
}
