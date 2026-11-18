const std = @import("std");
const Dir = std.Io.Dir;

/// The directories the source scans read when given no path.
pub const default_roots = [_][]const u8{ "src", "tools", "examples", "samples" };
/// Left out of every scan. An entry ending in '/' is a directory, any other a file.
pub const excluded = [_][]const u8{ "src/proto/", "src/proto.zig", "samples/dispatch.zig", "tools/checks/fixtures/" };

/// Returns the Zig files under paths (relative to root), or default_roots if empty.
/// The result uses '/' separators, is sorted by byte value, and contains no duplicates.
/// On error, failed.* names the path that could not be read.
pub fn collect(io: std.Io, arena: std.mem.Allocator, root: Dir, paths: []const []const u8, failed: *[]const u8) ![]const []const u8 {
    var files: std.ArrayList([]const u8) = .empty;
    for (if (paths.len == 0) &default_roots else paths) |path| {
        failed.* = path;
        if (path.len == 0 or Dir.path.isAbsolute(path) or Dir.path.diskDesignator(path).len != 0)
            return error.InvalidRelativePath;
        const relative = try Dir.path.resolveAlloc(arena, &.{path});
        if (Dir.path.sep != '/') std.mem.replaceScalar(u8, relative, Dir.path.sep, '/');
        if (std.mem.eql(u8, relative, "..") or std.mem.startsWith(u8, relative, "../"))
            return error.InvalidRelativePath;
        if (isExcluded(relative)) continue;
        failed.* = relative;
        switch ((try root.statFile(io, relative, .{})).kind) {
            .directory => try collectDir(io, arena, root, relative, failed, &files),
            .file => if (std.mem.endsWith(u8, relative, ".zig")) try files.append(arena, relative),
            else => return error.NotFileOrDirectory,
        }
    }

    std.mem.sortUnstable([]const u8, files.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
    var unique: usize = 0;
    for (files.items) |file| {
        if (unique != 0 and std.mem.eql(u8, files.items[unique - 1], file)) continue;
        files.items[unique] = file;
        unique += 1;
    }
    return files.items[0..unique];
}

fn isExcluded(path: []const u8) bool {
    for (excluded) |skip| {
        if (std.mem.eql(u8, path, skip)) return true;
        if (std.mem.endsWith(u8, skip, "/") and
            (std.mem.startsWith(u8, path, skip) or std.mem.eql(u8, path, skip[0 .. skip.len - 1]))) return true;
    }
    return false;
}

fn collectDir(io: std.Io, arena: std.mem.Allocator, root: Dir, path: []const u8, failed: *[]const u8, files: *std.ArrayList([]const u8)) !void {
    failed.* = path;
    const dir = try root.openDir(io, path, .{ .iterate = true });
    defer dir.close(io);
    const prefix = if (std.mem.eql(u8, path, ".")) "" else path;
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        switch (entry.kind) {
            .directory => if (std.mem.startsWith(u8, entry.name, ".") or
                std.mem.eql(u8, entry.name, "zig-out") or std.mem.eql(u8, entry.name, "zig-pkg")) continue,
            .file => if (!std.mem.endsWith(u8, entry.name, ".zig")) continue,
            else => continue,
        }
        const child = try std.mem.concat(arena, u8, &.{ prefix, if (prefix.len == 0) "" else "/", entry.name });
        if (isExcluded(child)) continue;
        if (entry.kind == .directory) {
            try collectDir(io, arena, root, child, failed, files);
            failed.* = path;
        } else {
            try files.append(arena, child);
        }
    }
}
