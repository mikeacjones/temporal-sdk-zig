//! Exports committed SDK sources and compares public-name lists.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const RefOption = enum { base, head };

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    var args = std.process.Args.Iterator.initAllocator(init.minimal.args, arena) catch |err| {
        std.debug.print("api-diff: cannot read arguments: {t}\n", .{err});
        std.process.exit(1);
    };
    _ = args.skip();
    const command = args.next() orelse usage();
    if (std.mem.eql(u8, command, "export")) {
        const repo = args.next() orelse usage();
        const option = std.meta.stringToEnum(RefOption, args.next() orelse usage()) orelse usage();
        const ref = args.next() orelse usage();
        const out = args.next() orelse usage();
        if (args.next() != null) usage();
        const export_tree: Export = .{ .io = init.io, .arena = arena, .repo = repo, .option = option, .ref = ref };
        export_tree.writeTree(out) catch std.process.exit(1);
    } else if (std.mem.eql(u8, command, "compare")) {
        const base = args.next() orelse usage();
        const head = args.next() orelse usage();
        if (args.next() != null) usage();
        compare(init.io, arena, base, head) catch |err| {
            std.debug.print("api-diff: cannot report public-name differences: {t}\n", .{err});
            std.process.exit(1);
        };
    } else usage();
}

fn usage() noreturn {
    std.debug.print(
        \\api-diff: usage: api_diff export <repo dir> <base|head> <ref> <out dir>
        \\api-diff:        api_diff compare <base list> <head list>
        \\
    , .{});
    std.process.exit(2);
}

const Export = struct {
    io: Io,
    arena: Allocator,
    repo: []const u8,
    option: RefOption,
    ref: []const u8,

    const Reported = error{Reported};

    fn writeTree(self: Export, out: []const u8) Reported!void {
        const io = self.io;
        const cwd = Io.Dir.cwd();
        const lock_path = self.arena.print("{s}.lock", .{out}) catch |err|
            return self.report("cannot name export lock: {t}", .{err});
        if (Io.Dir.path.dirname(lock_path)) |parent| {
            cwd.createDirPath(io, parent) catch |err|
                return self.report("cannot create export parent directory: {t}", .{err});
        }
        const lock = cwd.createFile(io, lock_path, .{ .truncate = false, .lock = .exclusive }) catch |err|
            return self.report("cannot lock export directory: {t}", .{err});
        defer lock.close(io);
        errdefer {
            cwd.deleteTree(io, out) catch {};
            cwd.createDirPath(io, out) catch {};
        }

        const probe = try self.git(&.{ "rev-parse", "--git-dir" });
        if (!succeeded(probe.term)) return self.report("{s} is not in a git repository", .{self.repo});

        const revision = self.arena.print("{s}^{{commit}}", .{self.ref}) catch |err|
            return self.report("cannot form git revision: {t}", .{err});
        const resolved = try self.git(&.{ "rev-parse", "--verify", "--quiet", "--end-of-options", revision });
        if (!succeeded(resolved.term)) return self.report("not a commit in this repository", .{});
        const sha = std.mem.trimEnd(u8, resolved.stdout, "\r\n");

        // Another build may be compiling this export. Leave a complete copy of the same commit in place.
        if (cwd.openDir(io, out, .{})) |dir| {
            defer dir.close(io);
            const previous = dir.readFileAlloc(io, ".commit", self.arena, .limited(128)) catch "";
            if (std.mem.eql(u8, previous, sha)) return;
        } else |_| {}

        cwd.deleteTree(io, out) catch |err| return self.report("cannot empty export directory: {t}", .{err});
        const dir = cwd.createDirPathOpen(io, out, .{}) catch |err|
            return self.report("cannot create export directory: {t}", .{err});
        defer dir.close(io);

        const archive = try self.git(&.{ "archive", "--format=tar", sha, "--", "src" });
        if (!succeeded(archive.term)) {
            const first_line, _ = std.mem.cutScalar(u8, archive.stderr, '\n') orelse .{ archive.stderr, "" };
            return self.report("git archive failed: {s}", .{std.mem.trimEnd(u8, first_line, "\r")});
        }
        var reader = Io.Reader.fixed(archive.stdout);
        std.tar.extract(io, dir, &reader, .{}) catch |err|
            return self.report("cannot extract src: {t}", .{err});
        dir.writeFile(io, .{ .sub_path = ".commit", .data = sha }) catch |err|
            return self.report("cannot record exported commit: {t}", .{err});
    }

    fn git(self: Export, args: []const []const u8) Reported!std.process.RunResult {
        const argv = self.arena.alloc([]const u8, 3 + args.len) catch |err|
            return self.report("cannot run git: {t}", .{err});
        argv[0..3].* = .{ "git", "-C", self.repo };
        @memcpy(argv[3..], args);
        return std.process.run(self.arena, self.io, .{ .argv = argv }) catch |err|
            return self.report("cannot run git: {t}", .{err});
    }

    fn report(self: Export, comptime format: []const u8, args: anytype) Reported {
        std.debug.print("api-diff: -D{t}={s}: " ++ format ++ "\n", .{ self.option, self.ref } ++ args);
        return error.Reported;
    }
};

fn succeeded(term: std.process.Child.Term) bool {
    return term == .exited and term.exited == 0;
}

fn compare(io: Io, arena: Allocator, base: []const u8, head: []const u8) !void {
    const Presence = enum { base, head, both };
    var names: std.StringArrayHashMapUnmanaged(Presence) = .empty;
    for ([_][]const u8{ base, head }, [_]Presence{ .base, .head }) |path, side| {
        const contents = Io.Dir.cwd().readFileAlloc(io, path, arena, .unlimited) catch |err| {
            std.debug.print("api-diff: cannot read {s}: {t}\n", .{ path, err });
            std.process.exit(1);
        };
        var lines = std.mem.tokenizeScalar(u8, contents, '\n');
        while (lines.next()) |name| {
            const entry = try names.getOrPut(arena, name);
            if (!entry.found_existing) {
                entry.value_ptr.* = side;
            } else if (entry.value_ptr.* != side) {
                entry.value_ptr.* = .both;
            }
        }
    }
    const ByName = struct {
        keys: []const []const u8,

        pub fn lessThan(self: @This(), a: usize, b: usize) bool {
            return std.mem.lessThan(u8, self.keys[a], self.keys[b]);
        }
    };
    names.sortUnstable(ByName{ .keys = names.keys() });

    var buffer: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    for (names.keys(), names.values()) |name, presence| {
        const sign: u8 = switch (presence) {
            .base => '-',
            .head => '+',
            .both => continue,
        };
        try writer.print("{c} {s}\n", .{ sign, name });
    }
    try writer.flush();
}
