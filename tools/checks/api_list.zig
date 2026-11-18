//! Lists the public names reachable through the build's api_root module.

const std = @import("std");
const Root = @import("api_root");
const options = @import("api_options");

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.initAllocator(init.minimal.args, init.arena.allocator()) catch |err| {
        std.debug.print("api: cannot read arguments: {t}\n", .{err});
        std.process.exit(1);
    };
    _ = args.skip();
    if (args.next() != null) {
        std.debug.print("api: takes no arguments\n", .{});
        std.process.exit(2);
    }

    writeList(init.io, init.arena.allocator()) catch |err| {
        std.debug.print("api: cannot write public names: {t}\n", .{err});
        std.process.exit(1);
    };
}

fn writeList(io: std.Io, arena: std.mem.Allocator) !void {
    const paths = try arena.alloc([]const u8, names.len);
    // The walk records each parent before its children.
    for (names, paths) |name, *path| {
        const parent = if (name.parent) |index| paths[index] else options.root_name;
        path.* = try arena.print("{s}.{f}", .{ parent, std.zig.fmtId(name.identifier) });
    }
    std.mem.sortUnstable([]const u8, paths, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &buffer);
    const writer = &stdout.interface;
    for (paths, 0..) |name, i| {
        if (i != 0 and std.mem.eql(u8, paths[i - 1], name)) continue;
        try writer.print("{s}\n", .{name});
    }
    try writer.flush();
}

const initial_branch_quota = 1_000_000;
const names = names: {
    @setEvalBranchQuota(initial_branch_quota);
    var skipped: [options.skip.len]?type = undefined;
    for (options.skip, &skipped) |path, *ty| ty.* = skipType(Root, path);

    var listing: Listing = .{};
    walkDecls(&listing, Root, null, &.{Root}, &skipped, @typeInfo(Root).@"struct".decl_names);
    var result: [listing.len]Name = undefined;
    var entry = listing.head;
    var index = result.len;
    while (entry) |node| {
        index -= 1;
        result[index] = node.name;
        entry = node.next;
    }
    break :names result;
};

const Name = struct { parent: ?usize, identifier: []const u8 };

// Build the list at comptime, where reflection can access declaration values that are types.
const Listing = struct {
    const Entry = struct { name: Name, next: ?*const Entry };

    head: ?*const Entry = null,
    len: usize = 0,

    fn add(listing: *Listing, parent: ?usize, identifier: []const u8) usize {
        const index = listing.len;
        // Grant more reflection work as the walk discovers names, including aliases.
        @setEvalBranchQuota(initial_branch_quota +| @as(u32, @intCast(index)) *| 64);
        const entry: Entry = .{ .name = .{ .parent = parent, .identifier = identifier }, .next = listing.head };
        listing.head = &entry;
        listing.len += 1;
        return index;
    }
};

fn skipType(comptime T: type, path: []const u8) ?type {
    switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => {},
        else => return null,
    }
    const name, const rest = std.mem.cutScalar(u8, path, '.') orelse .{ path, "" };
    if (!@hasDecl(T, name) or @TypeOf(@field(T, name)) != type) return null;
    const child = @field(T, name);
    return if (rest.len == 0) child else skipType(child, rest);
}

fn walkDecls(listing: *Listing, comptime T: type, parent: ?usize, ancestors: []const type, skipped: []const ?type, decl_names: []const [:0]const u8) void {
    for (decl_names) |name| {
        const child = listing.add(parent, name);
        if (@TypeOf(@field(T, name)) == type) {
            walkType(listing, @field(T, name), child, ancestors, skipped);
        }
    }
}

fn walkType(listing: *Listing, comptime T: type, parent: usize, ancestors: []const type, skipped: []const ?type) void {
    for (skipped) |ty| if (ty == T) return;
    // Stop cycles along this path. Aliases on other paths still expand the same type.
    for (ancestors) |ty| if (ty == T) return;

    switch (@typeInfo(T)) {
        inline .@"struct", .@"union", .@"enum" => |info| {
            for (info.field_names) |name| _ = listing.add(parent, name);
            walkDecls(listing, T, parent, ancestors ++ &[_]type{T}, skipped, info.decl_names);
        },
        .@"opaque" => |info| walkDecls(listing, T, parent, ancestors ++ &[_]type{T}, skipped, info.decl_names),
        .error_set => |info| for (info.error_names orelse &.{}) |name| {
            _ = listing.add(parent, name);
        },
        else => {},
    }
}
