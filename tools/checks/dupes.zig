const std = @import("std");
const sources = @import("sources.zig");
const Allocator = std.mem.Allocator;

const Location = struct { file: usize, line: usize };

const RankRange = struct {
    start: usize,
    end: usize,
};

const Candidate = struct {
    length: usize,
    ranks: RankRange,
    first_pos: usize,
    last_pos: usize,

    fn before(_: void, a: Candidate, b: Candidate) bool {
        if (a.length != b.length) return a.length > b.length;
        return a.first_pos < b.first_pos;
    }
};

pub fn main(init: std.process.Init) !void {
    run(init) catch |err| {
        std.debug.print("dupes: report failed: {t}\n", .{err});
        std.process.exit(1);
    };
}

fn run(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const root_path = args.next() orelse usage();
    var arg = args.next();
    var min_tokens: u32 = 100;
    if (arg != null and std.mem.eql(u8, arg.?, "--min-tokens")) {
        const value = args.next() orelse "";
        min_tokens = std.fmt.parseInt(u32, value, 10) catch badMinimum();
        if (min_tokens == 0) badMinimum();
        arg = args.next();
    }
    var paths: std.ArrayList([]const u8) = .empty;
    while (arg) |path| : (arg = args.next()) try paths.append(arena, path);

    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try scan(init.io, arena, root_path, min_tokens, paths.items, &stdout.interface);
}

fn scan(io: std.Io, arena: Allocator, root_path: []const u8, min_tokens: u32, paths: []const []const u8, out: *std.Io.Writer) !void {
    const root = std.Io.Dir.cwd().openDir(io, root_path, .{}) catch |err| cannotRead(root_path, err);
    defer root.close(io);
    var failed: []const u8 = ".";
    const files = sources.collect(io, arena, root, paths, &failed) catch |err| cannotRead(failed, err);

    var ids: std.ArrayList(usize) = .empty;
    var locations: std.ArrayList(Location) = .empty;
    var intern: std.EnumArray(std.zig.Token.Tag, std.StringHashMapUnmanaged(usize)) = .initFill(.empty);
    var next_id = files.len + 1;
    for (files, 0..) |file, file_index| {
        const source = root.readFileAllocOptions(io, file, arena, .limited(std.zig.max_src_size), .@"1", 0) catch |err| cannotRead(file, err);
        var tokenizer = std.zig.Tokenizer.init(source);
        var offset: usize = 0;
        var line: usize = 1;
        while (true) {
            const token = tokenizer.next();
            switch (token.tag) {
                .eof => break,
                .doc_comment, .container_doc_comment => continue,
                else => {},
            }
            const text = source[token.loc.start..token.loc.end];
            const entry = try intern.getPtr(token.tag).getOrPut(arena, text);
            if (!entry.found_existing) {
                entry.value_ptr.* = next_id;
                next_id += 1;
            }
            line += std.mem.countScalar(u8, source[offset..token.loc.start], '\n');
            offset = token.loc.start;
            try ids.append(arena, entry.value_ptr.*);
            try locations.append(arena, .{ .file = file_index, .line = line });
        }
        // Each file ends in a distinct token, so a shared prefix stops at the boundary.
        try ids.append(arena, file_index + 1);
        try locations.append(arena, .{ .file = file_index, .line = line });
    }

    const groups = try report(arena, files, ids.items, locations.items, min_tokens, out);
    try out.print("{d} groups of {d} or more tokens in {d} files\n", .{ groups, min_tokens, files.len });
    try out.flush();
}

fn report(arena: Allocator, files: []const []const u8, ids: []const usize, locations: []const Location, min_tokens: usize, out: *std.Io.Writer) !usize {
    if (min_tokens > ids.len / 2) return 0;
    const suffixes = try suffixArray(arena, ids);
    const lcp = try commonPrefixes(arena, ids, suffixes);
    const candidates = try findCandidates(arena, ids, suffixes, lcp, min_tokens);
    std.mem.sortUnstable(Candidate, candidates, {}, Candidate.before);

    // Coverage tracks the next uncovered token at or after each position.
    const coverage = try arena.alloc(usize, ids.len + 1);
    for (coverage, 0..) |*next, i| next.* = i;
    var copies: std.ArrayList(usize) = .empty;
    var positions: ?PositionIndex = null;
    var groups: usize = 0;
    for (candidates) |candidate| {
        const length = candidate.length;
        if (nextUncovered(coverage, candidate.first_pos) >= candidate.last_pos + length) continue;

        copies.clearRetainingCapacity();
        const ranks = candidate.ranks;
        if (ranks.end - ranks.start <= 128) {
            try copies.appendSlice(arena, suffixes[ranks.start..ranks.end]);
            std.mem.sortUnstable(usize, copies.items, {}, std.sort.asc(usize));
            var kept: usize = 0;
            var end: usize = 0;
            for (copies.items) |start| {
                if (start < end) continue;
                end = start + length;
                copies.items[kept] = start;
                kept += 1;
            }
            copies.shrinkRetainingCapacity(kept);
        } else {
            // Large overlapping intervals can contain quadratically many suffixes in
            // total. Jump to the next non-overlapping copy without sorting each interval.
            if (positions == null) positions = try PositionIndex.init(arena, suffixes);
            var start = candidate.first_pos;
            while (true) {
                try copies.append(arena, start);
                if (start + length > candidate.last_pos) break;
                start = positions.?.atOrAfter(ranks, start + length) orelse break;
            }
        }
        if (copies.items.len < 2) continue;
        for (copies.items) |start| {
            if (nextUncovered(coverage, start) < start + length) break;
        } else continue;

        groups += 1;
        try out.print("{d} tokens, {d} copies\n", .{ length, copies.items.len });
        for (copies.items) |start| {
            const end = start + length;
            const first = locations[start];
            try out.print("{s}:{d}-{d}\n", .{ files[first.file], first.line, locations[end - 1].line });
            var uncovered = nextUncovered(coverage, start);
            const after = nextUncovered(coverage, end);
            while (uncovered < end) {
                const next = nextUncovered(coverage, uncovered + 1);
                coverage[uncovered] = after;
                uncovered = next;
            }
        }
        try out.writeByte('\n');
    }
    return groups;
}

fn nextUncovered(coverage: []usize, start: usize) usize {
    var pos = start;
    while (coverage[pos] != pos) {
        coverage[pos] = coverage[coverage[pos]];
        pos = coverage[pos];
    }
    return pos;
}

// A wavelet matrix over the suffix array. Each layer stably partitions positions
// by one bit. Prefix counts map a range to its zero and one halves in the next layer.
const PositionIndex = struct {
    layers: []const Layer,

    const Layer = struct {
        ones: []const usize,
        zeros: usize,

        fn partition(self: Layer, range: RankRange) [2]RankRange {
            const before = self.ones[range.start];
            const through = self.ones[range.end];
            return .{
                .{ .start = range.start - before, .end = range.end - through },
                .{ .start = self.zeros + before, .end = self.zeros + through },
            };
        }
    };

    fn init(arena: Allocator, suffixes: []const usize) !PositionIndex {
        const bits = @bitSizeOf(usize) - @clz(suffixes.len - 1);
        const layers = try arena.alloc(Layer, bits);
        var current = try arena.dupe(usize, suffixes);
        var next = try arena.alloc(usize, suffixes.len);
        for (layers, 0..) |*layer, i| {
            const mask = @as(usize, 1) << @intCast(layers.len - i - 1);
            const ones = try arena.alloc(usize, suffixes.len + 1);
            ones[0] = 0;
            for (current, 0..) |pos, j| ones[j + 1] = ones[j] + @intFromBool(pos & mask != 0);
            const zeros = suffixes.len - ones[suffixes.len];
            layer.* = .{ .ones = ones, .zeros = zeros };
            for (current, 0..) |pos, j| {
                const dest = if (pos & mask == 0) j - ones[j] else zeros + ones[j];
                next[dest] = pos;
            }
            std.mem.swap([]usize, &current, &next);
        }
        return .{ .layers = layers };
    }

    fn atOrAfter(self: PositionIndex, ranks: RankRange, start: usize) ?usize {
        // Count positions below start to get the index of its successor in source order.
        var range = ranks;
        var index: usize = 0;
        for (self.layers, 0..) |layer, i| {
            const halves = layer.partition(range);
            const mask = @as(usize, 1) << @intCast(self.layers.len - i - 1);
            const bit = @intFromBool(start & mask != 0);
            if (bit == 1) index += halves[0].end - halves[0].start;
            range = halves[bit];
        }
        if (index == ranks.end - ranks.start) return null;

        range = ranks;
        var result: usize = 0;
        for (self.layers) |layer| {
            const halves = layer.partition(range);
            const zeros = halves[0].end - halves[0].start;
            const bit = @intFromBool(index >= zeros);
            if (bit == 1) index -= zeros;
            result = (result << 1) | bit;
            range = halves[bit];
        }
        return result;
    }
};

fn suffixArray(arena: Allocator, ids: []const usize) ![]usize {
    const suffixes = try arena.alloc(usize, ids.len);
    const by_tail = try arena.alloc(usize, ids.len);
    const counts = try arena.alloc(usize, ids.len + 1);
    var ranks = try arena.dupe(usize, ids);
    var next_ranks = try arena.alloc(usize, ids.len);
    for (by_tail, 0..) |*pos, i| pos.* = i;
    var width: usize = 0;
    var classes = std.mem.max(usize, ids);
    while (true) {
        // Stable counting sort by the first rank, using positions ordered by the second rank.
        const offsets = counts[0 .. classes + 1];
        @memset(offsets, 0);
        for (ranks) |rank| offsets[rank] += 1;
        var total: usize = 0;
        for (offsets) |*offset| {
            const count = offset.*;
            offset.* = total;
            total += count;
        }
        for (by_tail) |pos| {
            const offset = &offsets[ranks[pos]];
            suffixes[offset.*] = pos;
            offset.* += 1;
        }

        if (classes == ids.len) return suffixes;
        if (width != 0) {
            classes = 1;
            next_ranks[suffixes[0]] = classes;
            for (suffixes[0 .. suffixes.len - 1], suffixes[1..]) |a, b| {
                const a_tail = if (a + width < ids.len) ranks[a + width] else 0;
                const b_tail = if (b + width < ids.len) ranks[b + width] else 0;
                if (ranks[a] != ranks[b] or a_tail != b_tail) classes += 1;
                next_ranks[b] = classes;
            }
            if (classes == ids.len) return suffixes;
            std.mem.swap([]usize, &ranks, &next_ranks);
        }
        width = if (width == 0) 1 else 2 * width;
        // Suffixes with no second half have rank zero. For all others, the second
        // half is already in rank order in the suffix array from the previous pass.
        for (by_tail[0..width], ids.len - width..) |*pos, i| pos.* = i;
        var tail = width;
        for (suffixes) |pos| {
            if (pos < width) continue;
            by_tail[tail] = pos - width;
            tail += 1;
        }
    }
}

// The previous suffix's shared prefix gives a lower bound after dropping one token.
fn commonPrefixes(arena: Allocator, ids: []const usize, suffixes: []const usize) ![]usize {
    const rank = try arena.alloc(usize, ids.len);
    for (suffixes, 0..) |pos, i| rank[pos] = i;
    const lcp = try arena.alloc(usize, ids.len);
    lcp[0] = 0;
    var length: usize = 0;
    for (rank, 0..) |i, pos| {
        if (i == 0) {
            length = 0;
            continue;
        }
        const prev = suffixes[i - 1];
        while (pos + length < ids.len and prev + length < ids.len and ids[pos + length] == ids[prev + length]) length += 1;
        lcp[i] = length;
        length -|= 1;
    }
    return lcp;
}

const Interval = struct {
    length: usize = 0,
    first_rank: usize,
    first_pos: usize,
    last_pos: usize,
    common_left: ?usize,

    fn extend(self: *Interval, tail: Interval) void {
        self.first_pos = @min(self.first_pos, tail.first_pos);
        self.last_pos = @max(self.last_pos, tail.last_pos);
        if (self.common_left != tail.common_left) self.common_left = null;
    }
};

fn findCandidates(arena: Allocator, ids: []const usize, suffixes: []const usize, lcp: []const usize, min_tokens: usize) ![]Candidate {
    var active: std.ArrayList(Interval) = .empty;
    var candidates: std.ArrayList(Candidate) = .empty;
    for (suffixes, 1..) |pos, end| {
        const length = if (end == lcp.len) 0 else lcp[end];
        var tail: Interval = .{
            .first_rank = end - 1,
            .first_pos = pos,
            .last_pos = pos,
            .common_left = if (pos == 0) 0 else ids[pos - 1],
        };
        // A falling LCP closes each longer interval at its full occurrence set.
        while (active.getLastOrNull()) |last| {
            if (last.length <= length) break;
            var interval = active.pop().?;
            interval.extend(tail);
            if (interval.common_left == null and interval.last_pos - interval.first_pos >= interval.length) {
                try candidates.append(arena, .{
                    .length = interval.length,
                    .ranks = .{ .start = interval.first_rank, .end = end },
                    .first_pos = interval.first_pos,
                    .last_pos = interval.last_pos,
                });
            }
            tail = interval;
        }
        if (length < min_tokens) continue;
        if (active.items.len != 0 and active.items[active.items.len - 1].length == length) {
            active.items[active.items.len - 1].extend(tail);
        } else {
            tail.length = length;
            try active.append(arena, tail);
        }
    }
    return candidates.items;
}

fn cannotRead(path: []const u8, err: anyerror) noreturn {
    std.debug.print("dupes: cannot read {s}: {t}\n", .{ path, err });
    std.process.exit(1);
}

fn badMinimum() noreturn {
    std.debug.print("dupes: --min-tokens must be a positive integer\n", .{});
    usage();
}

fn usage() noreturn {
    std.debug.print("dupes: usage: dupes <root dir> [--min-tokens <n>] [<path>...]\n", .{});
    std.process.exit(2);
}

test "large overlapping occurrence sets retain only the leftmost disjoint copies" {
    var memory = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer memory.deinit();
    const arena = memory.allocator();
    const file_len = 514;
    var ids: [2 * file_len]usize = undefined;
    @memset(&ids, 3);
    ids[0] = 4;
    ids[file_len - 1] = 1;
    ids[file_len] = 5;
    ids[ids.len - 1] = 2;

    var locations: [ids.len]Location = undefined;
    for (&locations, 0..) |*location, i| {
        location.* = .{ .file = i / file_len, .line = i % file_len + 1 };
    }
    var out: std.Io.Writer.Allocating = .init(arena);
    const groups = try report(arena, &.{ "a.zig", "b.zig" }, &ids, &locations, 1, &out.writer);
    try std.testing.expectEqual(1, groups);
    try std.testing.expectEqualStrings("512 tokens, 2 copies\na.zig:2-513\nb.zig:2-513\n\n", out.written());
}
