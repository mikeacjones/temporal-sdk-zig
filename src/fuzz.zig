//! Register each target in `targets`, give it a `test "fuzz <target>"`, and put its
//! `testOne` in fuzz/<target>.zig. The build embeds the files in fuzz/<target>/.
//!
//! Corpus files are Smith input streams, in the order the target draws values:
//! integers, enums, bools and indices use 8 little-endian bytes, eos uses one byte,
//! bytes are literal, and a slice is a 4-byte little-endian length followed by its bytes.
//! Fit seeds to the target's buffers. An oversized slice replays as an empty slice,
//! and a stream that runs out yields defaults.
//!
//! A crash file `zig build fuzz` saves is exactly the stream the target consumed, so a
//! full-length slice adds 4 bytes of framing to its `max_input_len` bytes. The limits are in
//! fuzz/limits.zig, which build.zig shares. A target draws slices of at most `max_input_len`
//! bytes in total, plus at most `max_framing_len` bytes of lengths, fixed-size values and
//! eos bytes. So any stream it consumes, a saved crash file included, is at most
//! `max_file_len` bytes, the per-file limit build.zig enforces. All corpus files together
//! are at most `max_corpus_len` bytes. A target that would need more must shrink its buffers.

const std = @import("std");
const corpus = @import("fuzz_corpus");
const limits = @import("fuzz/limits.zig");

const targets = struct {
    pub const failure_attributes = @import("fuzz/failure_attributes.zig");
    pub const history_json = @import("fuzz/history_json.zig");
    pub const nexus_links = @import("fuzz/nexus_links.zig");
    pub const payload = @import("fuzz/payload.zig");
    pub const protojson = @import("fuzz/protojson.zig");
    pub const search_attributes = @import("fuzz/search_attributes.zig");
    pub const time_parse = @import("fuzz/time_parse.zig");
};

const Finding = struct { target: []const u8, bead: []const u8, reason: []const u8 };
const findings: []const Finding = @import("fuzz_known");

/// The most slice bytes a target draws from one stream.
pub const max_input_len = limits.max_input_len;

/// The most bytes of slice lengths, fixed-size values and eos bytes a target draws
/// besides its slice bytes.
pub const max_framing_len = limits.max_framing_len;

/// The largest corpus file build.zig accepts: `max_input_len` plus `max_framing_len`.
pub const max_file_len = limits.max_file_len;

/// Marks the place where target stops on an input that meets the open bug bead.
/// The pair must be listed in tools/checks/fuzz.zon.
pub fn known(comptime target: []const u8, comptime bead: []const u8) void {
    // A `return` inside a comptime block would end this runtime call at comptime,
    // which Zig rejects; so the lookup is a function evaluated at comptime.
    if (comptime !isListed(target, bead))
        @compileError(target ++ " guard for " ++ bead ++ " is missing from tools/checks/fuzz.zon");
}

fn isListed(target: []const u8, bead: []const u8) bool {
    for (findings) |finding| {
        if (std.mem.eql(u8, target, finding.target) and std.mem.eql(u8, bead, finding.bead)) return true;
    }
    return false;
}

comptime {
    for (std.meta.declarations(corpus)) |name| {
        if (!@hasDecl(targets, name)) @compileError("unregistered fuzz target: " ++ name);
    }
}

fn fuzzTarget(comptime name: []const u8) !void {
    const seeds = &@field(corpus, name);
    if (seeds.len < 3) @compileError("fuzz target " ++ name ++ " needs at least three corpus files");
    try std.testing.fuzz({}, @field(targets, name).testOne, .{ .corpus = seeds });
}

test "fuzz failure_attributes" {
    try fuzzTarget("failure_attributes");
}

test "fuzz history_json" {
    try fuzzTarget("history_json");
}

test "fuzz nexus_links" {
    try fuzzTarget("nexus_links");
}

test "fuzz payload" {
    try fuzzTarget("payload");
}

test "fuzz protojson" {
    try fuzzTarget("protojson");
}

test "fuzz search_attributes" {
    try fuzzTarget("search_attributes");
}

test "fuzz time_parse" {
    try fuzzTarget("time_parse");
}

test "fuzz: a stream with full-length slices fits max_file_len and replays whole" {
    const text = comptime text: {
        var bytes: [max_input_len]u8 = @splat(' ');
        bytes[0] = '[';
        bytes[bytes.len - 1] = ']';
        break :text bytes;
    };
    const length = std.mem.toBytes(std.mem.nativeToLittle(u32, max_input_len));
    const payloads = std.mem.toBytes(std.mem.nativeToLittle(u64, 1));
    const streams = .{
        .{ targets.history_json, length ++ text },
        .{ targets.protojson, payloads ++ length ++ text },
    };
    inline for (streams) |case| {
        const stream: []const u8 = &case[1];
        try std.testing.expect(stream.len <= max_file_len);
        var smith: std.testing.Smith = .{ .in = stream };
        try case[0].testOne({}, &smith);
        try std.testing.expectEqual(@as(usize, 0), smith.in.?.len);
        // The slice is taken whole, not replayed as an oversized empty one.
        var buffer: [max_input_len]u8 = undefined;
        var slices: std.testing.Smith = .{ .in = stream[stream.len - max_input_len - 4 ..] };
        try std.testing.expectEqual(@as(u32, max_input_len), slices.slice(&buffer));
    }
}

test "every known finding has a guard in its target" {
    inline for (findings) |finding| {
        const source = @embedFile("fuzz/" ++ finding.target ++ ".zig");
        if (std.mem.find(u8, source, "\"" ++ finding.bead ++ "\"") == null) {
            std.debug.print("fuzz: {s} is missing its guard for {s}\n", .{ finding.target, finding.bead });
            return error.MissingFuzzGuard;
        }
    }
}

test "known compiles as a guard for every listed finding" {
    inline for (findings) |finding| try std.testing.expectEqual({}, guardFor(finding.target, finding.bead, true));
}

fn guardFor(comptime target: []const u8, comptime bead: []const u8, met: bool) void {
    if (met) return known(target, bead);
}
