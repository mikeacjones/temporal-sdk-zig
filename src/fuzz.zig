//! Register each target in `targets`, give it a `test "fuzz <target>"`, and put its
//! `testOne` in fuzz/<target>.zig. The build embeds the files in fuzz/<target>/.
//!
//! Corpus files are Smith input streams, in the order the target draws values:
//! integers, enums, bools and indices use 8 little-endian bytes, eos uses one byte,
//! bytes are literal, and a slice is a 4-byte little-endian length followed by its bytes.
//! Fit seeds to the target's buffers. An oversized slice replays as an empty slice,
//! and a stream that runs out yields defaults.

const std = @import("std");
const corpus = @import("fuzz_corpus");

const targets = struct {
    pub const history_json = @import("fuzz/history_json.zig");
    pub const payload = @import("fuzz/payload.zig");
    pub const protojson = @import("fuzz/protojson.zig");
    pub const time_parse = @import("fuzz/time_parse.zig");
};

const Finding = struct { target: []const u8, bead: []const u8, reason: []const u8 };
const findings: []const Finding = @import("fuzz_known");

/// The longest input a target reads, and the largest corpus file build.zig accepts.
pub const max_input_len = 8192;

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

test "fuzz history_json" {
    try fuzzTarget("history_json");
}

test "fuzz payload" {
    try fuzzTarget("payload");
}

test "fuzz protojson" {
    try fuzzTarget("protojson");
}

test "fuzz time_parse" {
    try fuzzTarget("time_parse");
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
