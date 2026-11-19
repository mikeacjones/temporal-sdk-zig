//! hang-report-check <within seconds> <expected text>... -- <program> [<argument>...]
//!
//! Runs a program whose test hangs and checks that the test runner's hang report
//! (tools/test_runner/hang_report.zig) names it without ending the process: every expected
//! text must appear on the program's stderr within <within> seconds, and the program must still
//! be running 1 s after that. The program is then killed.
//!
//! Exit 0: every text was seen and the program was still running. Exit 1: one line naming the
//! problem, then the captured stderr. Exit 2: usage.

const std = @import("std");
const Io = std.Io;

const usage_text = "hang-report-check: usage: hang-report-check <within seconds> <expected text>... -- <program> [<argument>...]\n";

/// How long the program must go on running after the last expected text.
const confirm_seconds = 1;

const Args = struct {
    within: u32,
    expected: []const []const u8,
    argv: []const []const u8,
};

/// Parses the arguments after the program name, or returns error.Usage.
fn parseArgs(args: []const []const u8) error{Usage}!Args {
    if (args.len == 0) return error.Usage;
    const within = std.fmt.parseUnsigned(u32, args[0], 10) catch return error.Usage;
    if (within == 0) return error.Usage;
    const rest = args[1..];
    const dash = for (rest, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--")) break i;
    } else return error.Usage;
    const expected = rest[0..dash];
    const argv = rest[dash + 1 ..];
    if (expected.len == 0 or argv.len == 0) return error.Usage;
    for (expected) |text| if (text.len == 0) return error.Usage;
    return .{ .within = within, .expected = expected, .argv = argv };
}

/// The first expected text that `output` does not contain.
fn firstMissing(output: []const u8, expected: []const []const u8) ?[]const u8 {
    for (expected) |text| {
        if (std.mem.find(u8, output, text) == null) return text;
    }
    return null;
}

pub fn main(init: std.process.Init) void {
    const arena = init.arena.allocator();
    const all = init.minimal.args.toSlice(arena) catch {
        std.debug.print("hang-report-check: cannot read the arguments\n", .{});
        std.process.exit(1);
    };
    const args = parseArgs(if (all.len > 0) all[1..] else all) catch {
        std.debug.print(usage_text, .{});
        std.process.exit(2);
    };
    std.process.exit(check(init.io, arena, args));
}

fn check(io: Io, arena: std.mem.Allocator, args: Args) u8 {
    const program = args.argv[0];
    var child = std.process.spawn(io, .{
        .argv = args.argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .pipe,
    }) catch |err| {
        std.debug.print("hang-report-check: cannot start {s}: {t}\n", .{ program, err });
        return 1;
    };
    // SIGKILL first, to the child this handle owns (not yet reaped, so its pid cannot have been
    // reused): kill sends SIGTERM and then waits, and the wait must not depend on the program.
    defer {
        if (child.id) |pid| std.posix.kill(pid, .KILL) catch {};
        child.kill(io);
    }

    var buffer: Io.File.MultiReader.Buffer(1) = undefined;
    var reader: Io.File.MultiReader = undefined;
    reader.init(arena, io, buffer.toStreams(), &.{child.stderr.?});
    defer reader.deinit();
    const stderr = reader.reader(0);

    const within: Io.Timeout = (Io.Timeout{ .duration = .{ .raw = .fromSeconds(args.within), .clock = .awake } }).toDeadline(io);
    while (firstMissing(stderr.buffered(), args.expected)) |missing| {
        switch (fillUntil(io, &reader, within)) {
            .more => {},
            .timeout => return fail(program, stderr.buffered(), "printed no '{s}' within {d}s", .{ missing, args.within }),
            .ended => return fail(program, stderr.buffered(), "exited ({f}) before printing '{s}'", .{ stop(io, &child), missing }),
            .failed => |err| return fail(program, stderr.buffered(), "cannot read its stderr: {t}", .{err}),
        }
    }

    const confirm: Io.Timeout = (Io.Timeout{ .duration = .{ .raw = .fromSeconds(confirm_seconds), .clock = .awake } }).toDeadline(io);
    while (true) switch (fillUntil(io, &reader, confirm)) {
        .more => {},
        .timeout => return 0,
        .ended => return fail(program, stderr.buffered(), "ended within {d}s of printing the report ({f})", .{ confirm_seconds, stop(io, &child) }),
        .failed => |err| return fail(program, stderr.buffered(), "cannot read its stderr: {t}", .{err}),
    };
}

const Fill = union(enum) { more, timeout, ended, failed: anyerror };

fn fillUntil(io: Io, reader: *Io.File.MultiReader, deadline: Io.Timeout) Fill {
    reader.fill(4096, deadline) catch |err| switch (err) {
        error.Timeout => return .timeout,
        error.EndOfStream => {
            reader.checkAnyError() catch |e| return .{ .failed = e };
            return .ended;
        },
        else => |e| return .{ .failed = e },
    };
    // A pipe that is always readable can keep poll from returning Timeout.
    if (deadline.deadline.durationFromNow(io).raw.nanoseconds <= 0) return .timeout;
    return .more;
}

/// The program closed its stderr: it has exited or is exiting. Reaps it and returns how it
/// ended. A program that closed stderr and goes on running is killed, and shows as SIGKILL.
fn stop(io: Io, child: *std.process.Child) std.process.Child.Term {
    const pid = child.id orelse return .{ .unknown = 0 };
    std.posix.kill(pid, .KILL) catch {};
    return child.wait(io) catch .{ .unknown = 0 };
}

fn fail(program: []const u8, output: []const u8, comptime format: []const u8, args: anytype) u8 {
    std.debug.print("hang-report-check: {s} " ++ format ++ "\n--- stderr of {s} ---\n{s}", .{program} ++ args ++ .{ program, output });
    return 1;
}

test "parseArgs splits the limit, the texts and the command" {
    const args = try parseArgs(&.{ "60", "still running after", "waitsForever", "--", "./fixture", "--hang-report=2" });
    try std.testing.expectEqual(@as(u32, 60), args.within);
    try std.testing.expectEqual(@as(usize, 2), args.expected.len);
    try std.testing.expectEqualStrings("waitsForever", args.expected[1]);
    try std.testing.expectEqual(@as(usize, 2), args.argv.len);
    try std.testing.expectEqualStrings("./fixture", args.argv[0]);
    try std.testing.expectEqualStrings("--hang-report=2", args.argv[1]);
}

test "parseArgs keeps a second -- as an argument of the program" {
    const args = try parseArgs(&.{ "5", "x", "--", "prog", "--", "y" });
    try std.testing.expectEqual(@as(usize, 3), args.argv.len);
    try std.testing.expectEqualStrings("--", args.argv[1]);
}

test "parseArgs rejects a missing or bad part" {
    const bad = [_][]const []const u8{
        &.{},
        &.{"60"},
        &.{ "60", "--", "prog" },
        &.{ "60", "text", "--" },
        &.{ "60", "text", "prog" },
        &.{ "sixty", "text", "--", "prog" },
        &.{ "0", "text", "--", "prog" },
        &.{ "-1", "text", "--", "prog" },
        &.{ "60", "", "--", "prog" },
    };
    for (bad) |args| try std.testing.expectError(error.Usage, parseArgs(args));
}

test "firstMissing names the first text the output lacks" {
    const gpa = std.testing.allocator;
    const output = try std.mem.concat(gpa, u8, &.{ "test-hang-report: 'a' is still running after 2s\n", "in waitsForever\n" });
    defer gpa.free(output);
    try std.testing.expectEqual(@as(?[]const u8, null), firstMissing(output, &.{ "still running after", "waitsForever" }));
    try std.testing.expectEqualStrings("helperWaitsForever", firstMissing(output, &.{ "waitsForever", "helperWaitsForever" }).?);
}
