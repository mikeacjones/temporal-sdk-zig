//! The helper of `zig build alloc-failures` (see AGENTS.md): runs a function with each of its
//! allocations failing in turn and expects error.OutOfMemory from every call that saw a
//! failure. src/alloc_failure.zig and the fixtures under tools/checks/fixtures/alloc_failures/
//! import it as `alloc_failures`; build.zig gives each instance its known-findings file as
//! the anonymous import `alloc_failures_known`.

const std = @import("std");

/// One entry of tools/checks/alloc_failures.zon (or of a fixture .zon).
pub const Finding = struct { case: []const u8, bead: []const u8, reason: []const u8 };

/// The known findings this helper instance was built with (anonymous import set by build.zig).
pub const findings: []const Finding = @import("alloc_failures_known");

/// How the function under test treats memory.
pub const Memory = enum {
    /// It allocates into a caller's arena and frees nothing. Each call gets a fresh
    /// std.heap.ArenaAllocator over std.testing.allocator, with a std.testing.FailingAllocator
    /// between the function and that arena, so every allocation is failed once.
    arena,
    /// It frees everything it allocates. The case runs through std.testing.checkAllAllocationFailures
    /// over std.testing.allocator; `call` must return `!void`.
    owned,
};

/// The highest fail index tried before the case fails with error.FailIndexCapReached.
pub const max_fail_index: usize = 20_000;

/// Each case must see at least this many induced failures.
pub const min_induced_failures: usize = 2;

pub const Error = error{
    AllocationFailureMishandled,
    CallFailed,
    FailIndexCapReached,
    TooFewAllocations,
    StaleKnownFinding,
    UnknownFindingCase,
};

/// Runs `call` with allocations failing at index 0, 1, 2, ... until one call has no induced
/// failure. `call` is invoked as `@call(.auto, call, .{allocator} ++ args)`. Its return value
/// is discarded. Its error set may be wider than OutOfMemory.
pub fn run(case: []const u8, comptime memory: Memory, comptime call: anytype, args: anytype) Error!void {
    const known = knownFinding(case);
    const violated = switch (memory) {
        .arena => try runArena(case, known != null, call, args),
        .owned => try runOwned(case, known != null, call, args),
    };
    if (known) |finding| if (!violated) {
        report(case, "listed in the known-findings file for {s} but no longer fails; remove its entry", .{finding.bead});
        return error.StaleKnownFinding;
    };
}

/// Fails when a listed finding's case has no `test "<case>"` declaration in `source` (the
/// test root's own text, passed by @embedFile). Only a line that starts with the declaration
/// counts, so a case quoted in a comment or string does not.
pub fn expectFindingsHaveCases(source: []const u8) Error!void {
    for (findings) |finding| {
        if (!declaresTest(source, finding.case)) {
            report(finding.case, "listed in the known-findings file for {s}, but no case has that name", .{finding.bead});
            return error.UnknownFindingCase;
        }
    }
}

fn declaresTest(source: []const u8, case: []const u8) bool {
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        const rest = std.mem.trimStart(u8, line, " \t");
        if (!std.mem.startsWith(u8, rest, "test \"")) continue;
        const name = rest["test \"".len..];
        if (std.mem.startsWith(u8, name, case) and name.len > case.len and name[case.len] == '"') return true;
    }
    return false;
}

fn knownFinding(case: []const u8) ?Finding {
    for (findings) |finding| {
        if (std.mem.eql(u8, finding.case, case)) return finding;
    }
    return null;
}

/// Returns whether a call mishandled an induced failure; a listed case stops there quietly,
/// an unlisted one fails.
fn runArena(case: []const u8, listed: bool, comptime call: anytype, args: anytype) Error!bool {
    for (0..max_fail_index + 1) |fail_index| {
        // The arena sits under the failing allocator: on top, it would ask for a few large
        // chunks, and only those would fail.
        var scratch: std.heap.ArenaAllocator = .init(std.testing.allocator);
        defer scratch.deinit();
        var failing: std.testing.FailingAllocator = .init(scratch.allocator(), .{ .fail_index = fail_index });
        const result = @call(.auto, call, .{failing.allocator()} ++ args);
        if (!failing.has_induced_failure) {
            if (result) |_| {} else |err| {
                report(case, "fail index {d}: returned error.{s} with no induced failure; fix the case's input", .{ fail_index, @errorName(err) });
                return error.CallFailed;
            }
            if (fail_index < min_induced_failures) {
                report(case, "saw {d} induced failures; a case needs at least {d}", .{ fail_index, min_induced_failures });
                return error.TooFewAllocations;
            }
            return false;
        }
        if (result) |_| {
            if (listed) return true;
            report(case, "fail index {d}: succeeded instead of returning error.OutOfMemory", .{fail_index});
            return error.AllocationFailureMishandled;
        } else |err| if (@as(anyerror, err) != error.OutOfMemory) {
            if (listed) return true;
            report(case, "fail index {d}: returned error.{s} instead of error.OutOfMemory", .{ fail_index, @errorName(err) });
            return error.AllocationFailureMishandled;
        }
    }
    report(case, "reached fail index {d} with every call still failing; raise max_fail_index or shrink the input", .{max_fail_index});
    return error.FailIndexCapReached;
}

fn runOwned(case: []const u8, listed: bool, comptime call: anytype, args: anytype) Error!bool {
    var counting: std.testing.FailingAllocator = .init(std.testing.allocator, .{});
    @call(.auto, call, .{counting.allocator()} ++ args) catch |err| {
        report(case, "fail index {d}: returned error.{s} with no induced failure; fix the case's input", .{ counting.alloc_index, @errorName(err) });
        return error.CallFailed;
    };
    if (counting.alloc_index > max_fail_index) {
        report(case, "reached fail index {d} with every call still failing; raise max_fail_index or shrink the input", .{max_fail_index});
        return error.FailIndexCapReached;
    }
    if (counting.alloc_index < min_induced_failures) {
        report(case, "saw {d} induced failures; a case needs at least {d}", .{ counting.alloc_index, min_induced_failures });
        return error.TooFewAllocations;
    }
    std.testing.checkAllAllocationFailures(std.testing.allocator, call, args) catch |err| {
        if (listed) return true;
        report(case, "std.testing.checkAllAllocationFailures returned error.{s}", .{@errorName(err)});
        return error.AllocationFailureMishandled;
    };
    return false;
}

fn report(case: []const u8, comptime format: []const u8, args: anytype) void {
    std.debug.print("alloc-failures: {s}: " ++ format ++ "\n", .{case} ++ args);
}
