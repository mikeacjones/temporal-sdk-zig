//! The Nexus service contract shared by the handler and the caller: one operation, greet,
//! backed by a standalone activity. It holds no handler code.

const std = @import("std");
const temporal = @import("temporal");
const nexus = temporal.nexus;

pub const GreetingInput = struct {
    name: []const u8,
};

pub const GreetingOutput = struct {
    message: []const u8,
};

pub const Greet = nexus.OperationDef("greet", GreetingInput, GreetingOutput);

pub const GreetingService = nexus.ServiceDef("NexusGreetingService", .{Greet});

/// The ID of the activity behind a greet operation, "greeting-<name>". A business ID: while
/// the first activity for a name runs, a second operation for the same name fails
/// (`error.ActivityExecutionAlreadyStarted`, a non-retryable handler error), because the
/// handler sets no ID conflict policy. Allocated from `arena`.
pub fn greetingActivityId(arena: std.mem.Allocator, input: GreetingInput) error{OutOfMemory}![]const u8 {
    return std.fmt.allocPrint(arena, "greeting-{s}", .{input.name});
}
