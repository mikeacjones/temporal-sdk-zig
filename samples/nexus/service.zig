//! The Nexus service contract: the service's name, its operations, and their input and output
//! types. Both sides import this file: the handler worker binds the operations to handlers,
//! and caller workflows call them by these definitions. It holds no handler code.

const std = @import("std");
const temporal = @import("temporal");
const nexus = temporal.nexus;

pub const EchoInput = struct {
    message: []const u8,
};

pub const EchoOutput = EchoInput;

/// A synchronous operation: the handler returns its output in the response.
pub const Echo = nexus.OperationDef("echo", EchoInput, EchoOutput);

pub const Language = enum { en, fr, de, es, tr };

pub const HelloInput = struct {
    name: []const u8,
    language: Language,
};

pub const HelloOutput = struct {
    message: []const u8,
};

/// A workflow-run operation: the handler starts a workflow, and the operation completes with
/// its result.
pub const SayHello = nexus.OperationDef("say-hello", HelloInput, HelloOutput);

pub const HelloService = nexus.ServiceDef("my-hello-service", .{ Echo, SayHello });

/// The ID of the workflow behind a say-hello operation, "hello-<language>-<name>" with the
/// name trimmed of whitespace, lowercased, and its spaces replaced by dashes. A business ID: operations for
/// the same name and language resolve to the same workflow. Allocated from `arena`.
pub fn helloWorkflowId(arena: std.mem.Allocator, input: HelloInput) error{OutOfMemory}![]const u8 {
    const name = try std.ascii.allocLowerString(arena, std.mem.trim(u8, input.name, &std.ascii.whitespace));
    std.mem.replaceScalar(u8, name, ' ', '-');
    return std.fmt.allocPrint(arena, "hello-{t}-{s}", .{ input.language, name });
}
