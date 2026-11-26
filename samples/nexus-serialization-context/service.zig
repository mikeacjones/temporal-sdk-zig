//! The Nexus service contract, and the names both sides share: the namespaces and task queues
//! of the three workers, and the four endpoints the caller calls through. Two endpoints route
//! to each handler worker, so a handler sees which one a request came through
//! (`converter.Context.nexus`) and its codec picks that endpoint's key.

const temporal = @import("temporal");
const nexus = temporal.nexus;

pub const EchoInput = struct {
    message: []const u8,
};

pub const EchoOutput = EchoInput;

/// A synchronous operation: the handler returns its input in the response.
pub const Echo = nexus.OperationDef("echo", EchoInput, EchoOutput);

/// A workflow-run operation: the handler starts a workflow that returns its input, and the
/// operation completes with that workflow's result.
pub const EchoAsync = nexus.OperationDef("echo-async", EchoInput, EchoOutput);

pub const EchoService = nexus.ServiceDef("per-endpoint-encryption-service", .{ Echo, EchoAsync });

/// Each worker polls a task queue named like its namespace.
pub const caller_namespace = "nexus-serialization-caller";
pub const compressed_encrypted_namespace = "nexus-serialization-compressed-encrypted-handler";
pub const encrypted_namespace = "nexus-serialization-encrypted-handler";

/// Routes to the compressed-encrypted handler worker, which compresses and then encrypts.
pub const compressed_encrypted_1 = "nexus-serialization-compressed-encrypted-1";
pub const compressed_encrypted_2 = "nexus-serialization-compressed-encrypted-2";
/// Routes to the encrypted handler worker, which encrypts without compressing.
pub const encrypted_1 = "nexus-serialization-encrypted-1";
pub const encrypted_2 = "nexus-serialization-encrypted-2";

/// Every endpoint, in the order the caller workflow reports its replies.
pub const endpoints = [_][]const u8{ compressed_encrypted_1, compressed_encrypted_2, encrypted_1, encrypted_2 };

/// What the two operations called through one endpoint returned.
pub const EndpointReplies = struct {
    endpoint: []const u8,
    sync_message: []const u8,
    async_message: []const u8,
};
