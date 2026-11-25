//! The caller side: runs a greet operation from client code, with no workflow around it.
//! The client starts the operation on the endpoint (StartNexusOperationExecution) and
//! long-polls the server for its outcome (PollNexusOperationExecution), both through
//! `Client.callService`. Payloads go through the default converter only: `callService`
//! applies no codecs.

const std = @import("std");
const temporal = @import("temporal");
const service = @import("service.zig");

const Client = temporal.client.Client;
const converter = temporal.converter;

pub const namespace = "nexus-standalone-activity-caller-namespace";
pub const endpoint = "nexus-standalone-activity-endpoint";

/// The deadline of each poll. The server answers a poll before it with the outcome, or
/// with none when its own long-poll interval ends first.
const poll_timeout: std.Io.Duration = .fromSeconds(30);
/// Polls before `greetResult` gives up, so a server that never closes the operation can't
/// keep the caller waiting forever. The operation's schedule-to-close timeout ends it long
/// before then.
const max_polls = 10;

pub const Error = error{
    /// The operation failed, timed out or was canceled; the server's failure is logged.
    OperationFailed,
    /// `max_polls` polls ended without an outcome.
    NoOutcome,
};

/// A started operation.
pub const Operation = struct {
    id: []const u8,
    run_id: []const u8,
};

/// Starts the greet operation `operation_id` on `endpoint_name` in the client's namespace.
/// The operation fails if it hasn't closed `schedule_to_close` after this call. Memory is
/// `arena`'s.
pub fn startGreet(
    client: *Client,
    arena: std.mem.Allocator,
    io: std.Io,
    endpoint_name: []const u8,
    operation_id: []const u8,
    input: service.GreetingInput,
    schedule_to_close: std.Io.Duration,
) !Operation {
    const response = try client.callService(.workflow, "StartNexusOperationExecution", arena, .{
        .namespace = client.namespace,
        .identity = client.identity,
        .request_id = try temporal.client.uuid(arena, io),
        .operation_id = operation_id,
        .endpoint = endpoint_name,
        .service = service.GreetingService.service_name,
        .operation = service.Greet.operation_name,
        .schedule_to_close_timeout = durationProto(schedule_to_close),
        .input = try converter.toPayload(arena, input),
    }, null);
    return .{ .id = operation_id, .run_id = response.run_id };
}

/// Waits for `operation` to close and returns its output. Memory is `arena`'s.
pub fn greetResult(client: *Client, arena: std.mem.Allocator, operation: Operation) !service.GreetingOutput {
    var poller = try client.withRpcOptions(.{ .timeout = poll_timeout });
    for (0..max_polls) |_| {
        const response = poller.callService(.workflow, "PollNexusOperationExecution", arena, .{
            .namespace = client.namespace,
            .operation_id = operation.id,
            .run_id = operation.run_id,
            .wait_stage = .NEXUS_OPERATION_WAIT_STAGE_CLOSED,
        }, null) catch |err| switch (err) {
            error.DeadlineExceeded => continue,
            else => |e| return e,
        };
        switch (response.outcome orelse continue) {
            .result => |payload| return converter.fromPayload(service.GreetingOutput, arena, payload),
            .failure => |failure| {
                std.log.err("Operation {s} failed: {s}", .{ operation.id, failure.message });
                return error.OperationFailed;
            },
        }
    }
    return error.NoOutcome;
}

fn durationProto(d: std.Io.Duration) temporal.proto.google.protobuf.Duration {
    return .{
        .seconds = @intCast(@divTrunc(d.nanoseconds, std.time.ns_per_s)),
        .nanos = @intCast(@rem(d.nanoseconds, std.time.ns_per_s)),
    };
}

/// Connects a client to `namespace`, with the rest of its options from the environment and
/// the Temporal config file (`temporal.client.loadDefaultOptions`). Close it with `close`.
pub fn connect(gpa: std.mem.Allocator, io: std.Io, arena: std.mem.Allocator) !Client {
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = namespace;
    return Client.connect(gpa, io, options);
}
