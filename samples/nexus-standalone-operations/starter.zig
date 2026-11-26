//! Runs Nexus operations directly from the client, with no workflow around them, in namespace
//! my-standalone-operations-caller-namespace: the synchronous echo operation, then the
//! workflow-backed say-hello one, through the endpoint my-standalone-operations-endpoint.
//! Then lists and counts the standalone Nexus operations called through that endpoint.

const std = @import("std");
const temporal = @import("temporal");
const nexus = @import("nexus");
const names = @import("names.zig");

const service = nexus.service;
const query = "Endpoint = '" ++ names.endpoint ++ "'";

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    var options = try temporal.client.loadDefaultOptions(arena);
    options.namespace = names.caller_namespace;
    var client = temporal.client.Client.connect(gpa, io, options) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ names.caller_namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    // The synchronous echo operation. The endpoint must exist on the server (see worker.zig).
    var echo = client.executeNexusOperation(service.HelloService, service.Echo, .{
        .id = "nexus-standalone-echo-op",
        .endpoint = names.endpoint,
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{ .message = "hello" }) catch |err| {
        std.log.err("Unable to execute Echo operation: {t}", .{err});
        std.process.exit(1);
    };
    defer echo.deinit();
    std.log.info("Started Echo operation OperationID={s}", .{echo.id});

    const echo_result = echo.get(arena) catch |err| {
        std.log.err("Unable to get Echo operation result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Echo result: {s}", .{echo_result.message});

    // The asynchronous say-hello operation, which completes with its workflow's result.
    var hello = client.executeNexusOperation(service.HelloService, service.SayHello, .{
        .id = "nexus-standalone-hello-op",
        .endpoint = names.endpoint,
        .schedule_to_close_timeout = .fromSeconds(10),
    }, .{ .name = "Temporal", .language = .en }) catch |err| {
        std.log.err("Unable to execute Hello operation: {t}", .{err});
        std.process.exit(1);
    };
    defer hello.deinit();
    std.log.info("Started Hello operation OperationID={s}", .{hello.id});

    const hello_result = hello.get(arena) catch |err| {
        std.log.err("Unable to get Hello operation result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Hello result: {s}", .{hello_result.message});

    // Listing and counting are on the client. Visibility is eventually consistent, so an
    // operation that just closed may not be listed or counted yet.
    var it = client.listNexusOperations(query, .{}) catch |err| {
        std.log.err("Unable to list Nexus operations: {t}", .{err});
        std.process.exit(1);
    };
    defer it.deinit();
    std.log.info("ListNexusOperations results", .{});
    while (it.next(arena) catch |err| {
        std.log.err("Unable to list Nexus operations: {t}", .{err});
        std.process.exit(1);
    }) |info| {
        // The status enum is open, so a status from a newer server may have no name.
        const status = std.enums.tagName(temporal.client.NexusOperationExecutionStatus, info.status) orelse "unknown";
        std.log.info("\tOperationID: {s}, Operation: {s}, Status: {s}", .{ info.operation_id, info.operation, status });
    }

    const count = client.countNexusOperations(query) catch |err| {
        std.log.err("Unable to count Nexus operations: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Total Nexus operations: {d}", .{count});
}
