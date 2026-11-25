//! Runs the greet operation from client code in namespace
//! nexus-standalone-activity-caller-namespace, with no workflow around it, and prints the
//! greeting the handler's standalone activity built.

const std = @import("std");
const temporal = @import("temporal");
const caller = @import("caller.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();

    // The client is a heavyweight object that should be created once per process.
    var client = caller.connect(gpa, io, arena) catch |err| {
        std.log.err("Unable to create client for namespace {s}: {t}", .{ caller.namespace, err });
        std.process.exit(1);
    };
    defer client.close();

    const operation_id = try std.fmt.allocPrint(arena, "greeting-{s}", .{try temporal.client.uuid(arena, io)});
    const operation = caller.startGreet(&client, arena, io, caller.endpoint, operation_id, .{ .name = "World" }, .fromSeconds(10)) catch |err| {
        std.log.err("Unable to execute Greet operation: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("Started Greet operation OperationID={s}", .{operation.id});

    const result = caller.greetResult(&client, arena, operation) catch |err| {
        std.log.err("Unable to get Greet operation result: {t}", .{err});
        std.process.exit(1);
    };
    std.log.info("{s}", .{result.message});
}
