const std = @import("std");
const temporal = @import("temporal");

/// Connects to a port nothing listens on. The connection fails, but only after the core has
/// started its runtime and tried to connect, so the core library links and runs.
pub fn main(init: std.process.Init) !void {
    const host_port = "127.0.0.1:1";
    var client = temporal.client.Client.connect(init.gpa, init.io, .{
        .host_port = host_port,
        .core_log_level = null,
        .rpc_timeout = .fromMilliseconds(5_000),
        .retry = .{ .max_retries = 1, .max_elapsed_time = .fromMilliseconds(1_000) },
    }) catch |err| switch (err) {
        error.ConnectFailed => {
            std.debug.print("release smoke test passed: the core ran, and connecting to {s} failed as expected\n", .{host_port});
            return;
        },
        else => {
            std.debug.print("release smoke test failed: connecting to {s} should fail with ConnectFailed, not {t}\n", .{ host_port, err });
            std.process.exit(1);
        },
    };
    client.close();
    std.debug.print("release smoke test failed: connecting to {s} succeeded, but nothing should listen there\n", .{host_port});
    std.process.exit(1);
}
