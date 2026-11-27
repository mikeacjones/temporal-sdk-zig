//! A test that points file descriptor 2 away from the runner's stderr, as
//! src/bridge/EphemeralServer.zig's OutputRedirect does (to a temporary file, or /dev/null)
//! while a dev server downloads and starts, and then never ends, for the
//! "check hang report names a test whose stderr is redirected" case: the report must still
//! reach the stderr the process had when the runner started. The check kills the process.

const std = @import("std");

test "waits forever with stderr pointed at /dev/null" {
    const null_fd = try std.posix.openat(std.posix.AT.FDCWD, "/dev/null", .{ .ACCMODE = .WRONLY }, 0);
    if (std.posix.system.dup2(null_fd, std.posix.STDERR_FILENO) < 0) return error.Dup2Failed;
    _ = std.posix.system.close(null_fd);
    try waitsWithStderrRedirected();
}

noinline fn waitsWithStderrRedirected() !void {
    var event: std.Io.Event = .unset;
    try event.wait(std.testing.io);
}
