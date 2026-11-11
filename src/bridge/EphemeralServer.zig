//! Local Temporal servers started by sdk-core for tests and samples: the dev server (the
//! Temporal CLI's `server start-dev`) and the time-skipping test server.
//!
//! The core either runs an existing executable or downloads one into a cache directory and
//! reuses it on later starts.

const EphemeralServer = @This();

const std = @import("std");
const c = @import("core_c");
const bytes = @import("bytes.zig");
const Runtime = @import("Runtime.zig");
const Completion = @import("completion.zig").Completion;
const Allocator = std.mem.Allocator;
const Io = std.Io;

ptr: *c.TemporalCoreEphemeralServer,
runtime: Runtime,
/// The server's gRPC address, such as "127.0.0.1:61234". Owned.
target: []u8,
gpa: Allocator,

pub const Executable = union(enum) {
    /// Run this binary (the Temporal CLI for the dev server, temporal-test-server otherwise).
    existing_path: []const u8,
    download: Download,

    pub const Download = struct {
        /// "default" picks the version sdk-core recommends; otherwise an exact version.
        version: []const u8 = "default",
        /// Where downloads are cached. Empty uses the system temp directory.
        dest_dir: []const u8 = "",
        /// Re-download after this many seconds. 0 keeps the cached binary forever.
        ttl_seconds: u64 = 0,
    };
};

pub const SearchAttribute = struct {
    name: []const u8,
    type: enum { Text, Keyword, Int, Double, Bool, Datetime, KeywordList },
};

pub const DevServerOptions = struct {
    executable: Executable = .{ .download = .{} },
    namespace: []const u8 = "default",
    ip: []const u8 = "127.0.0.1",
    /// 0 picks a free port.
    port: u16 = 0,
    /// Empty keeps the database in memory.
    database_filename: []const u8 = "",
    ui: bool = false,
    ui_port: u16 = 0,
    log_format: []const u8 = "pretty",
    log_level: []const u8 = "warn",
    /// Custom search attributes to register at startup.
    search_attributes: []const SearchAttribute = &.{},
    /// Extra command-line arguments for the server.
    extra_args: []const []const u8 = &.{},
    /// 0 uses the core default.
    startup_timeout_ms: u64 = 0,
};

pub const TestServerOptions = struct {
    executable: Executable = .{ .download = .{} },
    /// 0 picks a free port.
    port: u16 = 0,
    extra_args: []const []const u8 = &.{},
    startup_timeout_ms: u64 = 0,
};

pub const StartError = error{ OutOfMemory, InvalidOptions, StartFailed };

const sdk_name = "temporal-zig";
const sdk_version = @import("../temporal.zig").version;

/// Starts a dev server and waits for it to accept connections. The runtime must outlive the
/// server. On `error.StartFailed`, `failure` (if given) receives the core's message, allocated
/// with `gpa`.
pub fn startDevServer(gpa: Allocator, io: Io, runtime: Runtime, options: DevServerOptions, failure: ?*?[]u8) StartError!EphemeralServer {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var args: std.ArrayList([]const u8) = .empty;
    for (options.search_attributes) |sa| {
        try args.append(arena, "--search-attribute");
        try args.append(arena, try std.fmt.allocPrint(arena, "{s}={t}", .{ sa.name, sa.type }));
    }
    try args.appendSlice(arena, options.extra_args);

    const test_server = try testServerOptions(arena, options.executable, options.port, args.items, options.startup_timeout_ms);
    const dev: c.TemporalCoreDevServerOptions = .{
        .test_server = test_server,
        .namespace_ = bytes.ref(options.namespace),
        .ip = bytes.ref(options.ip),
        .database_filename = bytes.ref(options.database_filename),
        .ui = options.ui,
        .ui_port = options.ui_port,
        .log_format = bytes.ref(options.log_format),
        .log_level = bytes.ref(options.log_level),
    };
    const done = try Completion(StartResult).create(gpa, io, StartResult.free);
    c.temporal_core_ephemeral_server_start_dev_server(runtime.ptr, &dev, done, onStart);
    // Uncancelable: the core reads the options from `arena` until it calls back, and startup
    // is bounded by its timeout.
    return finishStart(gpa, runtime, done.waitUncancelable(), failure);
}

/// Starts the time-skipping test server. See `startDevServer`.
pub fn startTestServer(gpa: Allocator, io: Io, runtime: Runtime, options: TestServerOptions, failure: ?*?[]u8) StartError!EphemeralServer {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const test_server = try testServerOptions(arena, options.executable, options.port, options.extra_args, options.startup_timeout_ms);
    const done = try Completion(StartResult).create(gpa, io, StartResult.free);
    c.temporal_core_ephemeral_server_start_test_server(runtime.ptr, test_server, done, onStart);
    return finishStart(gpa, runtime, done.waitUncancelable(), failure);
}

/// Stops the server and frees it. On `error.ShutdownFailed`, `failure` (if given) receives the
/// core's message; the server is freed either way.
pub fn shutdown(s: *EphemeralServer, io: Io, failure: ?*?[]u8) error{ OutOfMemory, ShutdownFailed }!void {
    defer s.deinit();
    const done = try Completion(?bytes.Owned).create(s.gpa, io, freeOwned);
    c.temporal_core_ephemeral_server_shutdown(s.ptr, done, onShutdown);
    const fail = done.waitUncancelable() orelse return;
    defer fail.deinit();
    if (failure) |f| f.* = fail.dupe(s.gpa) catch null;
    return error.ShutdownFailed;
}

/// Frees the server without stopping it first. Prefer `shutdown`.
pub fn deinit(s: *EphemeralServer) void {
    c.temporal_core_ephemeral_server_free(s.ptr);
    s.gpa.free(s.target);
    s.* = undefined;
}

const StartResult = struct {
    server: ?*c.TemporalCoreEphemeralServer,
    target: ?bytes.Owned,
    fail: ?bytes.Owned,

    fn free(r: StartResult) void {
        if (r.target) |t| t.deinit();
        if (r.fail) |f| f.deinit();
        if (r.server) |s| c.temporal_core_ephemeral_server_free(s);
    }
};

fn onStart(
    user_data: ?*anyopaque,
    server: ?*c.TemporalCoreEphemeralServer,
    target: ?*const c.TemporalCoreByteArray,
    fail: ?*const c.TemporalCoreByteArray,
) callconv(.c) void {
    const done: *Completion(StartResult) = @ptrCast(@alignCast(user_data.?));
    // The arrays come from the runtime's pool; freeing without the runtime pointer is allowed
    // and only skips recycling the buffer.
    done.complete(.{ .server = server, .target = bytes.Owned.take(null, target), .fail = bytes.Owned.take(null, fail) });
}

fn onShutdown(user_data: ?*anyopaque, fail: ?*const c.TemporalCoreByteArray) callconv(.c) void {
    const done: *Completion(?bytes.Owned) = @ptrCast(@alignCast(user_data.?));
    done.complete(bytes.Owned.take(null, fail));
}

fn freeOwned(o: ?bytes.Owned) void {
    if (o) |v| v.deinit();
}

fn finishStart(gpa: Allocator, runtime: Runtime, result: StartResult, failure: ?*?[]u8) StartError!EphemeralServer {
    defer if (result.fail) |f| f.deinit();
    defer if (result.target) |t| t.deinit();
    const server = result.server orelse {
        if (failure) |f| f.* = if (result.fail) |msg| msg.dupe(gpa) catch null else null;
        return error.StartFailed;
    };
    const target = gpa.dupe(u8, if (result.target) |t| t.bytes() else "") catch {
        c.temporal_core_ephemeral_server_free(server);
        return error.OutOfMemory;
    };
    return .{ .ptr = server, .runtime = runtime, .target = target, .gpa = gpa };
}

fn testServerOptions(
    arena: Allocator,
    executable: Executable,
    port: u16,
    extra_args: []const []const u8,
    startup_timeout_ms: u64,
) StartError!*c.TemporalCoreTestServerOptions {
    for (extra_args) |a| if (std.mem.indexOfScalar(u8, a, '\n') != null) return error.InvalidOptions;
    const joined = try std.mem.join(arena, "\n", extra_args);
    const opts = try arena.create(c.TemporalCoreTestServerOptions);
    opts.* = .{
        .existing_path = bytes.ref(""),
        .sdk_name = bytes.ref(sdk_name),
        .sdk_version = bytes.ref(sdk_version),
        .download_version = bytes.ref(""),
        .download_dest_dir = bytes.ref(""),
        .port = port,
        .extra_args = bytes.ref(joined),
        .download_ttl_seconds = 0,
        .startup_timeout_millis = startup_timeout_ms,
    };
    switch (executable) {
        .existing_path => |p| {
            if (p.len == 0) return error.InvalidOptions;
            opts.existing_path = bytes.ref(p);
        },
        .download => |d| {
            opts.download_version = bytes.ref(d.version);
            opts.download_dest_dir = bytes.ref(d.dest_dir);
            opts.download_ttl_seconds = d.ttl_seconds;
        },
    }
    return opts;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "a dev server starts, reports its address, and shuts down" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();

    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startDevServer(testing.allocator, testing.io, rt, .{
        .search_attributes = &.{.{ .name = "CustomerId", .type = .Keyword }},
    }, &failure) catch |err| {
        std.debug.print("dev server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    try testing.expect(std.mem.startsWith(u8, server.target, "127.0.0.1:"));
    try server.shutdown(testing.io, null);
}

test "a missing executable fails with the core's message" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    try testing.expectError(error.StartFailed, startDevServer(testing.allocator, testing.io, rt, .{
        .executable = .{ .existing_path = "/nonexistent/temporal" },
    }, &failure));
    try testing.expect(failure != null and failure.?.len > 0);
}

test "arguments with newlines are rejected" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    try testing.expectError(error.InvalidOptions, startDevServer(testing.allocator, testing.io, rt, .{
        .extra_args = &.{"--bad\narg"},
    }, null));
}

test "a time-skipping test server starts and shuts down" {
    const rt = try Runtime.init(testing.allocator, .{}, null);
    defer rt.deinit();
    var failure: ?[]u8 = null;
    defer if (failure) |f| testing.allocator.free(f);
    var server = startTestServer(testing.allocator, testing.io, rt, .{}, &failure) catch |err| {
        std.debug.print("test server failed to start: {s}\n", .{failure orelse "(no message)"});
        return err;
    };
    try testing.expect(server.target.len > 0);
    try server.shutdown(testing.io, null);
}
