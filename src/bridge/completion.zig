//! Turns sdk-core callbacks into results `std.Io` code can wait for.
//!
//! Async core functions take a `user_data` pointer and a callback that the core calls once,
//! on one of its own threads. `Completion(T)` is the cell behind `user_data`:
//!
//!     const done = try Completion(Result).create(gpa, io, freeResult);
//!     temporal_core_something(..., done, callback); // callback calls done.complete(result)
//!     const result = try done.wait();
//!
//! If the waiting task is canceled, the cell is abandoned rather than freed, because the
//! core will still call back into it. The callback then frees the result with `free_result`
//! and destroys the cell. Callers that can also cancel the core call (for example RPCs with a
//! cancellation token) should do that too, so the callback comes sooner.
//!
//! The waker is a futex wake on the cell's state, called from the core's thread. That is
//! safe with `std.Io.Threaded`, the Io workers run on. It is not used with the workflow Io.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

pub fn Completion(comptime T: type) type {
    return struct {
        const Self = @This();

        const pending: u32 = 0;
        const done: u32 = 1;
        const abandoned: u32 = 2;

        state: std.atomic.Value(u32) = .init(pending),
        result: T = undefined,
        io: Io,
        gpa: Allocator,
        free_result: *const fn (T) void,

        /// `free_result` releases a result nobody will read (after cancellation).
        pub fn create(gpa: Allocator, io: Io, free_result: *const fn (T) void) Allocator.Error!*Self {
            const self = try gpa.create(Self);
            self.* = .{ .io = io, .gpa = gpa, .free_result = free_result };
            return self;
        }

        /// Frees a cell whose core call was never started (for example it failed to build
        /// its options). Never call this after handing the cell to the core.
        pub fn destroyUnused(self: *Self) void {
            self.gpa.destroy(self);
        }

        /// Stores the result and wakes the waiter. Called once, from the core's callback,
        /// on any thread.
        pub fn complete(self: *Self, result: T) void {
            self.result = result;
            // Read everything needed after the state change first: once the waiter sees
            // `done` it frees the cell.
            const io = self.io;
            const state_ptr: *const u32 = &self.state.raw;
            if (self.state.cmpxchgStrong(pending, done, .acq_rel, .acquire)) |prev| {
                std.debug.assert(prev == abandoned);
                self.free_result(result);
                self.gpa.destroy(self);
                return;
            }
            // The address is only used as a wait-queue key, so waking after the waiter has
            // freed the cell is harmless.
            io.futexWake(u32, state_ptr, 1);
        }

        /// Waits for the result and frees the cell. On `error.Canceled` the cell is
        /// abandoned to the callback, which frees it and the result.
        pub fn wait(self: *Self) Io.Cancelable!T {
            while (self.state.load(.acquire) == pending) {
                self.io.futexWait(u32, &self.state.raw, pending) catch |err| switch (err) {
                    error.Canceled => {
                        if (self.state.cmpxchgStrong(pending, abandoned, .acq_rel, .acquire) == null)
                            return error.Canceled;
                        // The result arrived as the cancel did. Keep the result and re-arm
                        // the cancel request so the caller still sees it at its next wait.
                        self.io.recancel();
                        break;
                    },
                };
            }
            return self.take();
        }

        /// Waits for the result regardless of cancellation, and frees the cell.
        pub fn waitUncancelable(self: *Self) T {
            while (self.state.load(.acquire) == pending) {
                self.io.futexWaitUncancelable(u32, &self.state.raw, pending);
            }
            return self.take();
        }

        fn take(self: *Self) T {
            const result = self.result;
            self.gpa.destroy(self);
            return result;
        }
    };
}

// ---------------------------------------------------------------- tests

const testing = std.testing;
const c = @import("core_c");
const bytes = @import("bytes.zig");

const Text = struct {
    buf: []u8,
    fn free(t: Text) void {
        testing.allocator.free(t.buf);
    }
};

fn completeLater(done: *Completion(Text), ms: i64, text: []const u8) void {
    testing.io.sleep(.fromMilliseconds(ms), .awake) catch {};
    done.complete(.{ .buf = testing.allocator.dupe(u8, text) catch @panic("OOM") });
}

test "a result completed on another thread wakes the waiter" {
    const done = try Completion(Text).create(testing.allocator, testing.io, Text.free);
    const thread = try std.Thread.spawn(.{}, completeLater, .{ done, 20, "hello" });
    defer thread.join();
    const result = try done.wait();
    defer result.free();
    try testing.expectEqualStrings("hello", result.buf);
}

test "a result that is already complete is returned without blocking" {
    const done = try Completion(Text).create(testing.allocator, testing.io, Text.free);
    done.complete(.{ .buf = try testing.allocator.dupe(u8, "ready") });
    const result = done.waitUncancelable();
    defer result.free();
    try testing.expectEqualStrings("ready", result.buf);
}

fn waitFor(done: *Completion(Text)) Io.Cancelable!usize {
    const result = try done.wait();
    defer result.free();
    return result.buf.len;
}

test "canceling the waiter abandons the cell, and the late callback frees everything" {
    const done = try Completion(Text).create(testing.allocator, testing.io, Text.free);
    var waiter = try testing.io.concurrent(waitFor, .{done});
    try testing.io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectError(error.Canceled, waiter.cancel(testing.io));
    // The core calls back after the waiter gave up; testing.allocator reports any leak.
    completeLater(done, 0, "late");
}

const StartResult = struct {
    server: ?*c.TemporalCoreEphemeralServer,
    fail: ?bytes.Owned,
    fn free(r: StartResult) void {
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
    if (bytes.Owned.take(null, target)) |t| t.deinit();
    done.complete(.{ .server = server, .fail = bytes.Owned.take(null, fail) });
}

test "a real core callback completes on a core thread" {
    var options = std.mem.zeroes(c.TemporalCoreRuntimeOptions);
    const rt = c.temporal_core_runtime_new(&options);
    defer c.temporal_core_runtime_free(rt.runtime);
    try testing.expect(rt.fail == null);

    // Starting a dev server from a binary that doesn't exist fails fast, without a network.
    const test_server: c.TemporalCoreTestServerOptions = .{
        .existing_path = bytes.ref("/nonexistent/temporal"),
        .sdk_name = bytes.ref("temporal-zig"),
        .sdk_version = bytes.ref("0.0.0"),
        .download_version = bytes.ref(""),
        .download_dest_dir = bytes.ref(""),
        .port = 0,
        .extra_args = bytes.ref(""),
        .download_ttl_seconds = 0,
        .startup_timeout_millis = 5_000,
    };
    const dev: c.TemporalCoreDevServerOptions = .{
        .test_server = &test_server,
        .namespace_ = bytes.ref("default"),
        .ip = bytes.ref("127.0.0.1"),
        .database_filename = bytes.ref(""),
        .ui = false,
        .ui_port = 0,
        .log_format = bytes.ref("pretty"),
        .log_level = bytes.ref("warn"),
    };
    const done = try Completion(StartResult).create(testing.allocator, testing.io, StartResult.free);
    c.temporal_core_ephemeral_server_start_dev_server(rt.runtime, &dev, done, onStart);
    const result = try done.wait();
    defer result.free();
    try testing.expect(result.server == null);
    try testing.expect(result.fail.?.bytes().len > 0);
}
