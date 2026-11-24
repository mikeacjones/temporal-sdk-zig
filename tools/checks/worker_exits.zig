//! Sweeps the ways `worker.Worker.run` can end before or while its poll loops start, against
//! one dev server, so that an exit that never returns fails the `check worker exits` case
//! (run through time-limit) naming the point, instead of hanging `zig build test`.
//!
//!   worker-exits
//!     Exits 0 when every sweep ran to its end condition and found nothing, 1 when one found
//!     a problem or the program couldn't run (dev server or client), 2 with any argument.
//!
//! Shapes, each a Worker with a workflow and an activity: `workflow-activity`,
//! `workflow-activity-nexus` (plus a Nexus service) and `workflow-activity-sessions`
//! (`Options.sessions = .{}`). For each shape, in order, every iteration on a fresh Worker
//! whose allocator is a `std.testing.FailingAllocator` (checked for leaks after `deinit`) and
//! whose `run` runs in a task of its own:
//! - `concurrency` n = 0, 1, ...: the Worker's Io allows n `concurrent` calls, then refuses.
//!   `run` must succeed, or return `error.ConcurrencyUnavailable` after a refusal. Ends at the
//!   first n whose run had nothing refused (it started all its loops, or failed otherwise).
//! - `allocation` i = 0, 1, ...: every allocation from the i-th on fails. `Worker.init` and
//!   registration may return only `error.OutOfMemory`; `run` must return it, or succeed when
//!   it had started (main then stopped it). Ends at the first i with no induced failure.
//! - `stop` 0: `stop` before `run`. k = 1, 2, ...: `stop` at the k-th call `run`'s own thread
//!   makes into the Worker's Io (`concurrent`, `await`, `futexWait`,
//!   `futexWaitUncancelable`, `sleep`). `run` must succeed. Ends at the first k at which `run`
//!   parked in `await` (its poll loops run) before its k-th call; main stops it then. (Also
//!   ends if `run` returns on its own before its k-th call, a wrong result.)
//! Outside the allocation sweep, a failed `Worker.init` or registration ends the sweep.
//! - `validation` 0: the client's namespace doesn't exist; `run` must return
//!   `error.ValidationFailed`.
//!
//! stdout: `worker-exits: <shape> <sweep> <index>` before each iteration, the SDK's log
//! records, and at the end, when nothing was found, `worker-exits: <shape> <sweep>: <count>
//! iterations` for each. stderr only for problems, each line starting with the point:
//! - `worker-exits: <shape> <sweep> <index>: <phase> did not return within 30 s`: a watchdog
//!   thread saw one iteration last 30 s; `<phase>` is the call main was in (`Worker.init`,
//!   `registration`, `Worker.run` (awaiting it), `Worker.waitUntilRunning`, `Worker.stop`,
//!   `Worker.deinit`). The program stops the dev server and exits 1 at once.
//! - `... <index>: <call> returned <result>, expected <allowed>`: a wrong result.
//! - `... <index>: leaked <n> bytes in <m> allocations`: memory from the Worker's allocator
//!   not freed after `deinit`.
//! A wrong result or a leak is recorded and the sweeps go on, so one run lists every one.
//!
//! The `stop` sweep calls `Worker.stop` on `run`'s own thread, from inside the Io call, and
//! `stop` locks `Worker.stop_mutex`. That is safe only while `run` makes no counted call with
//! `stop_mutex` held. Today it makes none: under the mutex `run` stores fields, calls
//! `initiateShutdown`, and broadcasts `start_changed`, which is a `futexWake`, so `futexWake`
//! is not counted. A hang whose stack is `run`'s thread inside this wrapper waiting for
//! `stop_mutex` is this constraint, not an SDK bug: stop counting that call.
//!
//! Iteration counts vary with thread timing (and the process's first run also reads the
//! executable's debug info), so no count is asserted; only the end conditions end a sweep.

const std = @import("std");
const temporal = @import("temporal");
const Io = std.Io;
const Worker = temporal.worker.Worker;
const Client = temporal.client.Client;
const nexus = temporal.nexus;

pub const std_options: std.Options = .{ .logFn = logToStdout };

/// Formats each record into a stack buffer and writes it to stdout in one write, so the
/// SDK's logs stay off stderr, which `zig build` shows.
fn logToStdout(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    var buf: [4096]u8 = undefined;
    var w: Io.Writer = .fixed(&buf);
    w.print("[" ++ comptime level.asText() ++ "] (" ++ @tagName(scope) ++ "): " ++ format ++ "\n", args) catch {
        buf[buf.len - 1] = '\n';
        return writeStdout(&buf);
    };
    writeStdout(w.buffered());
}

fn writeStdout(bytes: []const u8) void {
    var rest = bytes;
    while (rest.len > 0) {
        const rc = std.posix.system.write(std.posix.STDOUT_FILENO, rest.ptr, rest.len);
        if (rc <= 0) return;
        rest = rest[@intCast(rc)..];
    }
}

fn printStdout(comptime format: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    writeStdout(std.fmt.bufPrint(&buf, format, args) catch return);
}

fn greet(ctx: *temporal.activity.Context, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}
const Greet = temporal.activity.Def("WorkerExitsGreet", greet);

fn hello(ctx: *temporal.workflow.Context, name: []const u8) ![]const u8 {
    return ctx.executeActivity(Greet, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name}).get();
}
const Hello = temporal.workflow.Def("WorkerExitsHello", hello);

const Echo = nexus.OperationDef("echo", []const u8, []const u8);
const Service = nexus.ServiceDef("worker-exits", .{Echo});
fn echo(_: *nexus.Context, input: []const u8) ![]const u8 {
    return input;
}
const Handler = nexus.ServiceHandler(Service, .{nexus.OperationHandler(Echo, echo)});

const Shape = enum(u8) { @"workflow-activity", @"workflow-activity-nexus", @"workflow-activity-sessions" };
const Sweep = enum(u8) { concurrency, allocation, stop, validation };
const Phase = enum(u8) { @"Worker.init", registration, @"Worker.run", @"Worker.waitUntilRunning", @"Worker.stop", @"Worker.deinit" };

/// One `FailingAllocator` behind a mutex: the run task, the poll loops and the debug-info
/// read all allocate through it. The mutex is locked on the real Io, never a wrapper.
const LockedFailing = struct {
    failing: std.testing.FailingAllocator,
    mutex: Io.Mutex = .init,
    io: Io,

    fn allocator(l: *LockedFailing) std.mem.Allocator {
        return .{ .ptr = l, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn lock(ctx: *anyopaque) *LockedFailing {
        const l: *LockedFailing = @ptrCast(@alignCast(ctx));
        l.mutex.lockUncancelable(l.io);
        return l;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const l = lock(ctx);
        defer l.mutex.unlock(l.io);
        return l.failing.allocator().rawAlloc(len, alignment, ret);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) bool {
        const l = lock(ctx);
        defer l.mutex.unlock(l.io);
        return l.failing.allocator().rawResize(memory, alignment, new_len, ret);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
        const l = lock(ctx);
        defer l.mutex.unlock(l.io);
        return l.failing.allocator().rawRemap(memory, alignment, new_len, ret);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const l = lock(ctx);
        defer l.mutex.unlock(l.io);
        l.failing.allocator().rawFree(memory, alignment, ret);
    }
};

/// The concurrency sweep's Io: `concurrent` succeeds `allowed` times, then refuses. The
/// vtable functions get only the base's userdata, so the state is file-level: one at a time.
const ScarceIo = struct {
    var inner: Io = undefined;
    var vtable: Io.VTable = undefined;
    var allowed: std.atomic.Value(usize) = .init(0);
    var refused: std.atomic.Value(usize) = .init(0);

    fn io(base: Io, n: usize) Io {
        inner = base;
        allowed.store(n, .release);
        refused.store(0, .release);
        vtable = base.vtable.*;
        vtable.concurrent = concurrent;
        return .{ .userdata = base.userdata, .vtable = &vtable };
    }

    fn concurrent(
        userdata: ?*anyopaque,
        result_len: usize,
        result_alignment: std.mem.Alignment,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) Io.ConcurrentError!*Io.AnyFuture {
        var n = allowed.load(.acquire);
        while (true) {
            if (n == 0) {
                _ = refused.fetchAdd(1, .acq_rel);
                return error.ConcurrencyUnavailable;
            }
            n = allowed.cmpxchgWeak(n, n - 1, .acq_rel, .acquire) orelse break;
        }
        return inner.vtable.concurrent(userdata, result_len, result_alignment, context, context_alignment, start);
    }
};

/// The stop sweep's Io: counts the calls `run`'s own thread makes at which it can block or
/// start work, and calls `Worker.stop` on that thread at the k-th. `futexWake` isn't counted
/// (see the top comment).
const StopAtIo = struct {
    var inner: Io = undefined;
    var vtable: Io.VTable = undefined;
    var worker: *Worker = undefined;
    var run_thread: std.atomic.Value(std.Thread.Id) = .init(0);
    var recorded: std.atomic.Value(bool) = .init(false);
    var count: std.atomic.Value(u64) = .init(0);
    var k: u64 = 0;
    var armed: std.atomic.Value(bool) = .init(false);
    /// Set when `run`'s thread enters `await`: its poll loops run.
    var parked: Io.Event = .unset;
    /// What main waits on: set by `parked`, by the k-th call, and by the run task's end.
    var wake: Io.Event = .unset;

    /// `worker` is set once the Worker exists, before its `run` starts.
    fn io(base: Io, at: u64) Io {
        inner = base;
        recorded.store(false, .release);
        count.store(0, .release);
        k = at;
        armed.store(true, .release);
        parked.reset();
        wake.reset();
        vtable = base.vtable.*;
        vtable.concurrent = concurrent;
        vtable.await = await;
        vtable.futexWait = futexWait;
        vtable.futexWaitUncancelable = futexWaitUncancelable;
        vtable.sleep = sleep;
        return .{ .userdata = base.userdata, .vtable = &vtable };
    }

    fn onRunThread() bool {
        return recorded.load(.acquire) and run_thread.load(.acquire) == std.Thread.getCurrentId();
    }

    fn tick() void {
        if (count.fetchAdd(1, .acq_rel) + 1 != k) return;
        if (armed.cmpxchgStrong(true, false, .acq_rel, .acquire) != null) return;
        worker.stop();
        wake.set(inner);
    }

    fn concurrent(
        userdata: ?*anyopaque,
        result_len: usize,
        result_alignment: std.mem.Alignment,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) Io.ConcurrentError!*Io.AnyFuture {
        if (onRunThread()) tick();
        return inner.vtable.concurrent(userdata, result_len, result_alignment, context, context_alignment, start);
    }

    fn await(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
        if (onRunThread()) {
            tick();
            parked.set(inner);
            wake.set(inner);
        }
        return inner.vtable.await(userdata, any_future, result, result_alignment);
    }

    fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
        if (onRunThread()) tick();
        return inner.vtable.futexWait(userdata, ptr, expected, timeout);
    }

    fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
        if (onRunThread()) tick();
        return inner.vtable.futexWaitUncancelable(userdata, ptr, expected);
    }

    fn sleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
        if (onRunThread()) tick();
        return inner.vtable.sleep(userdata, timeout);
    }
};

/// Reports an iteration that has lasted 30 s, naming its point and the call main is in, then
/// stops the dev server and ends the process with 1. Main publishes the point, then bumps
/// `iteration`.
const Watchdog = struct {
    const limit_seconds = 30;
    /// 0 before the first iteration; `finished` after the last.
    var iteration: std.atomic.Value(u64) = .init(0);
    const finished = std.math.maxInt(u64);
    var shape: std.atomic.Value(Shape) = .init(.@"workflow-activity");
    var sweep: std.atomic.Value(Sweep) = .init(.concurrency);
    var index: std.atomic.Value(u64) = .init(0);
    var phase: std.atomic.Value(Phase) = .init(.@"Worker.init");
    var server: ?*temporal.testing.DevServer = null;

    fn begin(s: Shape, sw: Sweep, i: u64) void {
        shape.store(s, .release);
        sweep.store(sw, .release);
        index.store(i, .release);
        phase.store(.@"Worker.init", .release);
        _ = iteration.fetchAdd(1, .acq_rel);
    }

    fn enter(p: Phase) void {
        phase.store(p, .release);
    }

    fn watch(io: Io) void {
        var seen: u64 = 0;
        var since = Io.Clock.awake.now(io);
        while (true) {
            io.sleep(.fromSeconds(1), .awake) catch {};
            const now = Io.Clock.awake.now(io);
            const it = iteration.load(.acquire);
            if (it != seen) {
                seen = it;
                since = now;
                continue;
            }
            if (it == 0 or it == finished) continue;
            if (since.durationTo(now).toSeconds() < limit_seconds) continue;
            const s = shape.load(.acquire);
            const sw = sweep.load(.acquire);
            const i = index.load(.acquire);
            const p = phase.load(.acquire);
            if (iteration.load(.acquire) != it) continue;
            std.debug.print("worker-exits: {t} {t} {d}: {t} did not return within {d} s\n", .{ s, sw, i, p, limit_seconds });
            if (server) |srv| srv.stop(io);
            std.process.exit(1);
        }
    }
};

/// The number of wrong results and leaks found so far.
var problems: usize = 0;

fn problem(shape: Shape, sweep: Sweep, index: u64, comptime format: []const u8, args: anytype) void {
    problems += 1;
    std.debug.print("worker-exits: {t} {t} {d}: ", .{ shape, sweep, index });
    std.debug.print(format ++ "\n", args);
}

fn runTask(w: *Worker, record: bool) Worker.RunError!void {
    if (record) {
        StopAtIo.run_thread.store(std.Thread.getCurrentId(), .release);
        StopAtIo.recorded.store(true, .release);
    }
    const result = w.run();
    if (record) {
        StopAtIo.recorded.store(false, .release);
        StopAtIo.wake.set(StopAtIo.inner);
    }
    return result;
}

const Context = struct {
    io: Io,
    gpa: std.mem.Allocator,
    client: *Client,
    /// A client whose namespace doesn't exist, for the validation sweep.
    bad_client: *Client,
};

/// What an iteration tells its sweep: whether the sweep has reached its end condition.
const Done = enum { more, end };

fn runIteration(c: Context, shape: Shape, sweep: Sweep, index: u64) Done {
    Watchdog.begin(shape, sweep, index);
    printStdout("worker-exits: {t} {t} {d}\n", .{ shape, sweep, index });

    var locked: LockedFailing = .{ .io = c.io, .failing = .init(c.gpa, .{
        .fail_index = if (sweep == .allocation) index else std.math.maxInt(usize),
    }) };
    var queue_buf: [128]u8 = undefined;
    const task_queue = std.fmt.bufPrint(&queue_buf, "worker-exits-{t}-{t}-{d}", .{ shape, sweep, index }) catch unreachable;
    const options: temporal.worker.Options = if (shape == .@"workflow-activity-sessions") .{ .sessions = .{} } else .{};
    const client = if (sweep == .validation) c.bad_client else c.client;

    const worker_io: Io = switch (sweep) {
        .concurrency => ScarceIo.io(c.io, index),
        .stop => if (index > 0) StopAtIo.io(c.io, index) else c.io,
        .allocation, .validation => c.io,
    };

    // Outside the allocation sweep, a failed `init` or registration fails the same way at every
    // later index: it is reported once and ends the sweep.
    var w = Worker.init(locked.allocator(), worker_io, client, task_queue, options) catch |err| {
        if (sweep != .allocation) problem(shape, sweep, index, "Worker.init returned {t}, expected success", .{err});
        checkLeaks(&locked, shape, sweep, index);
        return if (sweep == .allocation) .more else .end;
    };
    var deinited = false;
    defer if (!deinited) w.deinit();

    Watchdog.enter(.registration);
    register(&w, shape) catch |err| {
        if (sweep != .allocation or err != error.OutOfMemory)
            problem(shape, sweep, index, "registration returned {t}, expected {s}", .{ err, if (sweep == .allocation) "success or error.OutOfMemory" else "success" });
        Watchdog.enter(.@"Worker.deinit");
        w.deinit();
        deinited = true;
        checkLeaks(&locked, shape, sweep, index);
        return if (sweep == .allocation) .more else .end;
    };

    if (sweep == .stop) {
        if (index == 0) {
            Watchdog.enter(.@"Worker.stop");
            w.stop();
        } else StopAtIo.worker = &w;
    }

    Watchdog.enter(.@"Worker.run");
    var future = c.io.concurrent(runTask, .{ &w, sweep == .stop and index > 0 }) catch |err| {
        std.debug.print("worker-exits: starting the run task: {t}\n", .{err});
        std.process.exit(1);
    };

    var started = false;
    var last_stop = false;
    switch (sweep) {
        .concurrency, .allocation => {
            Watchdog.enter(.@"Worker.waitUntilRunning");
            if (w.waitUntilRunning()) |_| {
                started = true;
                Watchdog.enter(.@"Worker.stop");
                w.stop();
            } else |err| if (err != error.WorkerFailedToStart)
                problem(shape, sweep, index, "Worker.waitUntilRunning returned {t}, expected success or error.WorkerFailedToStart", .{err});
        },
        .stop => if (index > 0) {
            StopAtIo.wake.waitUncancelable(c.io);
            // Parked in `await` before its k-th call: its poll loops run, and every earlier
            // point has been swept.
            if (StopAtIo.parked.isSet() and StopAtIo.armed.cmpxchgStrong(true, false, .acq_rel, .acquire) == null) {
                last_stop = true;
                Watchdog.enter(.@"Worker.stop");
                w.stop();
            }
        },
        .validation => {},
    }

    Watchdog.enter(.@"Worker.run");
    const result = future.await(c.io);
    // Read once `run` has returned: a refusal after that point doesn't count.
    const refused = ScarceIo.refused.load(.acquire);
    // `run` ended before its k-th call without parking: nothing later to stop at.
    const ran_out = sweep == .stop and index > 0 and !last_stop and StopAtIo.armed.load(.acquire);

    Watchdog.enter(.@"Worker.deinit");
    w.deinit();
    deinited = true;
    checkLeaks(&locked, shape, sweep, index);
    const induced = locked.failing.has_induced_failure;

    switch (sweep) {
        .concurrency => {
            if (result) |_| {} else |err| if (err != error.ConcurrencyUnavailable or refused == 0)
                problem(shape, sweep, index, "Worker.run returned {t}, expected {s}", .{ err, if (refused == 0) "success" else "success or error.ConcurrencyUnavailable" });
            // With nothing refused, a larger n changes nothing.
            return if (refused == 0) .end else .more;
        },
        .allocation => {
            if (!induced) {
                if (result) |_| {} else |err| problem(shape, sweep, index, "Worker.run returned {t}, expected success", .{err});
                return .end;
            }
            if (result) |_| {
                if (!started) problem(shape, sweep, index, "Worker.run returned success, expected error.OutOfMemory (it never started)", .{});
            } else |err| if (err != error.OutOfMemory)
                problem(shape, sweep, index, "Worker.run returned {t}, expected error.OutOfMemory{s}", .{ err, if (started) " or success" else "" });
            return .more;
        },
        .stop => {
            if (result) |_| {} else |err| problem(shape, sweep, index, "Worker.run returned {t}, expected success", .{err});
            return if (last_stop or ran_out) .end else .more;
        },
        .validation => {
            if (result) |_| {
                problem(shape, sweep, index, "Worker.run returned success, expected error.ValidationFailed", .{});
            } else |err| if (err != error.ValidationFailed)
                problem(shape, sweep, index, "Worker.run returned {t}, expected error.ValidationFailed", .{err});
            return .end;
        },
    }
}

fn register(w: *Worker, shape: Shape) !void {
    try w.registerWorkflow(Hello);
    try w.registerActivity(Greet);
    if (shape == .@"workflow-activity-nexus") try w.registerNexusService(Handler);
}

/// Reads the counters after `deinit` (which joins the debug-info read) or a failed `init`.
fn checkLeaks(locked: *LockedFailing, shape: Shape, sweep: Sweep, index: u64) void {
    const f = &locked.failing;
    if (f.allocated_bytes != f.freed_bytes or f.allocations != f.deallocations)
        problem(shape, sweep, index, "leaked {d} bytes in {d} allocations", .{ f.allocated_bytes -| f.freed_bytes, f.allocations -| f.deallocations });
}

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 1) {
        std.debug.print("usage: worker-exits\n", .{});
        return 2;
    }

    var server = temporal.testing.DevServer.start(gpa, io, .{}) catch |err| {
        std.debug.print("worker-exits: starting the dev server: {t}\n", .{err});
        return 1;
    };
    defer server.stop(io);
    var client = Client.connect(gpa, io, server.clientOptions()) catch |err| {
        std.debug.print("worker-exits: connecting the client: {t}\n", .{err});
        return 1;
    };
    defer client.close();
    var bad_client = client.forNamespace("worker-exits-no-such-namespace") catch |err| {
        std.debug.print("worker-exits: making the client of a missing namespace: {t}\n", .{err});
        return 1;
    };
    defer bad_client.close();

    Watchdog.server = &server;
    const watchdog = std.Thread.spawn(.{}, Watchdog.watch, .{io}) catch |err| {
        std.debug.print("worker-exits: starting the watchdog: {t}\n", .{err});
        return 1;
    };
    watchdog.detach();

    const c: Context = .{ .io = io, .gpa = gpa, .client = &client, .bad_client = &bad_client };
    var counts: [std.enums.values(Shape).len][std.enums.values(Sweep).len]u64 = @splat(@splat(0));
    for (std.enums.values(Shape)) |shape| {
        for (std.enums.values(Sweep)) |sweep| {
            var index: u64 = 0;
            while (true) : (index += 1) {
                const done = runIteration(c, shape, sweep, index);
                counts[@backingInt(shape)][@backingInt(sweep)] = index + 1;
                if (done == .end) break;
            }
        }
    }
    Watchdog.iteration.store(Watchdog.finished, .release);
    // The server goes before the watchdog could see it freed.
    Watchdog.server = null;

    if (problems > 0) return 1;
    for (std.enums.values(Shape)) |shape| for (std.enums.values(Sweep)) |sweep|
        printStdout("worker-exits: {t} {t}: {d} iterations\n", .{ shape, sweep, counts[@backingInt(shape)][@backingInt(sweep)] });
    return 0;
}
