//! The `std.Io` implementation that workflow code runs on.
//!
//! Workflow code must replay identically, so every operation here is deterministic:
//!
//! - `io.async`, `io.concurrent`, `Group`, and `Select` run their work as coroutines on the
//!   fiber `Scheduler`, in FIFO order.
//! - Futex waits block the calling coroutine and wake in FIFO order, so `std.Io.Mutex`,
//!   `Condition`, `Event`, `Semaphore`, and `Queue` work unchanged.
//! - `io.sleep` and futex timeouts become durable timers through `Host`.
//! - `io.now` returns workflow time from `Host`, the same for every clock.
//! - `io.random` draws from a seeded generator; `setRandomSeed` changes the seed.
//! - File, network, process, and secure-random calls fail, as they would make replay depend
//!   on the outside world. They keep the error behavior of `std.Io.failing`.
//!
//! Cancellation follows `std.Io`: a cancel request is delivered as `error.Canceled` at the
//! next cancelable wait of the target task unless the task has blocked cancellation with
//! `swapCancelProtection`. Delivering it consumes the request; `recancel` re-arms it.

const WorkflowIo = @This();

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const assert = std.debug.assert;
const Scheduler = @import("Scheduler.zig");
const Fiber = Scheduler.Fiber;

/// The run's allocator. Task blocks and group state come from it and go back to `free_blocks`
/// for reuse, never to it: freeing the run's memory frees them, including the blocks of
/// coroutines that never exited.
memory: Allocator,
/// Finished blocks by size class (`block_min << i` bytes), linked through their first word.
free_blocks: [block_classes]?*FreeBlock = @splat(null),
scheduler: *Scheduler,
host: Host,
rng: std.Random.DefaultPrng,
/// Coroutines blocked in a futex wait, in the order they started waiting.
futex_head: ?*Wait = null,
futex_tail: ?*Wait = null,
/// Calls that would make replay depend on the outside world (secure randomness).
determinism_violations: u32 = 0,

/// What the workflow runtime provides: workflow time and durable timers.
pub const Host = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Current workflow time.
        now: *const fn (ctx: *anyopaque) Io.Timestamp,
        /// Start a durable timer. The host calls `WorkflowIo.fireTimer` when it fires.
        startTimer: *const fn (ctx: *anyopaque, timer: *Timer, duration: Io.Duration) void,
        /// Cancel a started timer that has not fired.
        cancelTimer: *const fn (ctx: *anyopaque, timer: *Timer) void,
    };
};

/// A durable timer started through `Host`. Lives on the waiting coroutine's stack.
pub const Timer = struct {
    wait: *Wait,
    fired: bool = false,
    /// For the host's bookkeeping (for example the command sequence number).
    host_data: usize = 0,
};

pub fn init(memory: Allocator, scheduler: *Scheduler, host: Host, random_seed: u64) WorkflowIo {
    return .{
        .memory = memory,
        .scheduler = scheduler,
        .host = host,
        .rng = .init(random_seed),
    };
}

pub fn setRandomSeed(w: *WorkflowIo, seed: u64) void {
    w.rng = .init(seed);
}

pub fn io(w: *WorkflowIo) Io {
    return .{ .userdata = w, .vtable = &vtable };
}

/// Runs coroutines until all are blocked. See `Scheduler.run`.
pub fn run(w: *WorkflowIo) void {
    w.scheduler.run();
}

/// Starts a top-level coroutine (the workflow function, a signal or update handler) that can
/// be canceled with `cancelTask`. `func` must return `void`. The task frees itself when
/// `func` returns, so the caller must track completion itself and never use the returned
/// pointer after that.
pub fn spawn(w: *WorkflowIo, name: []const u8, comptime func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !*Task {
    const Args = @TypeOf(args);
    const Erased = struct {
        fn start(context: *const anyopaque, _: *anyopaque) void {
            const a: *const Args = @ptrCast(@alignCast(context));
            @call(.auto, func, a.*);
        }
    };
    const task = try w.createTask(name, std.mem.asBytes(&args), .of(Args), 0, .@"1", Erased.start, .detached);
    return task;
}

/// Requests cancellation of a task started with `spawn` (for example on CancelWorkflow).
pub fn cancelTask(w: *WorkflowIo, task: *Task) void {
    w.requestCancel(task);
}

/// A blocking point for SDK code built on this Io (activity futures, signals): create it on
/// the waiting coroutine with `newWait`, block with `waitFor`, and resume it with `wake`.
pub fn newWaitPoint(w: *WorkflowIo) Wait {
    return w.newWait();
}

/// Blocks the current coroutine until `wait` is woken, or until the task is canceled when
/// `cancelable` is set.
pub fn waitFor(w: *WorkflowIo, wait: *Wait, cancelable: bool) Io.Cancelable!void {
    return w.block(wait, cancelable);
}

/// Wakes a coroutine blocked in `waitFor`. Safe to call more than once.
pub fn wake(w: *WorkflowIo, wait: *Wait) void {
    w.wakeWait(wait);
}

/// Called by the host when a timer started with `Host.VTable.startTimer` fires.
pub fn fireTimer(w: *WorkflowIo, timer: *Timer) void {
    assert(!timer.fired);
    timer.fired = true;
    w.wakeWait(timer.wait);
}

// ---------------------------------------------------------------- tasks

/// A coroutine started through this Io: a future, a group member, or a top-level task.
pub const Task = struct {
    fiber: *Fiber,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    context: []u8,
    result: []u8,
    kind: Kind,
    done: bool = false,
    /// Coroutine waiting in `await` or `cancel` for this task.
    awaiter: ?*Wait = null,
    cancel_requested: bool = false,
    cancel_protection: Io.CancelProtection = .unblocked,
    /// The cancelable wait this task is blocked in, if any.
    blocked_in: ?*Wait = null,
    group: ?*GroupState = null,
    group_next: ?*Task = null,
    /// Allocation backing this Task, its context, and its result.
    memory: []u8,
    memory_align: Alignment,

    const Kind = enum { future, group, detached };
};

/// A coroutine's pending block: woken by futexWake, a task finishing, a timer, or a cancel.
pub const Wait = struct {
    fiber: *Fiber,
    woken: bool = false,
    // Futex wait list link and address.
    futex_ptr: ?*const u32 = null,
    next: ?*Wait = null,
    prev: ?*Wait = null,
};

const GroupState = struct {
    pending: usize = 0,
    head: ?*Task = null,
    awaiter: ?*Wait = null,
};

fn createTask(
    w: *WorkflowIo,
    name: []const u8,
    context: []const u8,
    context_alignment: Alignment,
    result_len: usize,
    result_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    kind: Task.Kind,
) !*Task {
    const alignment = Alignment.of(Task).max(context_alignment).max(result_alignment);
    const context_offset = context_alignment.forward(@sizeOf(Task));
    const result_offset = result_alignment.forward(context_offset + context.len);
    const size = result_offset + result_len;
    const memory = try w.allocBlock(size, alignment);
    const task: *Task = @ptrCast(@alignCast(memory.ptr));
    task.* = .{
        .fiber = undefined,
        .start = start,
        .context = memory[context_offset..][0..context.len],
        .result = memory[result_offset..][0..result_len],
        .kind = kind,
        .memory = memory,
        .memory_align = alignment,
    };
    @memcpy(task.context, context);
    errdefer w.destroyTask(task);
    task.fiber = try w.scheduler.spawn(name, taskMain, .{ w, task });
    task.fiber.user = task;
    return task;
}

fn destroyTask(w: *WorkflowIo, task: *Task) void {
    w.freeBlock(task.memory, task.memory_align);
}

const block_min = 64;
const block_classes = 11; // 64 bytes to 64 KiB
const block_max = block_min << (block_classes - 1);
const block_align: Alignment = .@"16";
const FreeBlock = struct { next: ?*FreeBlock };

/// A block of at least `size` bytes. Pooled blocks are a whole size class long, so the
/// returned slice may be longer than `size`; pass it unchanged to `freeBlock`.
fn allocBlock(w: *WorkflowIo, size: usize, alignment: Alignment) error{OutOfMemory}![]u8 {
    if (size > block_max or alignment.compare(.gt, block_align)) {
        return (w.memory.rawAlloc(size, alignment, @returnAddress()) orelse return error.OutOfMemory)[0..size];
    }
    const class = blockClass(size);
    const len = @as(usize, block_min) << class;
    if (w.free_blocks[class]) |b| {
        w.free_blocks[class] = b.next;
        const p: [*]u8 = @ptrCast(b);
        return p[0..len];
    }
    return (w.memory.rawAlloc(len, block_align, @returnAddress()) orelse return error.OutOfMemory)[0..len];
}

fn freeBlock(w: *WorkflowIo, mem: []u8, alignment: Alignment) void {
    if (mem.len > block_max or alignment.compare(.gt, block_align)) {
        w.memory.rawFree(mem, alignment, @returnAddress());
        return;
    }
    const class = blockClass(mem.len);
    assert(mem.len == @as(usize, block_min) << class);
    const b: *FreeBlock = @ptrCast(@alignCast(mem.ptr));
    b.next = w.free_blocks[class];
    w.free_blocks[class] = b;
}

fn blockLen(size: usize) usize {
    return @as(usize, block_min) << blockClass(size);
}

fn blockClass(size: usize) std.math.Log2Int(usize) {
    if (size <= block_min) return 0;
    return @intCast(std.math.log2_int_ceil(usize, size) - comptime std.math.log2_int(usize, block_min));
}

/// Returns the pooled blocks to `memory`. Optional: freeing the run's memory frees them too.
/// Blocks of coroutines that are still running are not pooled and are not freed here.
pub fn releasePooledBlocks(w: *WorkflowIo) void {
    for (&w.free_blocks, 0..) |*head, class| {
        const len = @as(usize, block_min) << @intCast(class);
        while (head.*) |b| {
            head.* = b.next;
            const p: [*]u8 = @ptrCast(b);
            w.memory.rawFree(p[0..len], block_align, @returnAddress());
        }
    }
}

fn taskMain(w: *WorkflowIo, task: *Task) void {
    task.start(task.context.ptr, task.result.ptr);
    task.done = true;
    switch (task.kind) {
        .future => if (task.awaiter) |a| w.wakeWait(a),
        .group => {
            const g = task.group.?;
            g.pending -= 1;
            unlinkGroupTask(g, task);
            w.destroyTask(task);
            if (g.pending == 0) if (g.awaiter) |a| w.wakeWait(a);
        },
        .detached => w.destroyTask(task),
    }
}

fn unlinkGroupTask(g: *GroupState, task: *Task) void {
    var link = &g.head;
    while (link.*) |t| : (link = &t.group_next) {
        if (t == task) {
            link.* = t.group_next;
            return;
        }
    }
}

fn currentTask(w: *WorkflowIo) ?*Task {
    const f = w.scheduler.currentFiber() orelse return null;
    return @ptrCast(@alignCast(f.user orelse return null));
}

fn requestCancel(w: *WorkflowIo, task: *Task) void {
    if (task.done) return;
    task.cancel_requested = true;
    if (task.cancel_protection == .unblocked) {
        if (task.blocked_in) |wait| w.wakeWait(wait);
    }
}

// ---------------------------------------------------------------- blocking

fn wakeWait(w: *WorkflowIo, wait: *Wait) void {
    if (wait.woken) return;
    wait.woken = true;
    if (wait.fiber.state == .blocked) w.scheduler.wake(wait.fiber);
}

/// Blocks the current coroutine until `wait` is woken. A cancelable block also returns
/// `error.Canceled` when the current task has a pending, unblocked cancel request.
fn block(w: *WorkflowIo, wait: *Wait, cancelable: bool) Io.Cancelable!void {
    const task = w.currentTask();
    while (true) {
        if (cancelable) if (task) |t| {
            if (t.cancel_requested and t.cancel_protection == .unblocked) {
                t.cancel_requested = false;
                return error.Canceled;
            }
        };
        if (wait.woken) return;
        if (cancelable) if (task) |t| {
            t.blocked_in = wait;
        };
        w.scheduler.block();
        if (task) |t| t.blocked_in = null;
    }
}

fn newWait(w: *WorkflowIo) Wait {
    return .{ .fiber = w.scheduler.currentFiber() orelse
        @panic("a workflow Io operation that blocks was called outside a workflow coroutine") };
}

fn futexEnqueue(w: *WorkflowIo, wait: *Wait) void {
    wait.prev = w.futex_tail;
    wait.next = null;
    if (w.futex_tail) |t| t.next = wait else w.futex_head = wait;
    w.futex_tail = wait;
}

fn futexRemove(w: *WorkflowIo, wait: *Wait) void {
    if (wait.futex_ptr == null) return;
    if (wait.prev) |p| p.next = wait.next else w.futex_head = wait.next;
    if (wait.next) |n| n.prev = wait.prev else w.futex_tail = wait.prev;
    wait.futex_ptr = null;
    wait.next = null;
    wait.prev = null;
}

fn timeoutDuration(w: *WorkflowIo, timeout: Io.Timeout) ?Io.Duration {
    return switch (timeout) {
        .none => null,
        .duration => |d| d.raw,
        .deadline => |t| w.host.vtable.now(w.host.ctx).durationTo(t.raw),
    };
}

// ---------------------------------------------------------------- vtable

const vtable: Io.VTable = blk: {
    var v = Io.failing.vtable.*;
    v.async = async;
    v.concurrent = concurrent;
    v.await = await;
    v.cancel = cancel;
    v.groupAsync = groupAsync;
    v.groupConcurrent = groupConcurrent;
    v.groupAwait = groupAwait;
    v.groupCancel = groupCancel;
    v.recancel = recancel;
    v.swapCancelProtection = swapCancelProtection;
    v.checkCancel = checkCancel;
    v.futexWait = futexWait;
    v.futexWaitUncancelable = futexWaitUncancelable;
    v.futexWake = futexWake;
    v.now = now;
    v.clockResolution = clockResolution;
    v.sleep = sleep;
    v.random = random;
    v.randomSecure = randomSecure;
    break :blk v;
};

fn self(userdata: ?*anyopaque) *WorkflowIo {
    return @ptrCast(@alignCast(userdata.?));
}

fn async(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*Io.AnyFuture {
    // Always a coroutine, never run inline, so ordering doesn't depend on allocation success.
    return concurrent(userdata, result.len, result_alignment, context, context_alignment, start) catch
        @panic("out of memory starting a workflow coroutine");
}

fn concurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) Io.ConcurrentError!*Io.AnyFuture {
    const w = self(userdata);
    const task = w.createTask("async", context, context_alignment, result_len, result_alignment, start, .future) catch
        return error.ConcurrencyUnavailable;
    return @ptrCast(task);
}

fn awaitTask(w: *WorkflowIo, task: *Task, result: []u8) void {
    if (!task.done) {
        var wait = w.newWait();
        task.awaiter = &wait;
        w.block(&wait, false) catch unreachable;
        assert(task.done);
    }
    @memcpy(result, task.result);
    w.destroyTask(task);
}

fn await(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, _: Alignment) void {
    const w = self(userdata);
    w.awaitTask(@ptrCast(@alignCast(any_future)), result);
}

fn cancel(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, _: Alignment) void {
    const w = self(userdata);
    const task: *Task = @ptrCast(@alignCast(any_future));
    w.requestCancel(task);
    w.awaitTask(task, result);
}

fn groupState(w: *WorkflowIo, group: *Io.Group) !*GroupState {
    if (group.token.raw) |t| return @ptrCast(@alignCast(t));
    const mem = try w.allocBlock(@sizeOf(GroupState), .of(GroupState));
    const g: *GroupState = @ptrCast(@alignCast(mem.ptr));
    g.* = .{};
    group.token.raw = g;
    return g;
}

fn groupConcurrent(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) Io.ConcurrentError!void {
    const w = self(userdata);
    const g = w.groupState(group) catch return error.ConcurrencyUnavailable;
    const Erased = struct {
        fn run(ctx: *const anyopaque, result: *anyopaque) void {
            // The group start function rides in the result slot, which group tasks don't use.
            const f: *const *const fn (context: *const anyopaque) void = @ptrCast(@alignCast(result));
            f.*(ctx);
        }
    };
    const task = w.createTask("group", context, context_alignment, @sizeOf(usize), .of(usize), Erased.run, .group) catch
        return error.ConcurrencyUnavailable;
    const slot: **const fn (context: *const anyopaque) void = @ptrCast(@alignCast(task.result.ptr));
    slot.* = start;
    task.group = g;
    // Append so cancellation order matches start order.
    var link = &g.head;
    while (link.*) |t| link = &t.group_next;
    link.* = task;
    g.pending += 1;
}

fn groupAsync(
    userdata: ?*anyopaque,
    group: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    groupConcurrent(userdata, group, context, context_alignment, start) catch
        @panic("out of memory starting a workflow coroutine");
}

fn finishGroup(w: *WorkflowIo, group: *Io.Group, g: *GroupState) void {
    assert(g.pending == 0);
    const p: [*]u8 = @ptrCast(g);
    w.freeBlock(p[0..blockLen(@sizeOf(GroupState))], .of(GroupState));
    group.token.raw = null;
}

fn groupAwait(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) Io.Cancelable!void {
    const w = self(userdata);
    const g: *GroupState = @ptrCast(@alignCast(token));
    if (g.pending > 0) {
        var wait = w.newWait();
        g.awaiter = &wait;
        defer g.awaiter = null;
        try w.block(&wait, true);
    }
    w.finishGroup(group, g);
}

fn groupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    const w = self(userdata);
    const g: *GroupState = @ptrCast(@alignCast(token));
    var it = g.head;
    while (it) |t| : (it = t.group_next) w.requestCancel(t);
    if (g.pending > 0) {
        var wait = w.newWait();
        g.awaiter = &wait;
        defer g.awaiter = null;
        w.block(&wait, false) catch unreachable;
    }
    w.finishGroup(group, g);
}

fn recancel(userdata: ?*anyopaque) void {
    const w = self(userdata);
    if (w.currentTask()) |t| t.cancel_requested = true;
}

fn swapCancelProtection(userdata: ?*anyopaque, new: Io.CancelProtection) Io.CancelProtection {
    const w = self(userdata);
    const t = w.currentTask() orelse return .unblocked;
    const old = t.cancel_protection;
    t.cancel_protection = new;
    return old;
}

fn checkCancel(userdata: ?*anyopaque) Io.Cancelable!void {
    const w = self(userdata);
    const t = w.currentTask() orelse return;
    if (t.cancel_requested and t.cancel_protection == .unblocked) {
        t.cancel_requested = false;
        return error.Canceled;
    }
}

fn futexWaitInner(w: *WorkflowIo, ptr: *const u32, expected: u32, timeout: Io.Timeout, cancelable: bool) Io.Cancelable!void {
    if (ptr.* != expected) return;
    var wait = w.newWait();
    wait.futex_ptr = ptr;
    w.futexEnqueue(&wait);
    defer w.futexRemove(&wait);

    var timer: Timer = .{ .wait = &wait };
    const duration = w.timeoutDuration(timeout);
    if (duration) |d| {
        if (d.nanoseconds <= 0) return;
        w.host.vtable.startTimer(w.host.ctx, &timer, d);
    }
    defer if (duration != null and !timer.fired) w.host.vtable.cancelTimer(w.host.ctx, &timer);
    try w.block(&wait, cancelable);
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    return self(userdata).futexWaitInner(ptr, expected, timeout, true);
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    self(userdata).futexWaitInner(ptr, expected, .none, false) catch unreachable;
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const w = self(userdata);
    var remaining = max_waiters;
    var it = w.futex_head;
    while (it) |wait| {
        if (remaining == 0) break;
        it = wait.next;
        if (wait.futex_ptr == ptr) {
            w.futexRemove(wait);
            w.wakeWait(wait);
            remaining -= 1;
        }
    }
}

fn now(userdata: ?*anyopaque, _: Io.Clock) Io.Timestamp {
    const w = self(userdata);
    return w.host.vtable.now(w.host.ctx);
}

fn clockResolution(_: ?*anyopaque, _: Io.Clock) Io.Clock.ResolutionError!Io.Duration {
    // Workflow time comes from server event timestamps, which have millisecond precision.
    return .fromMilliseconds(1);
}

fn sleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const w = self(userdata);
    const d = w.timeoutDuration(timeout) orelse {
        // Sleeping forever: wait for cancellation.
        var wait = w.newWait();
        return w.block(&wait, true);
    };
    try checkCancel(userdata);
    if (d.nanoseconds <= 0) return;
    var wait = w.newWait();
    var timer: Timer = .{ .wait = &wait };
    w.host.vtable.startTimer(w.host.ctx, &timer, d);
    w.block(&wait, true) catch |err| {
        if (!timer.fired) w.host.vtable.cancelTimer(w.host.ctx, &timer);
        return err;
    };
    assert(timer.fired);
}

fn random(userdata: ?*anyopaque, buffer: []u8) void {
    self(userdata).rng.fill(buffer);
}

fn randomSecure(userdata: ?*anyopaque, _: []u8) Io.RandomSecureError!void {
    self(userdata).determinism_violations += 1;
    return error.EntropyUnavailable;
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

/// A host whose timers fire only when the test says so, in deadline order.
const FakeHost = struct {
    time_ns: i96 = 1_700_000_000 * std.time.ns_per_s,
    timers: std.ArrayList(Pending) = .empty,
    started: u32 = 0,
    canceled: u32 = 0,

    const Pending = struct { timer: *Timer, deadline_ns: i96, seq: u32 };

    fn host(h: *FakeHost) Host {
        return .{ .ctx = h, .vtable = &.{ .now = nowFn, .startTimer = startFn, .cancelTimer = cancelFn } };
    }

    fn nowFn(ctx: *anyopaque) Io.Timestamp {
        const h: *FakeHost = @ptrCast(@alignCast(ctx));
        return .{ .nanoseconds = h.time_ns };
    }

    fn startFn(ctx: *anyopaque, timer: *Timer, d: Io.Duration) void {
        const h: *FakeHost = @ptrCast(@alignCast(ctx));
        h.started += 1;
        timer.host_data = h.started;
        h.timers.append(testing.allocator, .{ .timer = timer, .deadline_ns = h.time_ns + d.nanoseconds, .seq = h.started }) catch @panic("OOM");
    }

    fn cancelFn(ctx: *anyopaque, timer: *Timer) void {
        const h: *FakeHost = @ptrCast(@alignCast(ctx));
        for (h.timers.items, 0..) |p, i| if (p.timer == timer) {
            _ = h.timers.orderedRemove(i);
            h.canceled += 1;
            return;
        };
        unreachable;
    }

    /// Advances time to the earliest timer and fires it. Returns false if none are pending.
    fn fireNext(h: *FakeHost, w: *WorkflowIo) bool {
        if (h.timers.items.len == 0) return false;
        var best: usize = 0;
        for (h.timers.items, 0..) |p, i| {
            const b = h.timers.items[best];
            if (p.deadline_ns < b.deadline_ns or (p.deadline_ns == b.deadline_ns and p.seq < b.seq)) best = i;
        }
        const p = h.timers.orderedRemove(best);
        h.time_ns = p.deadline_ns;
        w.fireTimer(p.timer);
        return true;
    }

    /// Runs coroutines, firing timers whenever everything is blocked, until no timers remain.
    fn runAll(h: *FakeHost, w: *WorkflowIo) void {
        w.run();
        while (h.fireNext(w)) w.run();
    }

    fn deinit(h: *FakeHost) void {
        h.timers.deinit(testing.allocator);
    }
};

const Env = struct {
    sched: Scheduler,
    fake: FakeHost,
    wio: WorkflowIo,

    fn init(e: *Env) void {
        e.sched = .init(testing.allocator, .{ .stack_size = 256 * 1024 });
        e.fake = .{};
        e.wio = .init(testing.allocator, &e.sched, e.fake.host(), 42);
    }

    fn deinit(e: *Env) void {
        e.wio.releasePooledBlocks();
        e.fake.deinit();
        e.sched.deinit();
    }
};

const Trace = struct {
    buf: [64]u32 = undefined,
    len: usize = 0,
    fn add(t: *Trace, v: u32) void {
        t.buf[t.len] = v;
        t.len += 1;
    }
    fn items(t: *const Trace) []const u32 {
        return t.buf[0..t.len];
    }
};

fn square(x: u64) u64 {
    return x * x;
}

fn futuresWorkflow(i: Io, out: *u64) void {
    var a = i.async(square, .{3});
    var b = i.async(square, .{4});
    out.* = a.await(i) + b.await(i);
}

test "io.async futures run as coroutines and return results" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var out: u64 = 0;
    _ = try e.wio.spawn("wf", futuresWorkflow, .{ e.wio.io(), &out });
    e.wio.run();
    try testing.expectEqual(@as(u64, 25), out);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn napThenRecord(i: Io, trace: *Trace, id: u32, secs: i64) void {
    i.sleep(.fromSeconds(secs), .awake) catch return;
    trace.add(id);
}

fn sleepers(i: Io, trace: *Trace) void {
    var g: Io.Group = .init;
    g.async(i, napThenRecord, .{ i, trace, 3, 30 });
    g.async(i, napThenRecord, .{ i, trace, 1, 10 });
    g.async(i, napThenRecord, .{ i, trace, 2, 20 });
    g.await(i) catch unreachable;
    trace.add(99);
}

test "sleep is a durable timer and groups wait for all members" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var trace: Trace = .{};
    _ = try e.wio.spawn("wf", sleepers, .{ e.wio.io(), &trace });
    e.wio.run();
    try testing.expectEqual(@as(usize, 3), e.fake.timers.items.len);
    try testing.expectEqual(@as(usize, 0), trace.len);
    e.fake.runAll(&e.wio);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3, 99 }, trace.items());
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn sleepForever(i: Io, result: *?anyerror) void {
    i.sleep(.fromSeconds(3600), .awake) catch |err| {
        result.* = err;
        return;
    };
    result.* = null;
}

fn cancelSleeper(i: Io, result: *?anyerror) void {
    var f = i.async(sleepForever, .{ i, result });
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    f.cancel(i);
}

test "canceling a sleeping future cancels its timer and delivers error.Canceled" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var result: ?anyerror = null;
    _ = try e.wio.spawn("wf", cancelSleeper, .{ e.wio.io(), &result });
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(?anyerror, error.Canceled), result);
    try testing.expectEqual(@as(u32, 1), e.fake.canceled);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn protectedSleep(i: Io, result: *?anyerror) void {
    const old = i.swapCancelProtection(.blocked);
    defer _ = i.swapCancelProtection(old);
    i.sleep(.fromSeconds(5), .awake) catch |err| {
        result.* = err;
        return;
    };
    result.* = null;
}

fn cancelProtected(i: Io, result: *?anyerror) void {
    var f = i.async(protectedSleep, .{ i, result });
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    f.cancel(i);
}

test "cancel protection lets cleanup finish after cancellation" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var result: ?anyerror = error.Unset;
    _ = try e.wio.spawn("wf", cancelProtected, .{ e.wio.io(), &result });
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(?anyerror, null), result);
}

fn lockedIncrement(i: Io, m: *Io.Mutex, counter: *u32, trace: *Trace, id: u32) void {
    m.lockUncancelable(i);
    defer m.unlock(i);
    const v = counter.*;
    trace.add(id);
    // Hold the lock across a durable wait; another coroutine must not get in.
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    counter.* = v + 1;
}

fn mutexWorkflow(i: Io, counter: *u32, trace: *Trace) void {
    var m: Io.Mutex = .init;
    var g: Io.Group = .init;
    for (1..4) |id| g.async(i, lockedIncrement, .{ i, &m, counter, trace, @as(u32, @intCast(id)) });
    g.await(i) catch unreachable;
}

test "std.Io.Mutex gives mutual exclusion across durable waits" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var counter: u32 = 0;
    var trace: Trace = .{};
    _ = try e.wio.spawn("wf", mutexWorkflow, .{ e.wio.io(), &counter, &trace });
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(u32, 3), counter);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, trace.items());
}

fn producer(i: Io, q: *Io.Queue(u32)) void {
    for (1..6) |v| {
        q.putOne(i, @intCast(v)) catch return;
        i.sleep(.fromSeconds(1), .awake) catch return;
    }
    q.close(i);
}

fn consumer(i: Io, q: *Io.Queue(u32), trace: *Trace) void {
    while (true) {
        const v = q.getOne(i) catch return;
        trace.add(v);
    }
}

fn queueWorkflow(i: Io, trace: *Trace) void {
    var buf: [2]u32 = undefined;
    var q: Io.Queue(u32) = .init(&buf);
    var g: Io.Group = .init;
    g.async(i, consumer, .{ i, &q, trace });
    g.async(i, producer, .{ i, &q });
    g.await(i) catch unreachable;
}

test "std.Io.Queue works as a workflow channel" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var trace: Trace = .{};
    _ = try e.wio.spawn("wf", queueWorkflow, .{ e.wio.io(), &trace });
    e.fake.runAll(&e.wio);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3, 4, 5 }, trace.items());
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn napFor(i: Io, secs: i64) i64 {
    i.sleep(.fromSeconds(secs), .awake) catch return -1;
    return secs;
}

const Race = union(enum) { slow: i64, fast: i64 };

fn selectWorkflow(i: Io, winner: *?Race) void {
    var buf: [2]Race = undefined;
    var sel: Io.Select(Race) = .init(i, &buf);
    sel.async(.slow, napFor, .{ i, 20 });
    sel.async(.fast, napFor, .{ i, 5 });
    winner.* = sel.await() catch unreachable;
    sel.cancelDiscard();
}

test "std.Io.Select picks the first result and cancels the rest" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var winner: ?Race = null;
    _ = try e.wio.spawn("wf", selectWorkflow, .{ e.wio.io(), &winner });
    e.fake.runAll(&e.wio);
    try testing.expectEqual(Race{ .fast = 5 }, winner.?);
    try testing.expectEqual(@as(u32, 1), e.fake.canceled);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn timeAndRandom(i: Io, out: *[3]u64) void {
    const t0 = Io.Timestamp.now(i, .real);
    i.sleep(.fromSeconds(90), .real) catch unreachable;
    const t1 = Io.Timestamp.now(i, .awake);
    out[0] = @intCast(@divTrunc(t0.durationTo(t1).nanoseconds, std.time.ns_per_s));
    var bytes: [16]u8 = undefined;
    i.random(&bytes);
    out[1] = std.mem.readInt(u64, bytes[0..8], .little);
    out[2] = std.mem.readInt(u64, bytes[8..16], .little);
}

test "now is workflow time and random is seeded" {
    var outs: [2][3]u64 = undefined;
    for (&outs) |*out| {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        _ = try e.wio.spawn("wf", timeAndRandom, .{ e.wio.io(), out });
        e.fake.runAll(&e.wio);
    }
    try testing.expectEqual(@as(u64, 90), outs[0][0]);
    try testing.expectEqualSlices(u64, &outs[0], &outs[1]);
}

fn touchesTheOutsideWorld(i: Io, results: *[2]bool) void {
    results[0] = if (Io.Dir.cwd().openFile(i, "build.zig", .{})) |_| false else |_| true;
    var buf: [8]u8 = undefined;
    results[1] = if (i.randomSecure(&buf)) |_| false else |_| true;
}

test "file I/O and secure randomness fail inside workflows" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var results: [2]bool = .{ false, false };
    _ = try e.wio.spawn("wf", touchesTheOutsideWorld, .{ e.wio.io(), &results });
    e.wio.run();
    try testing.expect(results[0]);
    try testing.expect(results[1]);
    try testing.expectEqual(@as(u32, 1), e.wio.determinism_violations);
}

fn napLong(i: Io) Io.Cancelable!void {
    try i.sleep(.fromSeconds(100), .awake);
}

fn workflowCancelTarget(i: Io, result: *?anyerror) void {
    var g: Io.Group = .init;
    g.async(i, napLong, .{i});
    g.await(i) catch |err| {
        result.* = err;
        g.cancel(i);
        return;
    };
    result.* = null;
}

test "cancelTask delivers cancellation to a top-level coroutine" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var result: ?anyerror = null;
    const task = try e.wio.spawn("wf", workflowCancelTarget, .{ e.wio.io(), &result });
    e.wio.run();
    e.wio.cancelTask(task);
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(?anyerror, error.Canceled), result);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}
