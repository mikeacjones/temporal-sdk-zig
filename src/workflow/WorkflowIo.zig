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
//! `swapCancelProtection`. Delivering it consumes the request; `recancel` re-arms it. A wait
//! that is both woken and canceled before its coroutine runs again returns `error.Canceled`;
//! a futex wake it was given goes on to the next waiter on the same address.

const WorkflowIo = @This();

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const assert = std.debug.assert;
const Scheduler = @import("Scheduler.zig");
const call_sites = @import("call_sites.zig");
const Fiber = Scheduler.Fiber;

/// The run's allocator. Task blocks and group state come from it and go back to `free_blocks`
/// for reuse, never to it: freeing the run's memory frees them, including the blocks of
/// coroutines that never exited.
memory: Allocator,
/// Finished blocks by size class (`block_min << i` bytes), linked through their first word.
free_blocks: [block_classes]?*FreeBlock = @splat(null),
/// Finished blocks too large or too aligned for a size class, for reuse by a block of the
/// same length and alignment. The run's allocator is an arena, which can't reclaim them.
free_oversize: ?*OversizeBlock = null,
scheduler: *Scheduler,
host: Host,
rng: std.Random.DefaultPrng,
/// Coroutines blocked in a futex wait, in the order they started waiting.
futex_head: ?*Wait = null,
futex_tail: ?*Wait = null,
/// The address of a futex wake that the running coroutine was given and then gave up by
/// returning error.Canceled. The rule: such a wake reaches some other waiter on the address
/// before the coroutine gives up control, at most once, and never the coroutine itself. A
/// `futexWake` on that address from the same coroutine pays it (std.Io.Condition hands a
/// canceled waiter's signal on itself); otherwise `settleOwedWake` passes it on before the
/// coroutine queues another futex wait, blocks, or ends. Non-null only while that coroutine
/// runs, so it is never another coroutine's debt.
owed_wake: ?*const u32 = null,
/// Calls that would make replay depend on the outside world (secure randomness).
determinism_violations: u32 = 0,
/// The read-only sections that have begun and not ended, newest first (see `ReadOnly`).
read_only_sections: ?*ReadOnly = null,
/// Whether any read-only section is open, in any coroutine or outside them. The rules apply
/// only to the code each section belongs to: see `currentReadOnly`.
read_only: bool = false,
/// The first coroutine start that failed. Sticky: never cleared, since the run already went
/// on differently than it would have with the coroutine started.
start_failure: ?StartFailure = null,

/// Read-only code (a query handler, an update validator) run by the host between
/// `beginReadOnly` and `endReadOnly`. The section belongs to the coroutine it began on, or to
/// the code outside any coroutine, and governs only that: a validator run in a handler's
/// coroutine that blocks leaves every other coroutine free. In it, waits that can be canceled
/// return error.Canceled instead of blocking, `io.concurrent` and `Group.concurrent` return
/// error.ConcurrencyUnavailable, `io.async` and `Group.async` run their function inline,
/// futex wakes that resume a coroutine are recorded as refused (but still wake it: see
/// `futexWake`), and `io.random` draws from the
/// section's own copy of the generator. `refuse` records the first refused operation in
/// `refused`. Lives on the stack of the code that runs the section.
pub const ReadOnly = struct {
    fiber: ?*Fiber = null,
    refused: ?[]const u8 = null,
    rng: std.Random.DefaultPrng = undefined,
    next: ?*ReadOnly = null,
};

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

/// A coroutine `io.async` or `Group.async` (and so `Select.async`) had to start and couldn't.
/// Kept in `start_failure`; the host fails the activation with it.
pub const StartFailure = struct {
    /// "io.async" or "Group.async".
    operation: []const u8,
    cause: Cause,

    pub const Cause = enum {
        /// The coroutine's Task block (or its Group's state) couldn't be allocated from the
        /// run's memory.
        task_block,
        /// `Scheduler.spawn` couldn't get a stack: mmap or the guard page's mprotect failed.
        stack,

        fn text(c: Cause) []const u8 {
            return switch (c) {
                .task_block => "out of memory allocating its task block",
                .stack => "the operating system refused to map its stack (memory or the process's limit on memory mappings is exhausted; lower worker.Options.workflow_stack_size or max_cached_workflows)",
            };
        }
    };

    /// "<operation> could not start a workflow coroutine: <cause text>"
    pub fn format(f: StartFailure, writer: *Io.Writer) Io.Writer.Error!void {
        try writer.print("{s} could not start a workflow coroutine: {s}", .{ f.operation, f.cause.text() });
    }
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
/// `reason` says what it waits on, for `Task.waiting_on`.
pub fn newWaitPoint(w: *WorkflowIo, reason: Wait.Reason) Wait {
    return w.newWait(reason);
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

/// `wake`, but a coroutine that this wakes runs before the others that are ready.
pub fn wakeFirst(w: *WorkflowIo, wait: *Wait) void {
    if (wait.woken) return;
    wait.woken = true;
    if (wait.fiber.state == .blocked) w.scheduler.wakeFirst(wait.fiber);
}

/// The running coroutine's name: the one `spawn` gave it, "async" or "group" for coroutines
/// started through the `std.Io` interface, or the last one `setCurrentName` set. Empty
/// outside a coroutine.
pub fn currentName(w: *const WorkflowIo) []const u8 {
    const f = w.scheduler.currentFiber() orelse return "";
    return f.name;
}

/// Renames the running coroutine. `name` is not copied and must outlive the coroutine. Does
/// nothing outside a coroutine.
pub fn setCurrentName(w: *WorkflowIo, name: []const u8) void {
    const f = w.scheduler.currentFiber() orelse return;
    f.name = name;
}

/// Opens `section` for the running coroutine (or for the code outside any coroutine).
pub fn beginReadOnly(w: *WorkflowIo, section: *ReadOnly) void {
    section.* = .{ .fiber = w.scheduler.currentFiber(), .rng = w.rng, .next = w.read_only_sections };
    w.read_only_sections = section;
    w.read_only = true;
}

/// Closes `section`, which other sections may have opened after and still hold open.
pub fn endReadOnly(w: *WorkflowIo, section: *ReadOnly) void {
    var link = &w.read_only_sections;
    while (link.*) |r| : (link = &r.next) if (r == section) {
        link.* = r.next;
        break;
    };
    w.read_only = w.read_only_sections != null;
}

/// The innermost read-only section of the code running now, or null.
pub fn currentReadOnly(w: *const WorkflowIo) ?*ReadOnly {
    var it = w.read_only_sections orelse return null;
    const fiber = w.scheduler.currentFiber();
    while (true) {
        if (it.fiber == fiber) return it;
        it = it.next orelse return null;
    }
}

/// Whether `operation` must be refused because the code running now is read-only. The first
/// refused operation is kept in its section's `refused` (`operation` must outlive it).
pub fn refuse(w: *WorkflowIo, operation: []const u8) bool {
    const section = w.currentReadOnly() orelse return false;
    if (section.refused == null) section.refused = operation;
    return true;
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
    /// The wait this task is blocked in, cancelable or not, if any: for describing where the
    /// coroutine waits (the stack trace query). Set and cleared around each block, with no
    /// allocation.
    waiting_on: ?*const Wait = null,
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
    /// Set when `futexWake` chose this wait, so a wait that then fails can pass the wake on.
    futex_woken: bool = false,
    // Futex wait list link and address.
    futex_ptr: ?*const u32 = null,
    next: ?*Wait = null,
    prev: ?*Wait = null,
    reason: Reason,

    /// What a wait waits on, as text: `what` ("a timer", "activity"), then `name` quoted
    /// when set (an activity type, escaped with `call_sites.text`), then `seq` when set (an activity's, when its name isn't
    /// known), then `duration` in parentheses when set (a timer's). The strings are borrowed
    /// and must outlive the wait; they are static, belong to the run, or are on the waiting
    /// coroutine's stack.
    pub const Reason = struct {
        what: []const u8,
        name: []const u8 = "",
        seq: ?u32 = null,
        duration: ?Io.Duration = null,

        pub fn format(r: Reason, writer: *Io.Writer) Io.Writer.Error!void {
            try writer.writeAll(r.what);
            if (r.name.len > 0) try writer.print(" \"{f}\"", .{call_sites.text(r.name)});
            if (r.seq) |seq| try writer.print(" (seq {d})", .{seq});
            if (r.duration) |d| try writer.print(" ({f})", .{d});
        }
    };
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
) error{ OutOfMemory, StackUnavailable }!*Task {
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
    task.fiber = w.scheduler.spawn(name, taskMain, .{ w, task }) catch return error.StackUnavailable;
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
const free_poison: u8 = 0xdf;
const OversizeBlock = struct { next: ?*OversizeBlock, len: usize, alignment: Alignment };

fn isOversize(size: usize, alignment: Alignment) bool {
    return size > block_max or alignment.compare(.gt, block_align);
}

/// A block of at least `size` bytes. Pooled blocks are a whole size class long, so the
/// returned slice may be longer than `size`; pass it unchanged to `freeBlock`.
fn allocBlock(w: *WorkflowIo, size: usize, alignment: Alignment) error{OutOfMemory}![]u8 {
    if (isOversize(size, alignment)) {
        var link = &w.free_oversize;
        while (link.*) |b| : (link = &b.next) {
            if (b.len == size and b.alignment == alignment) {
                link.* = b.next;
                const p: [*]u8 = @ptrCast(b);
                return p[0..size];
            }
        }
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
    if (isOversize(mem.len, alignment)) {
        // Only Task blocks are oversize, and a Task has room and alignment for the link.
        comptime assert(@sizeOf(Task) >= @sizeOf(OversizeBlock) and @alignOf(Task) >= @alignOf(OversizeBlock));
        const b: *OversizeBlock = @ptrCast(@alignCast(mem.ptr));
        b.* = .{ .next = w.free_oversize, .len = mem.len, .alignment = alignment };
        w.free_oversize = b;
        return;
    }
    const class = blockClass(mem.len);
    assert(mem.len == @as(usize, block_min) << class);
    // Tests check that no freed block is written to before it is reused.
    if (builtin.is_test) @memset(mem[@sizeOf(FreeBlock)..], free_poison);
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
    while (w.free_oversize) |b| {
        w.free_oversize = b.next;
        const p: [*]u8 = @ptrCast(b);
        w.memory.rawFree(p[0..b.len], b.alignment, @returnAddress());
    }
}

fn taskMain(w: *WorkflowIo, task: *Task) void {
    task.start(task.context.ptr, task.result.ptr);
    w.settleOwedWake();
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
/// `error.Canceled` when the current task has a pending, unblocked cancel request, and the
/// cancel wins when the wait was both woken and canceled before the coroutine ran again: it
/// returns `error.Canceled`, so the wait's outcome doesn't depend on the order of the two.
/// A futex wait passes such a wake on unless its coroutine wakes the address itself first
/// (see `owed_wake`); other callers own their waits.
fn block(w: *WorkflowIo, wait: *Wait, cancelable: bool) Io.Cancelable!void {
    const task = w.ownCancelTask();
    // Read-only code reaches here only through a wait that can't fail, so can't be refused: it
    // blocks its own coroutine (and only that one), and its query or update fails afterwards.
    if (!wait.woken) _ = w.refuse("a wait that can't be refused (an uncancelable std.Io wait, Future.await, Group.cancel)");
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
        w.settleOwedWake();
        const current = w.currentTask();
        if (current) |t| t.waiting_on = wait;
        w.scheduler.block();
        if (current) |t| t.waiting_on = null;
        if (task) |t| t.blocked_in = null;
    }
}

fn newWait(w: *WorkflowIo, reason: Wait.Reason) Wait {
    return .{ .reason = reason, .fiber = w.scheduler.currentFiber() orelse
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
    const w = self(userdata);
    // Read-only code may not start a coroutine, which would run after it returned.
    if (w.currentReadOnly() != null) {
        start(context.ptr, result.ptr);
        return null;
    }
    return w.startFuture(result.len, result_alignment, context, context_alignment, start) catch |err| {
        // Run inline so the caller's future has its result. The run already differs from one
        // where the coroutine started, so the activation fails and its commands are never
        // sent: the inline run never reaches history.
        w.recordStartFailure("io.async", err);
        start(context.ptr, result.ptr);
        return null;
    };
}

fn recordStartFailure(w: *WorkflowIo, operation: []const u8, err: error{ OutOfMemory, StackUnavailable }) void {
    if (w.start_failure != null) return;
    w.start_failure = .{ .operation = operation, .cause = switch (err) {
        error.OutOfMemory => .task_block,
        error.StackUnavailable => .stack,
    } };
}

fn startFuture(
    w: *WorkflowIo,
    result_len: usize,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) error{ OutOfMemory, StackUnavailable }!*Io.AnyFuture {
    const task = try w.createTask("async", context, context_alignment, result_len, result_alignment, start, .future);
    return @ptrCast(task);
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
    if (w.refuse("io.concurrent")) return error.ConcurrencyUnavailable;
    return w.startFuture(result_len, result_alignment, context, context_alignment, start) catch
        error.ConcurrencyUnavailable;
}

/// Waits for `task` to finish and copies its result out. With `pass_cancel`, a cancellation
/// of the waiting coroutine is passed on to `task`, as `std.Io.Threaded` does in `await`: the
/// wait goes on until `task` finishes, and if `task` didn't consume the cancellation (it had
/// already finished, or never reached a cancelable wait), the waiting coroutine's
/// cancellation is re-armed for its next cancelable wait.
fn awaitTask(w: *WorkflowIo, task: *Task, result: []u8, pass_cancel: bool) void {
    if (!task.done) {
        var wait = w.newWait(.{ .what = "the result of a coroutine started with io.async or io.concurrent (Future.await or Future.cancel)" });
        task.awaiter = &wait;
        if (w.block(&wait, pass_cancel)) |_| {} else |_| {
            const was_running = !task.done;
            w.requestCancel(task);
            // The cancel that ended the block woke `wait`; wait on a fresh one.
            while (!task.done) {
                wait = w.newWait(.{ .what = "the result of a coroutine started with io.async or io.concurrent (Future.await or Future.cancel)" });
                w.block(&wait, false) catch unreachable;
            }
            if (!was_running or task.cancel_requested) {
                if (w.ownCancelTask()) |t| t.cancel_requested = true;
            }
        }
        assert(task.done);
    }
    @memcpy(result, task.result);
    w.destroyTask(task);
}

fn await(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, _: Alignment) void {
    const w = self(userdata);
    w.awaitTask(@ptrCast(@alignCast(any_future)), result, true);
}

fn cancel(userdata: ?*anyopaque, any_future: *Io.AnyFuture, result: []u8, _: Alignment) void {
    const w = self(userdata);
    const task: *Task = @ptrCast(@alignCast(any_future));
    w.requestCancel(task);
    w.awaitTask(task, result, false);
}

fn groupState(w: *WorkflowIo, group: *Io.Group) error{OutOfMemory}!*GroupState {
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
    if (w.refuse("Group.concurrent")) return error.ConcurrencyUnavailable;
    return w.startGroupTask(group, context, context_alignment, start) catch error.ConcurrencyUnavailable;
}

fn startGroupTask(
    w: *WorkflowIo,
    group: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) error{ OutOfMemory, StackUnavailable }!void {
    const g = try w.groupState(group);
    const Erased = struct {
        fn run(ctx: *const anyopaque, result: *anyopaque) void {
            // The group start function rides in the result slot, which group tasks don't use.
            const f: *const *const fn (context: *const anyopaque) void = @ptrCast(@alignCast(result));
            f.*(ctx);
        }
    };
    const task = try w.createTask("group", context, context_alignment, @sizeOf(usize), .of(usize), Erased.run, .group);
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
    const w = self(userdata);
    if (w.currentReadOnly() != null) return start(context.ptr);
    w.startGroupTask(group, context, context_alignment, start) catch |err| {
        // As in `async`: the activation fails, so the inline run never reaches history.
        w.recordStartFailure("Group.async", err);
        start(context.ptr);
    };
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
    var result: Io.Cancelable!void = {};
    if (g.pending > 0) {
        if (w.refuse("Group.await")) return error.Canceled;
        var wait = w.newWait(.{ .what = "the coroutines of a std.Io.Group (Group.await)" });
        g.awaiter = &wait;
        // Runs before finishGroup below frees `g`.
        defer g.awaiter = null;
        w.block(&wait, true) catch |err| {
            // As `std.Io.Group.await` says: the cancellation goes to every member, and the
            // group finishes before the error is returned.
            var it = g.head;
            while (it) |t| : (it = t.group_next) w.requestCancel(t);
            while (g.pending > 0) {
                wait = w.newWait(.{ .what = "the coroutines of a std.Io.Group (Group.await)" });
                w.block(&wait, false) catch unreachable;
            }
            result = err;
        };
    }
    w.finishGroup(group, g);
    return result;
}

fn groupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    const w = self(userdata);
    const g: *GroupState = @ptrCast(@alignCast(token));
    var it = g.head;
    while (it) |t| : (it = t.group_next) w.requestCancel(t);
    if (g.pending > 0) {
        var wait = w.newWait(.{ .what = "the coroutines of a std.Io.Group (Group.cancel)" });
        g.awaiter = &wait;
        defer g.awaiter = null;
        w.block(&wait, false) catch unreachable;
    }
    w.finishGroup(group, g);
}

/// The running coroutine's task, for its cancellation state; null outside a coroutine and in
/// read-only code. A validator can run inside a coroutine (the workflow's or a handler's), and
/// that coroutine's cancel request and protection belong to it: read-only code mustn't
/// consume, set or change them, and sees an uncanceled task.
fn ownCancelTask(w: *WorkflowIo) ?*Task {
    if (w.currentReadOnly() != null) return null;
    return w.currentTask();
}

fn recancel(userdata: ?*anyopaque) void {
    const w = self(userdata);
    if (w.ownCancelTask()) |t| t.cancel_requested = true;
}

fn swapCancelProtection(userdata: ?*anyopaque, new: Io.CancelProtection) Io.CancelProtection {
    const w = self(userdata);
    const t = w.ownCancelTask() orelse return .unblocked;
    const old = t.cancel_protection;
    t.cancel_protection = new;
    return old;
}

fn checkCancel(userdata: ?*anyopaque) Io.Cancelable!void {
    const w = self(userdata);
    const t = w.ownCancelTask() orelse return;
    if (t.cancel_requested and t.cancel_protection == .unblocked) {
        t.cancel_requested = false;
        return error.Canceled;
    }
}

fn futexWaitInner(w: *WorkflowIo, ptr: *const u32, expected: u32, timeout: Io.Timeout, cancelable: bool) Io.Cancelable!void {
    if (ptr.* != expected) return;
    const duration = w.timeoutDuration(timeout);
    if (duration) |d| if (d.nanoseconds <= 0) return;
    // An uncancelable wait can't be refused: it blocks (or, outside a coroutine, panics).
    if (cancelable and w.refuse("a futex wait (std.Io.Mutex, Condition, Event, Queue)")) return error.Canceled;
    // Before this coroutine's own wait is queued, where a wake on the same address (a relock
    // of the same Mutex, a Condition at the same stack address) would land on it.
    w.settleOwedWake();
    var wait = w.newWait(.{
        .what = if (duration == null) "a std.Io Mutex, Condition, Event, Semaphore or Queue" else "a std.Io Mutex, Condition, Event, Semaphore or Queue, with a timeout",
        .duration = duration,
    });
    wait.futex_ptr = ptr;
    w.futexEnqueue(&wait);
    defer w.futexRemove(&wait);

    var timer: Timer = .{ .wait = &wait };
    if (duration) |d| {
        w.host.vtable.startTimer(w.host.ctx, &timer, d);
    }
    defer if (duration != null and !timer.fired) w.host.vtable.cancelTimer(w.host.ctx, &timer);
    w.block(&wait, cancelable) catch |err| {
        // A wake that `futexWake` gave this wait would be lost with it: a single-waiter wake
        // (Mutex.unlock) would leave the next waiter blocked on a free primitive. Owe it to
        // the next waiter rather than wake one now: the caller may hand it on itself, as
        // Condition does, and a second wake would make a timed waiter restart its timer.
        if (wait.futex_woken) {
            assert(w.owed_wake == null);
            w.owed_wake = ptr;
        }
        return err;
    };
}

/// Passes an owed futex wake (see `owed_wake`) to the next waiter on its address. Called
/// before the running coroutine queues a futex wait, blocks, or ends, so the wake can't
/// outlive it, and only while none of its own waits is queued, so the wake can't land on it.
fn settleOwedWake(w: *WorkflowIo) void {
    const ptr = w.owed_wake orelse return;
    w.owed_wake = null;
    if (std.debug.runtime_safety) {
        const current = w.scheduler.currentFiber();
        var it = w.futex_head;
        while (it) |wait| : (it = wait.next) assert(wait.fiber != current);
    }
    futexWake(w, ptr, 1);
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    return self(userdata).futexWaitInner(ptr, expected, timeout, true);
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    self(userdata).futexWaitInner(ptr, expected, .none, false) catch unreachable;
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const w = self(userdata);
    if (w.owed_wake == ptr and max_waiters > 0) w.owed_wake = null;
    if (w.currentReadOnly() != null) {
        // Read-only code changed a std.Io primitive a coroutine waits on. std.Io already changed
        // the primitive's memory (a Condition's count, a Queue's buffer) before this call, and
        // that can't be undone, so the wake goes through to keep the primitive consistent; the
        // read-only code's query fails, or its update is rejected, for it.
        var it = w.futex_head;
        while (it) |wait| : (it = wait.next) if (wait.futex_ptr == ptr) {
            _ = w.refuse("a futex wake that resumes a workflow coroutine (std.Io.Event.set, Mutex.unlock, Queue.put, ...)");
            break;
        };
    }
    var remaining = max_waiters;
    var it = w.futex_head;
    while (it) |wait| {
        if (remaining == 0) break;
        it = wait.next;
        if (wait.futex_ptr == ptr) {
            w.futexRemove(wait);
            wait.futex_woken = true;
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
    const d = w.timeoutDuration(timeout);
    // Read-only code returns at once from a sleep that needs no timer, and is refused one that
    // does, before anything reads the coroutine's cancel request (see `ownCancelTask`).
    if (d == null or d.?.nanoseconds > 0) {
        if (w.refuse("sleep")) return error.Canceled;
    } else if (w.currentReadOnly() != null) return;
    const duration = d orelse {
        // Sleeping forever: wait for cancellation.
        var wait = w.newWait(.{ .what = "a sleep with no timeout, until canceled" });
        return w.block(&wait, true);
    };
    try checkCancel(userdata);
    if (duration.nanoseconds <= 0) return;
    var wait = w.newWait(.{ .what = "a timer", .duration = duration });
    var timer: Timer = .{ .wait = &wait };
    w.host.vtable.startTimer(w.host.ctx, &timer, duration);
    w.block(&wait, true) catch |err| {
        if (!timer.fired) w.host.vtable.cancelTimer(w.host.ctx, &timer);
        return err;
    };
    assert(timer.fired);
}

fn random(userdata: ?*anyopaque, buffer: []u8) void {
    const w = self(userdata);
    // Read-only code draws from its section's copy, leaving the run's sequence as it was.
    if (w.currentReadOnly()) |section| return section.rng.fill(buffer);
    w.rng.fill(buffer);
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

/// Awaits a future that sleeps for an hour (`sleepForever`, or `protectedSleep` when
/// `protected`), then records how its own next cancelable wait ends in `after`.
fn awaitSleeper(i: Io, inner: *?anyerror, protected: bool, after: *?anyerror) void {
    if (protected) {
        var f = i.async(protectedSleep, .{ i, inner });
        f.await(i);
    } else {
        var f = i.async(sleepForever, .{ i, inner });
        f.await(i);
    }
    i.sleep(.fromSeconds(1), .awake) catch |err| {
        after.* = err;
        return;
    };
    after.* = null;
}

test "canceling a coroutine waiting in Future.await cancels the awaited one, which consumes it" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var inner: ?anyerror = error.Unset;
    var after: ?anyerror = error.Unset;
    const task = try e.wio.spawn("wf", awaitSleeper, .{ e.wio.io(), &inner, false, &after });
    e.wio.run();
    e.wio.cancelTask(task);
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(?anyerror, error.Canceled), inner);
    // The awaited coroutine took the cancellation; the awaiting one goes on uncanceled.
    try testing.expectEqual(@as(?anyerror, null), after);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

test "a cancellation the awaited future doesn't consume stays with the coroutine waiting in Future.await" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var inner: ?anyerror = error.Unset;
    var after: ?anyerror = error.Unset;
    const task = try e.wio.spawn("wf", awaitSleeper, .{ e.wio.io(), &inner, true, &after });
    e.wio.run();
    e.wio.cancelTask(task);
    e.fake.runAll(&e.wio);
    // The protected sleep finished; its awaiter's next cancelable wait gets the cancellation.
    try testing.expectEqual(@as(?anyerror, null), inner);
    try testing.expectEqual(@as(?anyerror, error.Canceled), after);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

/// Awaits a group of two hour-long sleepers and records how the await ended.
fn awaitSleeperGroup(i: Io, inner: *[2]?anyerror, result: *?anyerror) void {
    var g: Io.Group = .init;
    for (inner) |*r| g.async(i, sleepForever, .{ i, r });
    g.await(i) catch |err| {
        result.* = err;
        return;
    };
    result.* = null;
}

test "canceling a coroutine waiting in Group.await cancels every member and returns once they finished" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var inner: [2]?anyerror = .{ error.Unset, error.Unset };
    var result: ?anyerror = error.Unset;
    const task = try e.wio.spawn("wf", awaitSleeperGroup, .{ e.wio.io(), &inner, &result });
    e.wio.run();
    e.wio.cancelTask(task);
    e.wio.run();
    try testing.expectEqual(@as(?anyerror, error.Canceled), result);
    try testing.expectEqual(@as(?anyerror, error.Canceled), inner[0]);
    try testing.expectEqual(@as(?anyerror, error.Canceled), inner[1]);
    try testing.expectEqual(@as(u32, 2), e.fake.canceled);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

/// Fails if anything wrote to a pooled block after freeBlock put it on the free list.
fn expectFreeBlocksUntouched(w: *const WorkflowIo) !void {
    var free: usize = 0;
    for (w.free_blocks, 0..) |head, class| {
        const len = @as(usize, block_min) << @intCast(class);
        var it = head;
        while (it) |b| : (it = b.next) {
            const p: [*]const u8 = @ptrCast(b);
            for (p[@sizeOf(FreeBlock)..len]) |byte| try testing.expectEqual(free_poison, byte);
            free += 1;
        }
    }
    try testing.expect(free > 0);
}

/// Cancels a group of two hour-long sleepers with Group.cancel after a second.
fn cancelSleeperGroup(i: Io, inner: *[2]?anyerror) void {
    var g: Io.Group = .init;
    for (inner) |*r| g.async(i, sleepForever, .{ i, r });
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    g.cancel(i);
}

test "finishing a group with pending members leaves its freed state untouched" {
    {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var inner: [2]?anyerror = .{ error.Unset, error.Unset };
        var result: ?anyerror = error.Unset;
        const task = try e.wio.spawn("wf", awaitSleeperGroup, .{ e.wio.io(), &inner, &result });
        e.wio.run();
        e.wio.cancelTask(task);
        e.wio.run();
        try testing.expectEqual(@as(?anyerror, error.Canceled), result);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
        try expectFreeBlocksUntouched(&e.wio);
    }
    {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var inner: [2]?anyerror = .{ error.Unset, error.Unset };
        _ = try e.wio.spawn("wf", cancelSleeperGroup, .{ e.wio.io(), &inner });
        e.fake.runAll(&e.wio);
        try testing.expectEqual(@as(?anyerror, error.Canceled), inner[0]);
        try testing.expectEqual(@as(?anyerror, error.Canceled), inner[1]);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
        try expectFreeBlocksUntouched(&e.wio);
    }
    {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var trace: Trace = .{};
        _ = try e.wio.spawn("wf", sleepers, .{ e.wio.io(), &trace });
        e.fake.runAll(&e.wio);
        try testing.expectEqualSlices(u32, &.{ 1, 2, 3, 99 }, trace.items());
        try expectFreeBlocksUntouched(&e.wio);
    }
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

const SemaphoreState = struct {
    sem: Io.Semaphore = .{ .permits = 2 },
    active: u32 = 0,
    max_active: u32 = 0,
    acquired: Trace = .{},
};

fn semaphoreWorker(i: Io, s: *SemaphoreState, id: u32, hold_secs: i64) void {
    s.sem.wait(i) catch unreachable;
    defer s.sem.post(i);
    s.acquired.add(id);
    s.active += 1;
    s.max_active = @max(s.max_active, s.active);
    i.sleep(.fromSeconds(hold_secs), .awake) catch unreachable;
    s.active -= 1;
}

fn semaphoreWorkflow(i: Io, s: *SemaphoreState) void {
    var g: Io.Group = .init;
    // Holders release in the opposite order they acquired, so the waiters' order comes from
    // the semaphore, not from the timers.
    for (1..6) |id| g.async(i, semaphoreWorker, .{ i, s, @as(u32, @intCast(id)), @as(i64, @intCast(10 - id)) });
    g.await(i) catch unreachable;
}

test "std.Io.Semaphore limits holders across durable waits and wakes waiters in arrival order" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var s: SemaphoreState = .{};
    _ = try e.wio.spawn("wf", semaphoreWorkflow, .{ e.wio.io(), &s });
    e.wio.run();
    try testing.expectEqualSlices(u32, &.{ 1, 2 }, s.acquired.items());
    e.fake.runAll(&e.wio);
    try testing.expectEqual(@as(u32, 2), s.max_active);
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3, 4, 5 }, s.acquired.items());
    try testing.expectEqual(@as(usize, 2), s.sem.permits);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn eventWaiter(i: Io, ev: *Io.Event, trace: *Trace, id: u32) void {
    ev.wait(i) catch unreachable;
    trace.add(id);
}

fn eventSetter(i: Io, ev: *Io.Event, trace: *Trace) void {
    i.sleep(.fromSeconds(10), .awake) catch unreachable;
    trace.add(99);
    ev.set(i);
}

fn eventWorkflow(i: Io, trace: *Trace) void {
    var ev: Io.Event = .unset;
    var g: Io.Group = .init;
    for (1..4) |id| g.async(i, eventWaiter, .{ i, &ev, trace, @as(u32, @intCast(id)) });
    g.async(i, eventSetter, .{ i, &ev, trace });
    g.await(i) catch unreachable;
    // Once set, waiting returns at once.
    ev.wait(i) catch unreachable;
    trace.add(100);
}

test "std.Io.Event.set wakes every waiter after a durable timer" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var trace: Trace = .{};
    _ = try e.wio.spawn("wf", eventWorkflow, .{ e.wio.io(), &trace });
    e.wio.run();
    try testing.expectEqual(@as(usize, 0), trace.len);
    e.fake.runAll(&e.wio);
    try testing.expectEqualSlices(u32, &.{ 99, 1, 2, 3, 100 }, trace.items());
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

const ConditionState = struct {
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    tokens: u32 = 0,
    open: bool = false,
    trace: Trace = .{},
};

fn conditionWaiter(i: Io, s: *ConditionState, id: u32) void {
    s.mutex.lockUncancelable(i);
    defer s.mutex.unlock(i);
    while (s.tokens == 0 and !s.open) s.cond.wait(i, &s.mutex) catch unreachable;
    if (s.tokens > 0) s.tokens -= 1;
    s.trace.add(id);
}

fn conditionController(i: Io, s: *ConditionState) void {
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    s.mutex.lockUncancelable(i);
    s.tokens = 1;
    s.cond.signal(i);
    s.mutex.unlock(i);
    i.sleep(.fromSeconds(1), .awake) catch unreachable;
    s.mutex.lockUncancelable(i);
    s.open = true;
    s.cond.broadcast(i);
    s.mutex.unlock(i);
}

fn conditionWorkflow(i: Io, s: *ConditionState) void {
    var g: Io.Group = .init;
    for (1..4) |id| g.async(i, conditionWaiter, .{ i, s, @as(u32, @intCast(id)) });
    g.async(i, conditionController, .{ i, s });
    g.await(i) catch unreachable;
}

test "std.Io.Condition signal wakes one waiter and broadcast wakes the rest" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var s: ConditionState = .{};
    _ = try e.wio.spawn("wf", conditionWorkflow, .{ e.wio.io(), &s });
    e.wio.run();
    try testing.expectEqual(@as(usize, 0), s.trace.len);
    try testing.expect(e.fake.fireNext(&e.wio));
    e.wio.run();
    try testing.expectEqualSlices(u32, &.{1}, s.trace.items());
    try testing.expect(e.fake.fireNext(&e.wio));
    e.wio.run();
    try testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, s.trace.items());
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

const Blockers = struct {
    sem: Io.Semaphore = .{},
    event: Io.Event = .unset,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    results: [3]?anyerror = @splat(null),

    fn onSemaphore(i: Io, b: *Blockers) void {
        b.sem.wait(i) catch |err| {
            b.results[0] = err;
        };
    }

    fn onEvent(i: Io, b: *Blockers) void {
        b.event.wait(i) catch |err| {
            b.results[1] = err;
        };
    }

    fn onCondition(i: Io, b: *Blockers) void {
        b.mutex.lockUncancelable(i);
        defer b.mutex.unlock(i);
        b.cond.wait(i, &b.mutex) catch |err| {
            b.results[2] = err;
        };
    }

    fn run(i: Io, b: *Blockers) void {
        var sem = i.async(onSemaphore, .{ i, b });
        var event = i.async(onEvent, .{ i, b });
        var cond = i.async(onCondition, .{ i, b });
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        sem.cancel(i);
        event.cancel(i);
        cond.cancel(i);
    }
};

test "a coroutine canceled while blocked on a Semaphore, Event, or Condition gets error.Canceled" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var b: Blockers = .{};
    _ = try e.wio.spawn("wf", Blockers.run, .{ e.wio.io(), &b });
    e.wio.run();
    try testing.expectEqualSlices(?anyerror, &.{ null, null, null }, &b.results);
    e.fake.runAll(&e.wio);
    try testing.expectEqualSlices(?anyerror, &.{ error.Canceled, error.Canceled, error.Canceled }, &b.results);
    try testing.expectEqual(@as(?*Wait, null), e.wio.futex_head);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

/// A holder releases a primitive that two spawned waiters, B and C, block on, and B is canceled
/// before it runs again, either just before the release or just after it. The single wake the
/// release gives must not be lost with B.
const LostWake = struct {
    wio: *WorkflowIo,
    kind: Kind,
    cancel_first: bool,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    sem: Io.Semaphore = .{},
    signaled: bool = false,
    /// B's and C's outcomes: null when they got the primitive.
    results: [2]?anyerror = @splat(error.Unset),
    b: *Task = undefined,
    /// C's outcome when B, after its canceled wait, wakes from a long sleep: a wake B owed
    /// must reach C before B blocks, not when B ends.
    c_when_b_slept: ?anyerror = error.Unset,

    const Kind = enum { mutex, condition, semaphore };

    fn holder(s: *LostWake) void {
        const i = s.wio.io();
        if (s.kind == .mutex) s.mutex.lockUncancelable(i);
        // B and C block meanwhile.
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        if (s.cancel_first) s.wio.cancelTask(s.b);
        switch (s.kind) {
            .mutex => s.mutex.unlock(i),
            .condition => {
                s.mutex.lockUncancelable(i);
                s.signaled = true;
                s.cond.signal(i);
                s.mutex.unlock(i);
            },
            .semaphore => s.sem.post(i),
        }
        if (!s.cancel_first) s.wio.cancelTask(s.b);
    }

    fn waiter(s: *LostWake, index: usize) void {
        s.results[index] = s.acquire(s.wio.io());
        if (index == 0) {
            s.wio.io().sleep(.fromSeconds(100), .awake) catch unreachable;
            s.c_when_b_slept = s.results[1];
        }
    }

    fn acquire(s: *LostWake, i: Io) ?anyerror {
        switch (s.kind) {
            .mutex => {
                s.mutex.lock(i) catch |err| return err;
                s.mutex.unlock(i);
            },
            .condition => {
                s.mutex.lockUncancelable(i);
                defer s.mutex.unlock(i);
                while (!s.signaled) s.cond.wait(i, &s.mutex) catch |err| return err;
                s.signaled = false;
            },
            .semaphore => s.sem.wait(i) catch |err| return err,
        }
        return null;
    }

    fn expectWakeReachesC(kind: Kind, cancel_first: bool) !void {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var s: LostWake = .{ .wio = &e.wio, .kind = kind, .cancel_first = cancel_first };
        _ = try e.wio.spawn("holder", holder, .{&s});
        s.b = try e.wio.spawn("b", waiter, .{ &s, 0 });
        _ = try e.wio.spawn("c", waiter, .{ &s, 1 });
        e.fake.runAll(&e.wio);
        try testing.expectEqualSlices(?anyerror, &.{ error.Canceled, null }, &s.results);
        try testing.expectEqual(@as(?anyerror, null), s.c_when_b_slept);
        try testing.expectEqual(@as(?*Wait, null), e.wio.futex_head);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
        if (kind == .semaphore) try testing.expectEqual(@as(usize, 0), s.sem.permits);
    }
};

test "a std.Io.Mutex unlock that wakes a waiter canceled before it runs reaches the next waiter" {
    try LostWake.expectWakeReachesC(.mutex, false);
    try LostWake.expectWakeReachesC(.mutex, true);
}

test "a std.Io.Condition signal to a waiter canceled before it runs reaches the next waiter" {
    try LostWake.expectWakeReachesC(.condition, false);
    try LostWake.expectWakeReachesC(.condition, true);
}

test "a std.Io.Semaphore permit for a waiter canceled before it runs reaches the next waiter" {
    try LostWake.expectWakeReachesC(.semaphore, false);
    try LostWake.expectWakeReachesC(.semaphore, true);
}

/// std.Io.Condition already hands a canceled waiter's signal on (its `deregister` wakes one
/// waiter), so a canceled Condition waiter must not cost a third waiter a wake as well: a
/// spurious wake makes a waitTimeout restart its durable timer, which adds CancelTimer and
/// StartTimer commands that histories recorded before the fix don't have.
const CondTimedThird = struct {
    wio: *WorkflowIo,
    cancel_first: bool,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    signaled: bool = false,
    results: [3]?anyerror = @splat(error.Unset),
    b: *Task = undefined,

    fn holder(s: *CondTimedThird) void {
        const i = s.wio.io();
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        if (s.cancel_first) s.wio.cancelTask(s.b);
        s.mutex.lockUncancelable(i);
        s.signaled = true;
        s.cond.signal(i);
        s.mutex.unlock(i);
        if (!s.cancel_first) s.wio.cancelTask(s.b);
    }

    fn waiter(s: *CondTimedThird, index: usize) void {
        const i = s.wio.io();
        s.mutex.lockUncancelable(i);
        defer s.mutex.unlock(i);
        s.results[index] = while (!s.signaled) {
            if (index == 2) {
                s.cond.waitTimeout(i, &s.mutex, .{ .duration = .{ .raw = .fromSeconds(3600), .clock = .awake } }) catch |err| break err;
            } else s.cond.wait(i, &s.mutex) catch |err| break err;
        } else blk: {
            s.signaled = false;
            break :blk null;
        };
    }

    fn expectOneTimerForD(cancel_first: bool) !void {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var s: CondTimedThird = .{ .wio = &e.wio, .cancel_first = cancel_first };
        _ = try e.wio.spawn("holder", holder, .{&s});
        s.b = try e.wio.spawn("b", waiter, .{ &s, 0 });
        _ = try e.wio.spawn("c", waiter, .{ &s, 1 });
        _ = try e.wio.spawn("d", waiter, .{ &s, 2 });
        e.fake.runAll(&e.wio);
        try testing.expectEqualSlices(?anyerror, &.{ error.Canceled, null, error.Timeout }, &s.results);
        // The holder's sleep and D's one timer; D is never woken before its timer fires.
        try testing.expectEqual(@as(u32, 2), e.fake.started);
        try testing.expectEqual(@as(u32, 0), e.fake.canceled);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
};

test "a canceled std.Io.Condition waiter doesn't spuriously wake a timed waiter behind the one its signal goes to" {
    try CondTimedThird.expectOneTimerForD(false);
    try CondTimedThird.expectOneTimerForD(true);
}

/// B, woken by an unlock and canceled before it runs, falls back to `lockUncancelable` on the
/// same mutex, which the holder has locked again meanwhile. B's own wait is then the only one
/// on the address, so the wake B owes for its canceled wait must not land on that wait: B
/// would block with its wait already woken and never run again.
const RelockAfterCancel = struct {
    wio: *WorkflowIo,
    mutex: Io.Mutex = .init,
    b: *Task = undefined,
    /// B's canceled lock's outcome, and whether B then got the mutex.
    b_lock: ?anyerror = error.Unset,
    b_relocked: bool = false,

    fn holder(s: *RelockAfterCancel) void {
        const i = s.wio.io();
        s.mutex.lockUncancelable(i);
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        // Wakes B, then takes the mutex back before B runs.
        s.mutex.unlock(i);
        s.mutex.lockUncancelable(i);
        s.wio.cancelTask(s.b);
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        s.mutex.unlock(i);
    }

    fn waiter(s: *RelockAfterCancel) void {
        const i = s.wio.io();
        s.mutex.lock(i) catch |err| {
            s.b_lock = err;
            s.mutex.lockUncancelable(i);
            s.b_relocked = true;
            s.mutex.unlock(i);
            return;
        };
        s.b_lock = null;
        s.mutex.unlock(i);
    }
};

test "a waiter canceled after an unlock woke it can lock the same std.Io.Mutex uncancelably" {
    var e: Env = undefined;
    e.init();
    var s: RelockAfterCancel = .{ .wio = &e.wio };
    _ = try e.wio.spawn("holder", RelockAfterCancel.holder, .{&s});
    s.b = try e.wio.spawn("b", RelockAfterCancel.waiter, .{&s});
    e.fake.runAll(&e.wio);
    // A coroutine left blocked forever can't be torn down normally.
    defer if (e.sched.live == 0) e.deinit() else {
        e.fake.deinit();
        e.sched.deinitLeakingLive();
    };
    try testing.expectEqual(@as(?anyerror, error.Canceled), s.b_lock);
    try testing.expect(s.b_relocked);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

/// A consumer is canceled after a put handed it an element but before it ran, so its get
/// returns the element (std.Io.Queue reports what it received, and re-arms the cancel, which
/// the next get returns), and the consumer keeps draining the queue. Each get waits on a
/// Condition in its own frame, so a later get waits at the same address as the first: the
/// wake owed for the first wait mustn't land on the later one, or the consumer never runs
/// again and the next element is lost with it.
const QueueConsumer = struct {
    wio: *WorkflowIo,
    buf: [4]u32 = undefined,
    q: Io.Queue(u32) = undefined,
    consumer: *Task = undefined,
    got: [2]?u32 = .{ null, null },
    canceled: u32 = 0,

    fn producer(s: *QueueConsumer) void {
        const i = s.wio.io();
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        s.q.putOne(i, 7) catch unreachable;
        s.wio.cancelTask(s.consumer);
        i.sleep(.fromSeconds(1), .awake) catch unreachable;
        s.q.putOne(i, 8) catch unreachable;
    }

    fn consume(s: *QueueConsumer) void {
        const i = s.wio.io();
        for (&s.got) |*g| while (true) {
            g.* = s.q.getOne(i) catch |err| switch (err) {
                error.Canceled => {
                    s.canceled += 1;
                    continue;
                },
                error.Closed => return,
            };
            break;
        };
    }
};

test "a std.Io.Queue consumer canceled after a put woke it receives the next element too" {
    var e: Env = undefined;
    e.init();
    var s: QueueConsumer = .{ .wio = &e.wio };
    s.q = .init(&s.buf);
    _ = try e.wio.spawn("producer", QueueConsumer.producer, .{&s});
    s.consumer = try e.wio.spawn("consumer", QueueConsumer.consume, .{&s});
    e.fake.runAll(&e.wio);
    // A coroutine left blocked forever can't be torn down normally.
    defer if (e.sched.live == 0) e.deinit() else {
        e.fake.deinit();
        e.sched.deinitLeakingLive();
    };
    try testing.expectEqualSlices(?u32, &.{ 7, 8 }, &s.got);
    try testing.expectEqual(@as(u32, 1), s.canceled);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

const Received = union(enum) {
    received: (Io.QueueClosedError || Io.Cancelable)!u32,
    timeout: Io.Cancelable!void,
};

fn sendAfter(i: Io, q: *Io.Queue(u32), secs: i64, value: u32) void {
    i.sleep(.fromSeconds(secs), .awake) catch return;
    q.putOne(i, value) catch return;
}

fn receiveOrTimeout(i: Io, send_secs: i64, out: *[2]?u32) void {
    var buf: [1]u32 = undefined;
    var q: Io.Queue(u32) = .init(&buf);
    var drained: [1]u32 = undefined;
    // The non-blocking path: min = 0 returns at once with what is there.
    assert((q.get(i, &drained, 0) catch unreachable) == 0);

    var sender: Io.Group = .init;
    sender.async(i, sendAfter, .{ i, &q, send_secs, 7 });

    var results: [2]Received = undefined;
    var sel: Io.Select(Received) = .init(i, &results);
    sel.async(.received, Io.Queue(u32).getOne, .{ &q, i });
    sel.async(.timeout, Io.sleep, .{ i, .fromSeconds(10), .awake });
    switch (sel.await() catch unreachable) {
        .received => |r| out[0] = r catch unreachable,
        .timeout => |r| r catch unreachable,
    }
    sel.cancelDiscard();
    sender.await(i) catch unreachable;
    // A canceled receive takes nothing: a value sent afterwards stays in the queue.
    if ((q.get(i, &drained, 0) catch unreachable) == 1) out[1] = drained[0];
}

test "std.Io.Select races a std.Io.Queue receive against a timer" {
    // The value arrives first: the receive wins and the timer is canceled.
    {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var out: [2]?u32 = .{ null, null };
        _ = try e.wio.spawn("wf", receiveOrTimeout, .{ e.wio.io(), 5, &out });
        e.fake.runAll(&e.wio);
        try testing.expectEqualSlices(?u32, &.{ 7, null }, &out);
        try testing.expectEqual(@as(u32, 1), e.fake.canceled);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
    // The timer fires first: the receive is canceled, and the late value stays queued.
    {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var out: [2]?u32 = .{ null, null };
        _ = try e.wio.spawn("wf", receiveOrTimeout, .{ e.wio.io(), 20, &out });
        e.fake.runAll(&e.wio);
        try testing.expectEqualSlices(?u32, &.{ null, 7 }, &out);
        try testing.expectEqual(@as(u32, 0), e.fake.canceled);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
}

/// How a test makes the next coroutine start fail, from inside a coroutine.
const StartFault = union(enum) {
    /// The run's memory fails from this allocation on (0: the first one).
    task_block: usize,
    stack,

    /// Arms the fault; `disarm` undoes it. `failing` must outlive the armed span.
    fn arm(fault: StartFault, w: *WorkflowIo, failing: *std.testing.FailingAllocator) void {
        switch (fault) {
            .task_block => |index| {
                failing.* = .init(w.memory, .{ .fail_index = index });
                w.memory = failing.allocator();
            },
            .stack => w.scheduler.fail_stack_maps = 1,
        }
    }

    fn disarm(fault: StartFault, w: *WorkflowIo, failing: *std.testing.FailingAllocator) void {
        switch (fault) {
            .task_block => w.memory = failing.internal_allocator,
            .stack => w.scheduler.fail_stack_maps = 0,
        }
    }

    fn cause(fault: StartFault) StartFailure.Cause {
        return switch (fault) {
            .task_block => .task_block,
            .stack => .stack,
        };
    }
};

fn asyncUnderFault(i: Io, w: *WorkflowIo, fault: StartFault, out: *u64) void {
    var failing: std.testing.FailingAllocator = undefined;
    fault.arm(w, &failing);
    var f = i.async(square, .{5});
    fault.disarm(w, &failing);
    // A start that works again doesn't clear the failure.
    var g = i.async(square, .{2});
    out.* = f.await(i) + g.await(i);
}

test "io.async runs its function inline and records the failure when a coroutine can't start" {
    for ([_]StartFault{ .{ .task_block = 0 }, .stack }) |fault| {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var out: u64 = 0;
        _ = try e.wio.spawn("wf", asyncUnderFault, .{ e.wio.io(), &e.wio, fault, &out });
        e.wio.run();
        try testing.expectEqual(@as(u64, 29), out);
        try testing.expectEqualDeep(StartFailure{ .operation = "io.async", .cause = fault.cause() }, e.wio.start_failure.?);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
}

fn twoFailedStarts(i: Io, w: *WorkflowIo, out: *u64) void {
    var failing: std.testing.FailingAllocator = undefined;
    StartFault.arm(.stack, w, &failing);
    var f = i.async(square, .{3});
    StartFault.disarm(.stack, w, &failing);
    var g: Io.Group = .init;
    StartFault.arm(.{ .task_block = 0 }, w, &failing);
    g.async(i, addSquare, .{ out, 4 });
    StartFault.disarm(.{ .task_block = 0 }, w, &failing);
    g.await(i) catch unreachable;
    out.* += f.await(i);
}

fn addSquare(out: *u64, x: u64) void {
    out.* += x * x;
}

test "the first failed coroutine start is the one kept" {
    var e: Env = undefined;
    e.init();
    defer e.deinit();
    var out: u64 = 0;
    _ = try e.wio.spawn("wf", twoFailedStarts, .{ e.wio.io(), &e.wio, &out });
    e.wio.run();
    try testing.expectEqual(@as(u64, 25), out);
    try testing.expectEqualDeep(StartFailure{ .operation = "io.async", .cause = .stack }, e.wio.start_failure.?);
    try testing.expectEqual(@as(usize, 0), e.sched.live);
}

fn groupAsyncUnderFault(i: Io, w: *WorkflowIo, fault: StartFault, out: *u64) void {
    var g: Io.Group = .init;
    var failing: std.testing.FailingAllocator = undefined;
    fault.arm(w, &failing);
    g.async(i, addSquare, .{ out, 5 });
    fault.disarm(w, &failing);
    g.async(i, addSquare, .{ out, 2 });
    g.await(i) catch unreachable;
}

test "Group.async runs its function inline and records the failure when a coroutine can't start" {
    // Fail index 0 fails the group's state; 1 fails the task block after the state exists.
    for ([_]StartFault{ .{ .task_block = 0 }, .{ .task_block = 1 }, .stack }) |fault| {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var out: u64 = 0;
        _ = try e.wio.spawn("wf", groupAsyncUnderFault, .{ e.wio.io(), &e.wio, fault, &out });
        e.wio.run();
        try testing.expectEqual(@as(u64, 29), out);
        try testing.expectEqualDeep(StartFailure{ .operation = "Group.async", .cause = fault.cause() }, e.wio.start_failure.?);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
}

const Picked = union(enum) { a: u64, b: u64 };

fn selectUnderFault(i: Io, w: *WorkflowIo, fault: StartFault, out: *u64) void {
    var buf: [2]Picked = undefined;
    var sel: Io.Select(Picked) = .init(i, &buf);
    var failing: std.testing.FailingAllocator = undefined;
    fault.arm(w, &failing);
    sel.async(.a, square, .{5});
    fault.disarm(w, &failing);
    out.* = switch (sel.await() catch unreachable) {
        .a => |v| v,
        .b => |v| v + 1000,
    };
    sel.cancelDiscard();
}

test "a Select case runs inline and records the failure when its coroutine can't start" {
    for ([_]StartFault{ .{ .task_block = 0 }, .{ .task_block = 1 }, .stack }) |fault| {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var out: u64 = 0;
        _ = try e.wio.spawn("wf", selectUnderFault, .{ e.wio.io(), &e.wio, fault, &out });
        e.wio.run();
        try testing.expectEqual(@as(u64, 25), out);
        try testing.expectEqualDeep(StartFailure{ .operation = "Group.async", .cause = fault.cause() }, e.wio.start_failure.?);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
}

fn concurrentUnderFault(i: Io, w: *WorkflowIo, fault: StartFault, results: *[2]?anyerror) void {
    var failing: std.testing.FailingAllocator = undefined;
    fault.arm(w, &failing);
    if (i.concurrent(square, .{5})) |_| unreachable else |err| results[0] = err;
    fault.disarm(w, &failing);
    fault.arm(w, &failing);
    var g: Io.Group = .init;
    if (g.concurrent(i, addSquare, .{ undefined, 5 })) |_| unreachable else |err| results[1] = err;
    fault.disarm(w, &failing);
    g.await(i) catch unreachable;
}

test "io.concurrent and Group.concurrent still return ConcurrencyUnavailable and record nothing" {
    for ([_]StartFault{ .{ .task_block = 0 }, .stack }) |fault| {
        var e: Env = undefined;
        e.init();
        defer e.deinit();
        var results: [2]?anyerror = .{ null, null };
        _ = try e.wio.spawn("wf", concurrentUnderFault, .{ e.wio.io(), &e.wio, fault, &results });
        e.wio.run();
        try testing.expectEqual(@as(?anyerror, error.ConcurrencyUnavailable), results[0]);
        try testing.expectEqual(@as(?anyerror, error.ConcurrencyUnavailable), results[1]);
        try testing.expectEqual(@as(?StartFailure, null), e.wio.start_failure);
        try testing.expectEqual(@as(usize, 0), e.sched.live);
    }
}

test "StartFailure formats the operation and the cause" {
    var buf: [512]u8 = undefined;
    try testing.expectEqualStrings(
        "io.async could not start a workflow coroutine: out of memory allocating its task block",
        try std.fmt.bufPrint(&buf, "{f}", .{StartFailure{ .operation = "io.async", .cause = .task_block }}),
    );
    try testing.expectEqualStrings(
        "Group.async could not start a workflow coroutine: the operating system refused to map its stack (memory or the process's limit on memory mappings is exhausted; lower worker.Options.workflow_stack_size or max_cached_workflows)",
        try std.fmt.bufPrint(&buf, "{f}", .{StartFailure{ .operation = "Group.async", .cause = .stack }}),
    );
}
