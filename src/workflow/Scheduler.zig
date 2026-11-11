//! Deterministic, single-threaded, cooperative scheduler for workflow coroutines.
//!
//! Each coroutine runs on its own stack (a fiber). The thread
//! that calls `run` acts as the scheduler: it resumes ready fibers one at a time in FIFO order
//! until none are ready, then returns. A fiber runs until it finishes, calls `block` (waits
//! for `wake`), or calls `yield` (goes to the back of the ready queue). Nothing here touches
//! threads, clocks, or randomness, so the same sequence of spawns and wakes always produces
//! the same interleaving. That is the property workflow replay depends on.
//!
//! The deterministic `std.Io` implementation for workflow code is built on top of this.

const Scheduler = @This();

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

comptime {
    const ok = switch (builtin.cpu.arch) {
        .aarch64 => true,
        // The System V ABI only; Windows x64 has a different callee-saved register set.
        .x86_64 => builtin.os.tag != .windows,
        else => false,
    };
    if (!ok) @compileError("workflow fibers are not implemented for " ++ @tagName(builtin.cpu.arch) ++ "-" ++ @tagName(builtin.os.tag));
}

gpa: Allocator,
options: Options,
/// Saved stack pointer of the thread inside `run` while a fiber is executing.
main_sp: usize = undefined,
current: ?*Fiber = null,
ready_head: ?*Fiber = null,
ready_tail: ?*Fiber = null,
/// Stacks of finished fibers, reused by later spawns.
free_stacks: std.ArrayList(Stack) = .empty,
/// Fibers spawned and not yet finished.
live: usize = 0,
next_id: u32 = 1,

pub const Options = struct {
    /// Usable stack bytes per fiber. Memory is reserved virtually and only touched pages
    /// become resident, so a generous size costs little.
    stack_size: usize = 1024 * 1024,
    /// Finished fiber stacks kept for reuse.
    max_pooled_stacks: usize = 16,
};

pub const Fiber = struct {
    /// Saved stack pointer while the fiber is not running. Its callee-saved registers are
    /// stored on its own stack, just above this address.
    sp: usize,
    scheduler: *Scheduler,
    stack: Stack,
    start: *const fn (args: *anyopaque) void,
    args: *anyopaque,
    id: u32,
    /// For stack traces and debugging. Not owned.
    name: []const u8,
    state: State,
    queue_next: ?*Fiber = null,

    pub const State = enum { ready, running, blocked, done };
};

pub fn init(gpa: Allocator, options: Options) Scheduler {
    return .{ .gpa = gpa, .options = options };
}

/// All spawned fibers must have finished. Tearing down blocked fibers is the caller's job
/// (cancel them and `run` until they exit).
pub fn deinit(s: *Scheduler) void {
    assert(s.live == 0);
    assert(s.current == null);
    for (s.free_stacks.items) |stack| stack.unmap();
    s.free_stacks.deinit(s.gpa);
    s.* = undefined;
}

/// Creates a fiber that will call `func` with `args` and puts it at the back of the ready
/// queue. `func` must return `void`. Can be called from `run`'s caller or from a fiber.
pub fn spawn(s: *Scheduler, name: []const u8, comptime func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !*Fiber {
    const Args = @TypeOf(args);
    if (@typeInfo(@TypeOf(func)).@"fn".return_type.? != void)
        @compileError("fiber functions must return void");

    const Trampoline = struct {
        fn start(p: *anyopaque) void {
            const a: *Args = @ptrCast(@alignCast(p));
            @call(.auto, func, a.*);
        }
    };

    const stack = try s.takeStack();
    errdefer s.releaseStack(stack);

    // Lay out the Fiber header and the arguments at the top of the stack; the fiber's own
    // frames grow downward from just below them.
    const top = @intFromPtr(stack.usable().ptr) + stack.usable().len;
    const args_addr = std.mem.alignBackward(usize, top - @sizeOf(Args), @max(@alignOf(Args), 16));
    const fiber_addr = std.mem.alignBackward(usize, args_addr - @sizeOf(Fiber), 16);
    const a: *Args = @ptrFromInt(args_addr);
    a.* = args;
    const f: *Fiber = @ptrFromInt(fiber_addr);
    f.* = .{
        .sp = initialStack(f),
        .scheduler = s,
        .stack = stack,
        .start = Trampoline.start,
        .args = a,
        .id = s.next_id,
        .name = name,
        .state = .ready,
    };
    s.next_id += 1;
    s.live += 1;
    s.push(f);
    return f;
}

/// Resumes ready fibers in FIFO order until none are ready. Returns with every live fiber
/// blocked. Must not be called from a fiber.
pub fn run(s: *Scheduler) void {
    assert(s.current == null);
    while (s.pop()) |f| {
        assert(f.state == .ready);
        f.state = .running;
        s.current = f;
        switchStack(&s.main_sp, f.sp);
        // Back on the scheduler: the fiber blocked, yielded, or finished.
        const prev = s.current.?;
        s.current = null;
        if (prev.state == .done) s.releaseStack(prev.stack);
    }
}

/// The fiber currently executing, or null when called outside a fiber.
pub fn currentFiber(s: *const Scheduler) ?*Fiber {
    return s.current;
}

/// Suspends the calling fiber until another party calls `wake` on it.
pub fn block(s: *Scheduler) void {
    const f = s.current orelse unreachable; // block called outside a fiber
    f.state = .blocked;
    s.switchToMain(f);
}

/// Moves the calling fiber to the back of the ready queue and lets the others run.
pub fn yield(s: *Scheduler) void {
    const f = s.current orelse unreachable; // yield called outside a fiber
    f.state = .ready;
    s.push(f);
    s.switchToMain(f);
}

/// Makes a blocked fiber ready. It runs during the current or next call to `run`.
pub fn wake(s: *Scheduler, f: *Fiber) void {
    assert(f.state == .blocked);
    f.state = .ready;
    s.push(f);
}

fn switchToMain(s: *Scheduler, f: *Fiber) void {
    switchStack(&f.sp, s.main_sp);
    // Resumed by `run`.
    assert(s.current == f);
}

fn push(s: *Scheduler, f: *Fiber) void {
    assert(f.queue_next == null);
    if (s.ready_tail) |tail| tail.queue_next = f else s.ready_head = f;
    s.ready_tail = f;
}

fn pop(s: *Scheduler) ?*Fiber {
    const f = s.ready_head orelse return null;
    s.ready_head = f.queue_next;
    if (s.ready_head == null) s.ready_tail = null;
    f.queue_next = null;
    return f;
}

/// First code a new fiber runs, called by `fiberEntry` with the Fiber that `initialStack`
/// placed in a callee-saved register.
fn fiberMain(f: *Fiber) callconv(.c) noreturn {
    const s = f.scheduler;
    assert(s.current == f);
    f.start(f.args);
    f.state = .done;
    s.live -= 1;
    // `run` releases the stack once it is no longer executing on it.
    var dead_sp: usize = undefined;
    switchStack(&dead_sp, s.main_sp);
    unreachable; // a finished fiber is never resumed
}

/// Saves the callee-saved registers on the current stack, stores the stack pointer in
/// `save_sp`, switches to `load_sp`, restores the registers saved there, and returns into
/// the code that last switched away from that stack.
///
/// This is an ordinary function call as far as the compiler is concerned: every register
/// the ABI lets a callee clobber is assumed clobbered, and the rest are saved here. That is
/// what makes it safe under optimization, unlike a switch written as inline assembly.
fn switchStack(save_sp: *usize, load_sp: usize) void {
    // Calling the naked routine through a C function pointer makes the compiler treat it
    // as an opaque call: caller-saved registers are assumed clobbered, and the routine
    // itself preserves the callee-saved ones.
    const impl: *const fn (*usize, usize) callconv(.c) void = switch (builtin.cpu.arch) {
        .aarch64 => @ptrCast(&switchStackAarch64),
        .x86_64 => @ptrCast(&switchStackX86_64),
        else => unreachable,
    };
    impl(save_sp, load_sp);
}

/// Callee-saved on AAPCS64: x19-x28, fp (x29), lr (x30), and the low halves of v8-v15.
/// Frame layout (160 bytes, low to high): x19..x28, fp, lr, d8..d15.
const aarch64_frame_size = 160;

fn switchStackAarch64() callconv(.naked) void {
    asm volatile (
        \\ sub sp, sp, #160
        \\ stp x19, x20, [sp, #0]
        \\ stp x21, x22, [sp, #16]
        \\ stp x23, x24, [sp, #32]
        \\ stp x25, x26, [sp, #48]
        \\ stp x27, x28, [sp, #64]
        \\ stp x29, x30, [sp, #80]
        \\ stp d8, d9, [sp, #96]
        \\ stp d10, d11, [sp, #112]
        \\ stp d12, d13, [sp, #128]
        \\ stp d14, d15, [sp, #144]
        \\ mov x2, sp
        \\ str x2, [x0]
        \\ mov sp, x1
        \\ ldp x19, x20, [sp, #0]
        \\ ldp x21, x22, [sp, #16]
        \\ ldp x23, x24, [sp, #32]
        \\ ldp x25, x26, [sp, #48]
        \\ ldp x27, x28, [sp, #64]
        \\ ldp x29, x30, [sp, #80]
        \\ ldp d8, d9, [sp, #96]
        \\ ldp d10, d11, [sp, #112]
        \\ ldp d12, d13, [sp, #128]
        \\ ldp d14, d15, [sp, #144]
        \\ add sp, sp, #160
        \\ ret
    );
}

/// Callee-saved on System V x86_64: rbx, rbp, r12-r15. The return address is on the stack.
/// Frame layout (low to high): r15, r14, r13, r12, rbx, rbp, return address.
const x86_64_frame_words = 7;

fn switchStackX86_64() callconv(.naked) void {
    asm volatile (
        \\ pushq %%rbp
        \\ pushq %%rbx
        \\ pushq %%r12
        \\ pushq %%r13
        \\ pushq %%r14
        \\ pushq %%r15
        \\ movq %%rsp, (%%rdi)
        \\ movq %%rsi, %%rsp
        \\ popq %%r15
        \\ popq %%r14
        \\ popq %%r13
        \\ popq %%r12
        \\ popq %%rbx
        \\ popq %%rbp
        \\ retq
    );
}

/// Where a new fiber starts: `switchStack` "returns" here with the Fiber pointer in a
/// callee-saved register (x19 / rbx) and the stack pointer just below the Fiber header.
fn fiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
            \\ mov x0, x19
            \\ mov x29, #0
            \\ bl %[fiberMain]
            \\ brk #1
            :
            : [fiberMain] "X" (&fiberMain),
        ),
        .x86_64 => asm volatile (
            \\ movq %%rbx, %%rdi
            \\ xorl %%ebp, %%ebp
            \\ callq %[fiberMain:P]
            \\ ud2
            :
            : [fiberMain] "X" (&fiberMain),
        ),
        else => unreachable,
    }
}

/// Builds the frame `switchStack` restores the first time it switches to `f`: a saved
/// register set whose return address is `fiberEntry` and whose first callee-saved register
/// holds `f`. Returns the stack pointer to save in `f.sp`.
fn initialStack(f: *Fiber) usize {
    const top = @intFromPtr(f); // 16-byte aligned, just below the Fiber header
    switch (builtin.cpu.arch) {
        .aarch64 => {
            const sp = top - aarch64_frame_size;
            const regs: [*]usize = @ptrFromInt(sp);
            @memset(regs[0 .. aarch64_frame_size / @sizeOf(usize)], 0);
            regs[0] = @intFromPtr(f); // x19
            regs[11] = @intFromPtr(&fiberEntry); // x30 (lr)
            return sp;
        },
        .x86_64 => {
            // `switchStack` pops six registers and the return address (7 words), leaving
            // rsp = sp + 56. The `call` in fiberEntry needs rsp 16-byte aligned, so sp must
            // be 8 mod 16. top is 16-aligned: sp = top - 72 gives rsp = top - 16.
            const sp = top - 72;
            const regs: [*]usize = @ptrFromInt(sp);
            @memset(regs[0..x86_64_frame_words], 0);
            regs[4] = @intFromPtr(f); // rbx
            regs[6] = @intFromPtr(&fiberEntry); // return address
            return sp;
        },
        else => unreachable,
    }
}

fn takeStack(s: *Scheduler) !Stack {
    if (s.free_stacks.pop()) |stack| return stack;
    return Stack.map(s.options.stack_size);
}

fn releaseStack(s: *Scheduler, stack: Stack) void {
    if (s.free_stacks.items.len < s.options.max_pooled_stacks) {
        if (s.free_stacks.append(s.gpa, stack)) |_| return else |_| {}
    }
    stack.unmap();
}

/// A fiber stack: an anonymous mapping whose lowest page is inaccessible, so overflowing
/// the stack faults immediately instead of corrupting the neighboring memory.
const Stack = struct {
    memory: []align(std.heap.page_size_min) u8,

    fn map(usable_size: usize) !Stack {
        const page = std.heap.pageSize();
        const len = std.mem.alignForward(usize, usable_size, page) + page;
        const memory = try std.posix.mmap(
            null,
            len,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        );
        errdefer std.posix.munmap(memory);
        if (std.c.mprotect(memory.ptr, page, .{}) != 0) return error.GuardPageFailed;
        return .{ .memory = memory };
    }

    fn unmap(stack: Stack) void {
        std.posix.munmap(stack.memory);
    }

    fn usable(stack: Stack) []u8 {
        return stack.memory[std.heap.pageSize()..];
    }
};

// ---------------------------------------------------------------- tests

const testing = std.testing;

const Log = struct {
    entries: [256]u32 = undefined,
    len: usize = 0,

    fn add(l: *Log, v: u32) void {
        l.entries[l.len] = v;
        l.len += 1;
    }

    fn items(l: *const Log) []const u32 {
        return l.entries[0..l.len];
    }
};

fn yielder(s: *Scheduler, log: *Log, id: u32, times: u32) void {
    for (0..times) |_| {
        log.add(id);
        s.yield();
    }
    log.add(id + 100);
}

test "fibers interleave in FIFO order, identically on every run" {
    var first: Log = .{};
    for (0..2) |round| {
        var s: Scheduler = .init(testing.allocator, .{});
        defer s.deinit();
        var log: Log = .{};
        for (1..4) |id| _ = try s.spawn("y", yielder, .{ &s, &log, @intCast(id), 2 });
        s.run();
        try testing.expectEqual(@as(usize, 0), s.live);
        try testing.expectEqualSlices(u32, &.{ 1, 2, 3, 1, 2, 3, 101, 102, 103 }, log.items());
        if (round == 0) first = log else try testing.expectEqualSlices(u32, first.items(), log.items());
    }
}

const Gate = struct {
    waiter: ?*Fiber = null,
    log: *Log,
};

fn waitOnGate(s: *Scheduler, gate: *Gate) void {
    gate.log.add(1);
    gate.waiter = s.currentFiber();
    s.block();
    gate.log.add(2);
}

test "a blocked fiber resumes after wake on the next run" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var log: Log = .{};
    var gate: Gate = .{ .log = &log };
    _ = try s.spawn("waiter", waitOnGate, .{ &s, &gate });
    s.run();
    try testing.expectEqualSlices(u32, &.{1}, log.items());
    try testing.expectEqual(Fiber.State.blocked, gate.waiter.?.state);
    try testing.expectEqual(@as(usize, 1), s.live);

    s.wake(gate.waiter.?);
    s.run();
    try testing.expectEqualSlices(u32, &.{ 1, 2 }, log.items());
    try testing.expectEqual(@as(usize, 0), s.live);
}

fn child(log: *Log, id: u32) void {
    log.add(id);
}

fn parent(s: *Scheduler, log: *Log) void {
    log.add(1);
    _ = s.spawn("child-a", child, .{ log, 2 }) catch unreachable;
    _ = s.spawn("child-b", child, .{ log, 3 }) catch unreachable;
    log.add(4);
}

test "fibers spawned from a fiber run after it, in spawn order" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var log: Log = .{};
    _ = try s.spawn("parent", parent, .{ &s, &log });
    s.run();
    try testing.expectEqualSlices(u32, &.{ 1, 4, 2, 3 }, log.items());
}

fn recordStack(out: *usize) void {
    var local: u8 = 0;
    std.mem.doNotOptimizeAway(&local);
    out.* = @intFromPtr(&local);
}

test "finished fiber stacks are reused" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 64 * 1024 });
    defer s.deinit();
    var first: usize = 0;
    var second: usize = 0;
    _ = try s.spawn("a", recordStack, .{&first});
    s.run();
    try testing.expectEqual(@as(usize, 1), s.free_stacks.items.len);
    _ = try s.spawn("b", recordStack, .{&second});
    try testing.expectEqual(@as(usize, 0), s.free_stacks.items.len);
    s.run();
    try testing.expectEqual(first, second);
}

fn deep(depth: u32) u64 {
    var buf: [1024]u8 = undefined;
    @memset(&buf, @truncate(depth));
    std.mem.doNotOptimizeAway(&buf);
    if (depth == 0) return buf[0];
    return deep(depth - 1) + buf[512];
}

fn useStack(out: *u64) void {
    out.* = deep(200); // about 200 KiB of frames
}

test "a fiber can use most of its stack" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 512 * 1024 });
    defer s.deinit();
    var result: u64 = 0;
    _ = try s.spawn("deep", useStack, .{&result});
    s.run();
    try testing.expect(result != 0);
}

fn pingPong(s: *Scheduler, counter: *u64, n: u64) void {
    for (0..n) |_| {
        counter.* += 1;
        s.yield();
    }
}

test "context switch cost" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var counter: u64 = 0;
    const n = 200_000;
    _ = try s.spawn("a", pingPong, .{ &s, &counter, n });
    _ = try s.spawn("b", pingPong, .{ &s, &counter, n });
    const start = std.Io.Timestamp.now(testing.io, .awake);
    s.run();
    const elapsed = start.durationTo(std.Io.Timestamp.now(testing.io, .awake));
    try testing.expectEqual(@as(u64, 2 * n), counter);
    // Each yield is two switches: fiber -> scheduler -> next fiber.
    const switches: i96 = 2 * 2 * n;
    std.debug.print("\nfiber context switch: {d} ns\n", .{@divTrunc(elapsed.nanoseconds, switches)});
}
