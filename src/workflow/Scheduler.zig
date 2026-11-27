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
/// Thread Sanitizer's context of the thread inside `run`, set at every `run` call because
/// successive calls can come from different threads. `void` without the sanitizer.
tsan_thread: tsan.Handle = undefined,
current: ?*Fiber = null,
ready_head: ?*Fiber = null,
ready_tail: ?*Fiber = null,
/// Stacks of finished fibers, reused by later spawns.
free_stacks: std.ArrayList(Stack) = .empty,
/// Fibers spawned and not yet finished.
live: usize = 0,
/// The unfinished fibers in spawn order, linked through `Fiber.live_next`.
live_head: ?*Fiber = null,
live_tail: ?*Fiber = null,
next_id: u32 = 1,
/// How many times a fiber has returned control to `run`. With `sdk_work`, one of the only two
/// fields another thread may read (atomically) while the scheduler's thread works: the
/// worker's deadlock detector watches `switches` advance, and doesn't count time while
/// `sdk_work` is set. Workflow code never sees either, so they don't affect determinism.
switches: std.atomic.Value(u64) = .init(0),
/// Set by the scheduler's thread, outside any fiber, while it does SDK work that can take
/// longer than the deadlock limit and isn't workflow code: reading debug info to answer the
/// stack trace query or to write a failure's stack trace. Never set while a fiber runs. Only the scheduler's thread writes it.
sdk_work: std.atomic.Value(bool) = .init(false),
/// Tests only: while non-zero, takeStack fails as Stack.map would and counts it down, before
/// looking at the pool.
fail_stack_maps: if (builtin.is_test) u32 else void = if (builtin.is_test) 0 else {},

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
    /// Links in the scheduler's list of unfinished fibers (`firstLive`).
    live_prev: ?*Fiber = null,
    live_next: ?*Fiber = null,
    /// Owned by the layer that spawned the fiber (the workflow `std.Io` keeps its task here).
    user: ?*anyopaque = null,
    /// Thread Sanitizer's fiber for this stack; `void` without the sanitizer.
    tsan_fiber: tsan.Handle,

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

/// Unmaps the stacks of every unfinished fiber, as if each had finished, and returns how many
/// there were. Only for fibers that can never be resumed or woken again: none may be ready,
/// and the caller must free, without following them, every pointer into those stacks (the
/// Fiber headers and anything the fibers' frames held), since no defer of theirs runs.
pub fn abandonLive(s: *Scheduler) usize {
    assert(s.current == null);
    assert(s.ready_head == null);
    const count = s.live;
    var it = s.live_head;
    while (it) |f| {
        // The header lives on the stack it describes: read it before unmapping.
        const next = f.live_next;
        const stack = f.stack;
        assert(f.state == .blocked);
        tsan.destroy(f.tsan_fiber);
        stack.unmap();
        it = next;
    }
    s.live = 0;
    s.live_head = null;
    s.live_tail = null;
    return count;
}

/// Like `deinit`, for fibers that refuse to exit: frees the pooled stacks and the pool's
/// list, and leaves the stacks of unfinished fibers mapped. Those fibers (their headers live
/// on their stacks) must never be resumed or woken afterwards.
pub fn deinitLeakingLive(s: *Scheduler) void {
    assert(s.current == null);
    if (builtin.sanitize_thread) {
        var it = s.live_head;
        while (it) |f| : (it = f.live_next) tsan.destroy(f.tsan_fiber);
    }
    for (s.free_stacks.items) |stack| stack.unmap();
    s.free_stacks.deinit(s.gpa);
    s.* = undefined;
}

/// Stack bytes the scheduler holds, counted at the configured size: one stack per
/// unfinished fiber plus the pooled ones. Depends only on the spawns and exits, not on
/// how many pages were touched.
pub fn stackBytes(s: *const Scheduler) usize {
    return s.stackCount() * s.options.stack_size;
}

/// Stacks the scheduler holds: unfinished fibers plus pooled stacks.
pub fn stackCount(s: *const Scheduler) usize {
    return s.live + s.free_stacks.items.len;
}

/// Creates a fiber that will call `func` with `args` and puts it at the back of the ready
/// queue. `func` must return `void`. Can be called from `run`'s caller or from a fiber.
/// Returns `error.StackTooSmall`, before it takes a stack, when `options.stack_size` can't
/// hold the arguments, the Fiber header and the first frame it writes at the stack's top.
pub fn spawn(s: *Scheduler, name: []const u8, comptime func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) !*Fiber {
    const Args = @TypeOf(args);
    if (@typeInfo(@TypeOf(func)).@"fn".return_type.? != void)
        @compileError("fiber functions must return void");
    if (s.options.stack_size < spawnBytes(Args)) return error.StackTooSmall;

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
        .tsan_fiber = tsan.create(),
    };
    s.next_id += 1;
    s.live += 1;
    f.live_prev = s.live_tail;
    if (s.live_tail) |t| t.live_next = f else s.live_head = f;
    s.live_tail = f;
    s.push(f);
    return f;
}

/// Resumes ready fibers in FIFO order until none are ready. Returns with every live fiber
/// blocked. Must not be called from a fiber.
pub fn run(s: *Scheduler) void {
    assert(s.current == null);
    s.tsan_thread = tsan.current();
    while (s.pop()) |f| {
        assert(f.state == .ready);
        f.state = .running;
        s.current = f;
        tsan.switchTo(f.tsan_fiber);
        switchStack(&s.main_sp, f.sp);
        // Back on the scheduler: the fiber blocked, yielded, or finished.
        _ = s.switches.fetchAdd(1, .release);
        const prev = s.current.?;
        s.current = null;
        if (prev.state == .done) {
            // The handle lives on the stack, so it goes before the stack can be unmapped.
            tsan.destroy(prev.tsan_fiber);
            s.releaseStack(prev.stack);
        }
    }
}

/// The fiber currently executing, or null when called outside a fiber.
pub fn currentFiber(s: *const Scheduler) ?*Fiber {
    return s.current;
}

/// Whether `addr` lies in the stack of any unfinished fiber (running, ready or blocked).
/// Callable inside or outside a fiber.
pub fn onLiveStack(s: *const Scheduler, addr: usize) bool {
    var it = s.live_head;
    while (it) |f| : (it = f.live_next) {
        const start = @intFromPtr(f.stack.memory.ptr);
        if (addr >= start and addr < start + f.stack.memory.len) return true;
    }
    return false;
}

/// The oldest unfinished fiber; `Fiber.live_next` gives the rest in spawn order.
pub fn firstLive(s: *const Scheduler) ?*Fiber {
    return s.live_head;
}

/// The return addresses of a blocked fiber's stack, innermost first, stored in `buf`: read
/// from the registers the fiber saved when it switched away, by `std.debug`'s unwinder, so
/// a fiber pays nothing for this until it is asked. Empty for a fiber that isn't blocked,
/// and where the target can't unwind. The first address is in this file (where the fiber
/// switched away). Takes the stack and debug info reads of a panic's trace: call it from a
/// thread's own stack, not a fiber's.
pub fn blockedTrace(f: *const Fiber, buf: []usize) []usize {
    if (f.state != .blocked) return buf[0..0];
    if (comptime !std.options.allow_stack_tracing or std.debug.cpu_context.Native == noreturn) return buf[0..0];
    var ctx = suspendedContext(f);
    return std.debug.captureCurrentStackTrace(.{ .context = &ctx }, buf).return_addresses;
}

/// The registers of a fiber that switched away, as they are just after its `switchStack`
/// returns: the callee-saved ones from its frame, the stack pointer above that frame, and
/// the program counter at the frame's return address. The rest are zero; nothing needs
/// them, as the ABI lets a callee clobber them.
fn suspendedContext(f: *const Fiber) std.debug.cpu_context.Native {
    const Native = std.debug.cpu_context.Native;
    var ctx: Native = std.mem.zeroes(Native);
    switch (builtin.cpu.arch) {
        .aarch64 => {
            const regs: [*]const usize = @ptrFromInt(f.sp);
            for (0..10) |i| ctx.x[19 + i] = regs[i];
            ctx.x[29] = regs[10];
            ctx.x[30] = regs[11];
            ctx.sp = f.sp + aarch64_frame_size;
            ctx.pc = regs[11];
        },
        .x86_64 => {
            const regs: [*]const usize = @ptrFromInt(f.sp);
            ctx.gprs.set(.r15, regs[0]);
            ctx.gprs.set(.r14, regs[1]);
            ctx.gprs.set(.r13, regs[2]);
            ctx.gprs.set(.r12, regs[3]);
            ctx.gprs.set(.rbx, regs[4]);
            ctx.gprs.set(.rbp, regs[5]);
            ctx.gprs.set(.rip, regs[6]);
            ctx.gprs.set(.rsp, f.sp + x86_64_frame_words * @sizeOf(usize));
        },
        else => comptime unreachable,
    }
    return ctx;
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

/// `wake`, but the fiber goes to the front of the ready queue: it runs next.
pub fn wakeFirst(s: *Scheduler, f: *Fiber) void {
    assert(f.state == .blocked);
    assert(f.queue_next == null);
    f.state = .ready;
    f.queue_next = s.ready_head;
    s.ready_head = f;
    if (s.ready_tail == null) s.ready_tail = f;
}

fn switchToMain(s: *Scheduler, f: *Fiber) void {
    tsan.switchTo(s.tsan_thread);
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
    if (f.live_prev) |p| p.live_next = f.live_next else s.live_head = f.live_next;
    if (f.live_next) |n| n.live_prev = f.live_prev else s.live_tail = f.live_prev;
    // `run` releases the stack once it is no longer executing on it.
    var dead_sp: usize = undefined;
    tsan.switchTo(s.tsan_thread);
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
/// It enters `fiberMain` with a zero frame pointer and a zero return address, the usual
/// end-of-stack marker, so stack unwinders (panics, debug allocators) stop at the fiber's
/// base instead of walking into this function, which has no unwind information.
fn fiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .aarch64 => asm volatile (
            \\ mov x0, x19
            \\ mov x29, #0
            \\ mov x30, #0
            \\ b %[fiberMain]
            :
            : [fiberMain] "X" (&fiberMain),
        ),
        .x86_64 => asm volatile (
            \\ movq %%rbx, %%rdi
            \\ xorl %%ebp, %%ebp
            \\ pushq $0
            \\ jmp %[fiberMain:P]
            :
            : [fiberMain] "X" (&fiberMain),
        ),
        else => unreachable,
    }
}

/// Thread Sanitizer's fiber interface. `switchStack` changes stacks behind the sanitizer's
/// back, so the scheduler tells it which fiber runs. Without the sanitizer `Handle` is
/// `void` and every function is empty.
const tsan = struct {
    const Handle = if (builtin.sanitize_thread) *anyopaque else void;
    extern fn __tsan_get_current_fiber() *anyopaque;
    extern fn __tsan_create_fiber(flags: c_uint) *anyopaque;
    extern fn __tsan_destroy_fiber(fiber: *anyopaque) void;
    extern fn __tsan_switch_to_fiber(fiber: *anyopaque, flags: c_uint) void;

    /// The calling thread's own context.
    inline fn current() Handle {
        return if (builtin.sanitize_thread) __tsan_get_current_fiber() else {};
    }

    inline fn create() Handle {
        return if (builtin.sanitize_thread) __tsan_create_fiber(0) else {};
    }

    inline fn destroy(h: Handle) void {
        if (builtin.sanitize_thread) __tsan_destroy_fiber(h);
    }

    /// Flags 0 make the switch synchronizing: it releases on the old context and acquires
    /// on the new one, so a fiber resumed on another thread sees what it wrote before.
    inline fn switchTo(h: Handle) void {
        if (builtin.sanitize_thread) __tsan_switch_to_fiber(h, 0);
    }
};

/// The most bytes `spawn` writes at the top of a stack for arguments of type `Args`: the
/// arguments and their alignment, the Fiber header and its alignment, and the first frame
/// `initialStack` builds below the header.
fn spawnBytes(comptime Args: type) usize {
    const first_frame = switch (builtin.cpu.arch) {
        .aarch64 => aarch64_frame_size,
        .x86_64 => 72,
        else => unreachable,
    };
    return @sizeOf(Args) + (@max(@alignOf(Args), 16) - 1) + @sizeOf(Fiber) + 15 + first_frame;
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
            // rsp = sp + 56. fiberEntry then pushes a zero return address, and fiberMain
            // must see rsp = 8 mod 16 on entry, as after a `call`. So rsp must be 16-byte
            // aligned before that push: with top 16-aligned, sp = top - 72 gives top - 16.
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
    if (builtin.is_test and s.fail_stack_maps > 0) {
        s.fail_stack_maps -= 1;
        return error.OutOfMemory;
    }
    if (s.free_stacks.pop()) |stack| return stack;
    return Stack.map(s.options.stack_size);
}

fn releaseStack(s: *Scheduler, stack: Stack) void {
    if (s.free_stacks.items.len < s.options.max_pooled_stacks) {
        if (s.free_stacks.append(s.gpa, stack)) |_| return else |_| {}
    }
    stack.unmap();
}

/// The length of the mapping for a stack of `usable_size` bytes: rounded up to whole pages,
/// plus the guard page. Null when that overflows `usize`.
pub fn stackMappingLen(usable_size: usize) ?usize {
    const page = std.heap.pageSize();
    const rounded = std.math.add(usize, usable_size, page - 1) catch return null;
    return std.math.add(usize, std.mem.alignBackward(usize, rounded, page), page) catch null;
}

/// A fiber stack: an anonymous mapping whose lowest page is inaccessible, so overflowing
/// the stack faults immediately instead of corrupting the neighboring memory.
const Stack = struct {
    memory: []align(std.heap.page_size_min) u8,

    fn map(usable_size: usize) !Stack {
        const page = std.heap.pageSize();
        const len = stackMappingLen(usable_size) orelse return error.OutOfMemory;
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

test "spawn fails without touching the stack pool while fail_stack_maps is set" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var log: Log = .{};
    _ = try s.spawn("y", yielder, .{ &s, &log, 1, 0 });
    s.run();
    try testing.expectEqual(@as(usize, 1), s.free_stacks.items.len);

    s.fail_stack_maps = 1;
    try testing.expectError(error.OutOfMemory, s.spawn("y", yielder, .{ &s, &log, 2, 0 }));
    try testing.expectEqual(@as(u32, 0), s.fail_stack_maps);
    try testing.expectEqual(@as(usize, 1), s.free_stacks.items.len);
    try testing.expectEqual(@as(usize, 0), s.live);

    _ = try s.spawn("y", yielder, .{ &s, &log, 3, 0 });
    try testing.expectEqual(@as(usize, 0), s.free_stacks.items.len);
    s.run();
    try testing.expectEqualSlices(u32, &.{ 101, 103 }, log.items());
}

test "abandonLive unmaps blocked fibers so deinit sees none live" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var log: Log = .{};
    var first: Gate = .{ .log = &log };
    var second: Gate = .{ .log = &log };
    _ = try s.spawn("waiter", waitOnGate, .{ &s, &first });
    _ = try s.spawn("waiter", waitOnGate, .{ &s, &second });
    _ = try s.spawn("y", yielder, .{ &s, &log, 7, 0 });
    s.run();
    try testing.expectEqual(@as(usize, 2), s.live);
    try testing.expectEqual(@as(usize, 2), s.abandonLive());
    try testing.expectEqual(@as(usize, 0), s.live);
    try testing.expectEqual(@as(?*Fiber, null), s.firstLive());
    try testing.expectEqual(@as(usize, 1), s.stackCount());
    try testing.expectEqualSlices(u32, &.{ 1, 1, 107 }, log.items());
}

test "switches counts each return of a fiber to the scheduler" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var log: Log = .{};
    var gate: Gate = .{ .log = &log };
    _ = try s.spawn("waiter", waitOnGate, .{ &s, &gate });
    try testing.expectEqual(0, s.switches.load(.acquire));
    s.run(); // blocks once
    try testing.expectEqual(1, s.switches.load(.acquire));
    s.wake(gate.waiter.?);
    s.run(); // finishes
    try testing.expectEqual(2, s.switches.load(.acquire));
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

/// Publishes the address of a local of its own stack, then blocks until `release` wakes it.
fn holdStackLocal(s: *Scheduler, published: *usize, self: *?*Fiber) void {
    var local: u8 = 0;
    std.mem.doNotOptimizeAway(&local);
    published.* = @intFromPtr(&local);
    self.* = s.currentFiber();
    s.block();
}

/// Checks another fiber's published local, its own local and a heap address, then wakes
/// the other fiber.
fn checkOnStack(s: *Scheduler, other_addr: *const usize, other: *const ?*Fiber, heap_addr: usize, out: *[3]bool) void {
    var local: u8 = 0;
    std.mem.doNotOptimizeAway(&local);
    out.* = .{ s.onLiveStack(other_addr.*), s.onLiveStack(@intFromPtr(&local)), s.onLiveStack(heap_addr) };
    s.wake(other.*.?);
}

test "spawn returns StackTooSmall for a zero stack size" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 0 });
    defer s.deinit();
    var out: usize = 0;
    try testing.expectError(error.StackTooSmall, s.spawn("a", recordStack, .{&out}));
    try testing.expectEqual(@as(usize, 0), s.stackCount());
    try testing.expectEqual(@as(usize, 0), s.live);
}

fn takeBig(big: [8192]u8, out: *u8) void {
    out.* = big[8191];
}

test "spawn returns StackTooSmall for arguments larger than the stack, and holds no stack" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 4096 });
    defer s.deinit();
    var out: u8 = 0;
    const big: [8192]u8 = @splat(7);
    try testing.expectError(error.StackTooSmall, s.spawn("big", takeBig, .{ big, &out }));
    try testing.expectEqual(@as(usize, 0), s.stackCount());
    try testing.expectEqual(@as(usize, 0), s.live);
}

test "spawn returns OutOfMemory for a stack size too large to map" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = std.math.maxInt(usize) });
    defer s.deinit();
    var out: usize = 0;
    try testing.expectError(error.OutOfMemory, s.spawn("a", recordStack, .{&out}));
    try testing.expectEqual(@as(usize, 0), s.stackCount());
}

test "stackMappingLen rounds up to whole pages plus a guard page, or is null on overflow" {
    const page = std.heap.pageSize();
    try testing.expectEqual(@as(?usize, null), stackMappingLen(std.math.maxInt(usize)));
    try testing.expectEqual(@as(?usize, page), stackMappingLen(0));
    try testing.expectEqual(@as(?usize, 2 * page), stackMappingLen(1));
    try testing.expectEqual(@as(?usize, 2 * page), stackMappingLen(page));
    try testing.expectEqual(@as(?usize, 3 * page), stackMappingLen(page + 1));
    // The largest whole-page length is maxInt - page + 1: one guard page below it leaves
    // maxInt - 2 * page + 1 usable bytes, and one byte more no longer fits.
    const largest_len = std.math.maxInt(usize) - page + 1;
    try testing.expectEqual(@as(?usize, largest_len), stackMappingLen(largest_len - page));
    try testing.expectEqual(@as(?usize, null), stackMappingLen(largest_len - page + 1));
}

test "onLiveStack tells every live fiber's stack from other memory" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 64 * 1024 });
    defer s.deinit();
    const heap = try testing.allocator.create(u8);
    defer testing.allocator.destroy(heap);
    var held: usize = 0;
    var holder: ?*Fiber = null;
    _ = try s.spawn("hold", holdStackLocal, .{ &s, &held, &holder });
    s.run(); // the holder publishes its local and blocks
    try testing.expect(holder != null);
    // Outside any fiber: the blocked fiber's stack still counts, the heap doesn't.
    try testing.expect(s.onLiveStack(held));
    try testing.expect(!s.onLiveStack(@intFromPtr(heap)));
    try testing.expect(!s.onLiveStack(@intFromPtr(&held)));
    var seen: [3]bool = .{ false, false, true };
    _ = try s.spawn("check", checkOnStack, .{ &s, &held, &holder, @intFromPtr(heap), &seen });
    s.run();
    try testing.expectEqual([3]bool{ true, true, false }, seen);
    try testing.expectEqual(@as(usize, 0), s.live);
    // Both finished: their stacks are no longer live.
    try testing.expect(!s.onLiveStack(held));
    try testing.expect(!s.onLiveStack(@intFromPtr(heap)));
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

test "deinitLeakingLive frees the pool and leaves a blocked fiber's stack mapped" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 64 * 1024 });
    var first: usize = 0;
    _ = try s.spawn("a", recordStack, .{&first});
    s.run();
    var log: Log = .{};
    var gate: Gate = .{ .log = &log };
    _ = try s.spawn("stuck", waitOnGate, .{ &s, &gate });
    var second: usize = 0;
    _ = try s.spawn("b", recordStack, .{&second});
    s.run();
    try testing.expectEqual(@as(usize, 1), s.live);
    try testing.expectEqual(@as(usize, 2), s.stackCount());
    try testing.expectEqual(@as(usize, 2 * 64 * 1024), s.stackBytes());
    const stuck = gate.waiter.?;
    s.deinitLeakingLive();
    // The blocked fiber's header is on its stack, which is still mapped. Unmap it here so
    // the test process doesn't keep it.
    try testing.expectEqual(Fiber.State.blocked, stuck.state);
    stuck.stack.unmap();
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

test "two fibers can yield to each other many times" {
    var s: Scheduler = .init(testing.allocator, .{});
    defer s.deinit();
    var counter: u64 = 0;
    const n = 200_000;
    _ = try s.spawn("a", pingPong, .{ &s, &counter, n });
    _ = try s.spawn("b", pingPong, .{ &s, &counter, n });
    s.run();
    try testing.expectEqual(@as(u64, 2 * n), counter);
}

noinline fn parkHere(s: *Scheduler, gate: *Gate) void {
    gate.waiter = s.currentFiber();
    s.block();
    gate.log.add(2);
}

fn parkCaller(s: *Scheduler, gate: *Gate) void {
    parkHere(s, gate);
    std.mem.doNotOptimizeAway(gate);
}

test "blockedTrace unwinds a blocked fiber's stack through its callers" {
    var s: Scheduler = .init(testing.allocator, .{ .stack_size = 256 * 1024 });
    defer s.deinit();
    var log: Log = .{};
    var gate: Gate = .{ .log = &log };
    const f = try s.spawn("parked", parkCaller, .{ &s, &gate });
    var buf: [32]usize = undefined;
    try testing.expectEqual(@as(usize, 0), blockedTrace(f, &buf).len); // ready, not blocked
    s.run();
    try testing.expectEqual(f, s.firstLive().?);
    const trace = blockedTrace(f, &buf);
    if (std.debug.SelfInfo != void and std.options.allow_stack_tracing and std.debug.cpu_context.Native != noreturn) {
        try testing.expect(trace.len >= 2);
        // One of the frames returns into parkCaller, which called parkHere.
        const start = @intFromPtr(&parkCaller);
        var found = false;
        for (trace) |addr| found = found or (addr > start and addr < start + 4096);
        try testing.expect(found);
    }
    s.wake(f);
    s.run();
    try testing.expectEqual(@as(?*Fiber, null), s.firstLive());
}
