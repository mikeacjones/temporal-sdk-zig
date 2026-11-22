//! The hang report of the project's test runner (tools/test_runner/main.zig).
//!
//! A detached watchdog thread watches the test that the runner's thread is running. When one
//! test has run for longer than the limit, the watchdog prints, to stderr, the test's name, the
//! elapsed time and the stack of every thread in the process, then lets the test go on. With
//! `zig build test --test-timeout`, the build runner prints the test's stderr in its 'timed
//! out' error, so the report says where a hung test waits. AGENTS.md ("Hang reports")
//! documents the format.
//!
//! "Stderr" here is the one the process had when `start` ran: the report writes to a copy of
//! file descriptor 2 taken then, so a test that points fd 2 at /dev/null while it starts a dev
//! server still gets its report seen.
//!
//! Stacks are read by freezing one thread at a time: SIGURG is sent to it, its handler copies
//! the interrupted registers into a global slot and spins until the watchdog lets it go.
//!
//! The invariant: the watchdog reads a thread's stack only while that thread is frozen, and
//! only through a copy the kernel makes, which fails instead of faulting on a bad address.
//! While a thread is frozen the watchdog takes no lock, allocates nothing and calls nothing
//! that could wait on another thread, so a freeze always ends, whatever the frozen thread or
//! any other holds. That is why the frozen stack is walked by its frame pointers (which Zig
//! keeps in Debug and ReleaseSafe) and not by std.debug's unwinder, which takes a lock that
//! any thread capturing a stack trace holds. Symbols are looked up only after every thread
//! runs again, and each row is formatted before stderr is locked, so the report never holds
//! stderr while it waits on that lock.
//!
//! Nor does the report wait without end on a lock another thread holds, frozen or not: a hung
//! test may well be stuck holding one. It tries std's stderr lock for at most `lock_wait`, and
//! when it stays taken, the rest of the report is written without it. std.debug's symbol lock
//! cannot be tried: std takes it inside each lookup, again for every frame, so a thread can take
//! it between any two of them. So the rows are formatted and written by a helper thread, which
//! names itself "hang-report-sym" (macOS can only name the calling thread), and whose lookups go
//! through an Io that records how long it waits on futexes. When its futex waits during the
//! report add up to `lock_wait`, counting the one in progress, the watchdog takes the rest of the
//! report over and shows bare addresses; the helper, if it ever goes on, writes nothing more. A
//! thread that keeps taking and releasing the lock therefore slows the report by at most
//! `lock_wait` in all, whatever the number of frames.
//!
//! Nothing here may end the process: every failure becomes a `test-hang-report: ` line.

const builtin = @import("builtin");
const std = @import("std");
const Io = std.Io;
const posix = std.posix;
const cpu_context = std.debug.cpu_context;

const os = builtin.os.tag;
const is_linux = os == .linux;
const is_darwin = os.isDarwin();

/// Whether this target can freeze threads and walk their frame records. Elsewhere a report
/// has no stacks.
const have_stacks = (is_linux or is_darwin) and
    (builtin.cpu.arch == .aarch64 or builtin.cpu.arch == .x86_64) and
    std.options.allow_stack_tracing;

const prefix = "test-hang-report: ";
/// The most threads one report shows.
const max_threads = 512;
/// The most frames one thread's stack shows.
const max_frames = 64;
/// How long a thread may take to answer the freeze signal.
const freeze_limit: Io.Duration = .fromSeconds(1);
/// The longest sleep between two looks at the running test.
const poll_interval: Io.Duration = .fromMilliseconds(250);
const freeze_signal: posix.SIG = .URG;

const no_test = std.math.maxInt(usize);

/// Set once by `start` before the watchdog exists; read by `begin`, `end` and the watchdog.
var limit_seconds: u32 = 0;
var watching = false;
/// The id (see `currentThreadId`) of the thread that runs the tests.
var test_thread_id: u64 = 0;

// The running test, written only by the test thread and read by the watchdog as a sequence
// lock: `generation` is odd while `begin` or `end` writes, and changes with every write, so
// that the watchdog never pairs one test's name with another's start time.
var generation: std.atomic.Value(u64) = .init(0);
var test_index: std.atomic.Value(usize) = .init(no_test);
var test_start_ns: std.atomic.Value(i64) = .init(0);

fn io() Io {
    return Io.Threaded.global_single_threaded.io();
}

fn nowNanoseconds() i64 {
    return @truncate(Io.Clock.awake.now(io()).nanoseconds);
}

/// Spawns the detached watchdog thread (named "hang-report"). The thread that calls it is
/// recorded as the thread that runs the tests. Called once, from main, before
/// mainServer or mainTerminal. Never in a builtin.fuzz build.
pub fn start(seconds: u32) void {
    if (seconds == 0 or watching) return;
    limit_seconds = seconds;
    report_fd = saveStderr();
    test_thread_id = currentThreadId();
    const thread = std.Thread.spawn(.{}, watchdogMain, .{}) catch |err| {
        print("cannot start the watchdog thread: {t}; hang reports are off\n", .{err});
        return;
    };
    thread.detach();
    watching = true;
}

/// The test at builtin.test_functions[test_index] starts on this thread now.
pub fn begin(index: usize) void {
    @disableInstrumentation();
    if (!watching) return;
    const started = nowNanoseconds();
    _ = generation.fetchAdd(1, .seq_cst);
    test_index.store(index, .seq_cst);
    test_start_ns.store(started, .seq_cst);
    _ = generation.fetchAdd(1, .seq_cst);
}

/// The test begun last has returned.
pub fn end() void {
    @disableInstrumentation();
    if (!watching) return;
    _ = generation.fetchAdd(1, .seq_cst);
    test_index.store(no_test, .seq_cst);
    _ = generation.fetchAdd(1, .seq_cst);
}

const Running = struct { generation: u64, index: usize, start_ns: i64 };

/// The running test, or null when none runs or `begin` or `end` is writing.
fn running() ?Running {
    const before = generation.load(.seq_cst);
    if (before % 2 != 0) return null;
    const index = test_index.load(.seq_cst);
    const start_ns = test_start_ns.load(.seq_cst);
    if (generation.load(.seq_cst) != before) return null;
    if (index == no_test) return null;
    return .{ .generation = before, .index = index, .start_ns = start_ns };
}

fn watchdogMain() void {
    setOwnName("hang-report");
    const limit_ns: i64 = @as(i64, limit_seconds) * std.time.ns_per_s;
    var reported: ?u64 = null;
    while (true) {
        const wait: Io.Duration = if (running()) |r| wait: {
            if (reported == r.generation) break :wait poll_interval;
            const elapsed = nowNanoseconds() - r.start_ns;
            if (elapsed >= limit_ns) {
                reported = r.generation;
                report(r.index, @intCast(@divTrunc(@max(elapsed, 0), std.time.ns_per_s)));
                break :wait poll_interval;
            }
            break :wait .fromNanoseconds(@min(limit_ns - elapsed, poll_interval.nanoseconds));
        } else poll_interval;
        io().sleep(wait, .awake) catch {};
    }
}

fn setOwnName(name: [:0]const u8) void {
    if (is_linux) {
        _ = std.os.linux.prctl(@backingInt(std.os.linux.PR.SET_NAME), @intFromPtr(name.ptr), 0, 0, 0);
    } else if (is_darwin) {
        _ = std.c.pthread_setname_np(name.ptr);
    }
}

/// The id a debugger shows for the calling thread: the Linux tid, or macOS's
/// pthread_threadid_np.
fn currentThreadId() u64 {
    if (is_linux) return @intCast(std.os.linux.gettid());
    if (is_darwin) {
        var id: u64 = 0;
        _ = std.c.pthread_threadid_np(null, &id);
        return id;
    }
    return 0;
}

/// Where the report is written: a close-on-exec copy of file descriptor 2 taken by `start`,
/// before any test runs, so that a test that points fd 2 elsewhere (as
/// src/bridge/EphemeralServer.zig's OutputRedirect does, to /dev/null, while a dev server
/// downloads and starts) does not send the report there too. Fd 2 itself if the copy failed.
var report_fd: posix.fd_t = posix.STDERR_FILENO;

/// A close-on-exec copy of file descriptor 2, so that the processes a test starts do not
/// inherit it, or fd 2 itself when it cannot be copied.
fn saveStderr() posix.fd_t {
    const min_fd = 3;
    const rc = if (builtin.link_libc)
        std.c.fcntl(posix.STDERR_FILENO, posix.F.DUPFD_CLOEXEC, @as(c_int, min_fd))
    else if (is_linux)
        std.os.linux.fcntl(posix.STDERR_FILENO, posix.F.DUPFD_CLOEXEC, min_fd)
    else
        return posix.STDERR_FILENO;
    if (posix.errno(rc) != .SUCCESS) return posix.STDERR_FILENO;
    return @intCast(rc);
}

/// Writes one line, or several, to stderr. Errors are ignored: the report only prints.
fn print(comptime format: []const u8, args: anytype) void {
    var buffer: [4096]u8 = undefined;
    var w: Io.Writer = .fixed(&buffer);
    w.print(prefix ++ format, args) catch {};
    writeStderr(w.buffered());
}

/// The longest the report waits for a lock another thread may hold.
const lock_wait: Io.Duration = .fromSeconds(1);

/// Set when std's stderr lock stayed taken for `lock_wait`; the rest of the report then writes
/// without it. Reset by each report.
var stderr_held = false;

fn writeStderr(bytes: []const u8) void {
    if (!stderr_held) {
        const debug_io = std.Options.debug_io;
        const deadline = Io.Clock.awake.now(io()).addDuration(lock_wait);
        while (true) {
            const locked = debug_io.tryLockStderr(&.{}, null) catch null;
            if (locked) |_| {
                // The lock keeps the report's lines apart from other threads' writes to
                // stderr; the bytes go to `report_fd`, which may no longer be what fd 2 is.
                defer debug_io.unlockStderr();
                writeUnlocked(bytes);
                return;
            }
            if (Io.Clock.awake.now(io()).compare(.gte, deadline)) break;
            io().sleep(.fromMilliseconds(5), .awake) catch {};
        }
        stderr_held = true;
        writeUnlocked(prefix ++ "another thread holds std's stderr lock; the rest of this report is written without it\n");
    }
    writeUnlocked(bytes);
}

/// Writes to `report_fd` without std's stderr lock.
fn writeUnlocked(bytes: []const u8) void {
    var rest = bytes;
    while (rest.len != 0) {
        const rc = posix.system.write(report_fd, rest.ptr, rest.len);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                const written: usize = @intCast(rc);
                if (written == 0) return;
                rest = rest[@min(written, rest.len)..];
            },
            .INTR => {},
            else => return,
        }
    }
}

// One report at a time, from the watchdog only, so its storage is static.

const Outcome = enum {
    /// `traces[i]` holds the stack.
    captured,
    /// The test's watchdog; not frozen.
    watchdog,
    /// The signal could not be sent: the thread ended after the list was taken.
    exited,
    /// The thread did not run the handler within `freeze_limit`.
    no_answer,
    /// macOS: a Mach thread without a pthread.
    not_posix,
    /// The signal could not be sent for another reason, in `errors[i]`.
    signal_failed,
};

const Thread = struct {
    id: u64,
    name: [64]u8 = undefined,
    name_len: usize = 0,
    /// macOS: the pthread to signal; null for a thread without one.
    pthread: if (is_darwin) ?std.c.pthread_t else void = if (is_darwin) null else {},
    outcome: Outcome = .no_answer,
    err: i32 = 0,
    /// Linux, for `no_answer`: the kernel's view of the thread when it gave no answer.
    kernel: ?struct { state: u8, urg_pending: bool, urg_blocked: bool } = null,

    fn nameSlice(t: *const Thread) []const u8 {
        return t.name[0..t.name_len];
    }
};

var threads: [max_threads]Thread = undefined;
var traces: [max_threads]std.debug.StackTrace = undefined;
var addresses: [max_threads][max_frames]usize = undefined;

// The freeze slot. `state` is `idle`, `capturing`, `captured`, or `requestFor(id)` while the
// thread with that id is asked to freeze. Keeping the target's id in the same word as the
// request means a late handler of a thread whose turn has passed cannot take another's.
const idle: u64 = 0;
const capturing: u64 = 1;
const captured: u64 = 2;
fn requestFor(id: u64) u64 {
    return id | (1 << 63);
}
var state: std.atomic.Value(u64) = .init(idle);
var slot_context: ?cpu_context.Native = null;

const List = struct { count: usize, total: usize };

fn report(index: usize, elapsed_seconds: u64) void {
    const name = builtin.test_functions[index].name;
    stderr_held = false;
    if (!have_stacks) {
        print("'{s}' is still running after {d}s\n", .{ name, elapsed_seconds });
        print("thread stacks are not available on this OS\n", .{});
        print("end of the report for '{s}'\n", .{name});
        return;
    }
    const watchdog_id = currentThreadId();
    const list = listThreads(watchdog_id) catch |err| {
        releaseDarwinPorts();
        print("'{s}' is still running after {d}s\n", .{ name, elapsed_seconds });
        print("cannot list the threads: {t}\n", .{err});
        print("end of the report for '{s}'\n", .{name});
        return;
    };
    defer releaseDarwinPorts();
    print("'{s}' is still running after {d}s; the stacks of its {d} threads follow\n", .{ name, elapsed_seconds, list.total });
    freezeAll(threads[0..list.count], watchdog_id);
    writeReport(threads[0..list.count], list.total);
    print("end of the report for '{s}'\n", .{name});
}

/// Fills `threads` with the test thread first, then the others in the order the OS lists
/// them. `total` counts every thread, `count` those that fit.
fn listThreads(watchdog_id: u64) !List {
    var list: List = .{ .count = 1, .total = 1 };
    threads[0] = .{ .id = test_thread_id };
    var found_test = false;
    const Add = struct {
        fn add(l: *List, t: Thread, test_id: u64, found: *bool) void {
            if (t.id == test_id and !found.*) {
                found.* = true;
                threads[0] = t;
                return;
            }
            l.total += 1;
            if (l.count < max_threads) {
                threads[l.count] = t;
                l.count += 1;
            }
        }
    };
    if (is_linux) {
        var dir = try Io.Dir.openDirAbsolute(io(), "/proc/self/task", .{ .iterate = true });
        defer dir.close(io());
        var it = dir.iterate();
        while (try it.next(io())) |entry| {
            // A Linux tid is a positive pid_t, so it fits tgkill without a checked cast.
            const tid = std.fmt.parseUnsigned(u31, entry.name, 10) catch continue;
            var t: Thread = .{ .id = tid };
            readLinuxName(&t);
            Add.add(&list, t, test_thread_id, &found_test);
        }
    } else if (is_darwin) {
        try listDarwinThreads();
        for (darwin_ports[0..darwin_port_count]) |port| {
            var t: Thread = .{ .id = 0 };
            if (pthread_from_mach_thread_np(port)) |pt| {
                t.pthread = pt;
                _ = std.c.pthread_threadid_np(pt, &t.id);
                var buf: [64:0]u8 = @splat(0);
                if (std.c.pthread_getname_np(pt, &buf, buf.len) == 0) {
                    const len = std.mem.indexOfScalar(u8, &buf, 0) orelse buf.len;
                    @memcpy(t.name[0..len], buf[0..len]);
                    t.name_len = len;
                }
            } else {
                t.id = machThreadId(port);
                t.outcome = .not_posix;
            }
            Add.add(&list, t, test_thread_id, &found_test);
        }
    }
    if (!found_test) {
        // The test thread is always alive while its test runs; keep its row anyway.
        threads[0] = .{ .id = test_thread_id, .outcome = .exited };
    }
    for (threads[0..list.count]) |*t| {
        if (t.id == watchdog_id) t.outcome = .watchdog;
    }
    return list;
}

/// Fills `t.kernel` from /proc/self/task/<tid>/status: the scheduler state and whether
/// SIGURG is pending for the thread or blocked by it.
fn readLinuxSignalState(t: *Thread) void {
    var path_buf: [64]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/proc/self/task/{d}/status", .{t.id}) catch return;
    var text_buf: [4096]u8 = undefined;
    const text = Io.Dir.cwd().readFile(io(), path, &text_buf) catch return;
    const urg_bit: u64 = @as(u64, 1) << (@as(u6, @intCast(@backingInt(freeze_signal))) - 1);
    var kernel: @typeInfo(@FieldType(Thread, "kernel")).optional.child = .{ .state = '?', .urg_pending = false, .urg_blocked = false };
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        const key = line[0..colon];
        if (std.mem.eql(u8, key, "State")) {
            if (value.len != 0) kernel.state = value[0];
        } else if (std.mem.eql(u8, key, "SigPnd") or std.mem.eql(u8, key, "SigBlk")) {
            const mask = std.fmt.parseUnsigned(u64, value, 16) catch continue;
            if (key[3] == 'P') kernel.urg_pending = mask & urg_bit != 0 else kernel.urg_blocked = mask & urg_bit != 0;
        }
    }
    t.kernel = kernel;
}

fn readLinuxName(t: *Thread) void {
    var path_buf: [64]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/proc/self/task/{d}/comm", .{t.id}) catch return;
    const text = Io.Dir.cwd().readFile(io(), path, &t.name) catch return;
    t.name_len = std.mem.trimEnd(u8, text, "\n").len;
}

// macOS: the pthread of a Mach thread, or null for a thread that has none; std lacks it.
extern "c" fn pthread_from_mach_thread_np(std.c.mach_port_t) ?std.c.pthread_t;
// macOS: copies memory through the kernel; std declares it but does not export it from std.c.
extern "c" fn mach_vm_read_overwrite(
    target_task: std.c.mach_port_t,
    address: std.c.mach_vm_address_t,
    size: std.c.mach_vm_size_t,
    data: std.c.mach_vm_address_t,
    outsize: *std.c.mach_vm_size_t,
) std.c.kern_return_t;
var darwin_ports: []std.c.mach_port_t = &.{};
var darwin_port_count: usize = 0;

fn listDarwinThreads() !void {
    var ports: std.c.mach_port_array_t = undefined;
    var count: std.c.mach_msg_type_number_t = 0;
    if (std.c.task_threads(std.c.mach_task_self(), &ports, &count) != 0) return error.TaskThreadsFailed;
    darwin_ports = ports[0..count];
    darwin_port_count = count;
}

/// Releases every send right and the array from `listDarwinThreads`.
fn releaseDarwinPorts() void {
    if (!is_darwin) return;
    for (darwin_ports) |port| _ = std.c.mach_port_deallocate(std.c.mach_task_self(), port);
    if (darwin_ports.len != 0) {
        _ = std.c.vm_deallocate(
            std.c.mach_task_self(),
            @intFromPtr(darwin_ports.ptr),
            darwin_ports.len * @sizeOf(std.c.mach_port_t),
        );
    }
    darwin_ports = &.{};
    darwin_port_count = 0;
}

fn machThreadId(port: std.c.mach_port_t) u64 {
    var info: std.c.thread_identifier_info = undefined;
    var count: std.c.mach_msg_type_number_t = std.c.THREAD.IDENTIFIER.INFO_COUNT;
    if (std.c.thread_info(port, std.c.THREAD.IDENTIFIER.INFO, @ptrCast(&info), &count) != 0) return 0;
    return info.thread_id;
}

/// Freezes each thread in turn, records its return addresses and lets it go. See the
/// invariant at the top of this file.
fn freezeAll(list: []Thread, watchdog_id: u64) void {
    var previous: posix.Sigaction = undefined;
    const action: posix.Sigaction = .{
        .handler = .{ .sigaction = freezeHandler },
        .mask = posix.sigfillset(),
        .flags = posix.SA.SIGINFO | posix.SA.ONSTACK | posix.SA.RESTART,
    };
    posix.sigaction(freeze_signal, &action, &previous);
    defer posix.sigaction(freeze_signal, &previous, null);

    for (list, 0..) |*t, i| {
        if (t.outcome == .watchdog or t.outcome == .not_posix or t.outcome == .exited) continue;
        if (t.id == watchdog_id) continue;
        freezeOne(t, i);
    }
}

fn freezeOne(t: *Thread, i: usize) void {
    slot_context = null;
    state.store(requestFor(t.id), .seq_cst);
    switch (sendFreeze(t)) {
        .sent => {},
        .exited => {
            state.store(idle, .seq_cst);
            t.outcome = .exited;
            return;
        },
        .failed => |code| {
            state.store(idle, .seq_cst);
            t.outcome = .signal_failed;
            t.err = code;
            return;
        },
    }
    const deadline = Io.Clock.awake.now(io()).addDuration(freeze_limit);
    while (state.load(.seq_cst) != captured) {
        if (Io.Clock.awake.now(io()).compare(.gte, deadline)) {
            if (state.cmpxchgStrong(requestFor(t.id), idle, .seq_cst, .seq_cst) == null) {
                t.outcome = .no_answer;
                if (is_linux) readLinuxSignalState(t);
                return;
            }
            // The thread took the request: it is a copy away from `captured`.
            while (state.load(.seq_cst) != captured) std.Thread.yield() catch {};
            break;
        }
        io().sleep(.fromMilliseconds(1), .awake) catch {};
    }
    // The thread is frozen from here to the store of `idle`: only `walkFrames`, which takes no
    // lock and reads memory only through the kernel, runs in between.
    traces[i] = if (slot_context) |*context|
        walkFrames(context, &addresses[i])
    else
        .{ .return_addresses = &.{}, .skipped = .none };
    t.outcome = .captured;
    state.store(idle, .seq_cst);
}

/// The return addresses of a frozen thread, from its interrupted registers and its chain of
/// frame records ({saved frame pointer, return address} at the frame pointer, on aarch64 and
/// x86_64 alike). The first entry is the interrupted PC plus 1, as std.debug records it, since
/// printing subtracts 1 from each. The walk stops at a null frame pointer or return address
/// (the end of the stack), at a record that cannot be read or does not lie above the last one
/// (a frame without a frame pointer), or after `max_frames`; the last two are shown as
/// "(additional stack frames may have been skipped...)".
fn walkFrames(context: *const cpu_context.Native, buf: *[max_frames]usize) std.debug.StackTrace {
    var n: usize = 0;
    buf[n] = context.getPc() +| 1;
    n += 1;
    var fp = context.getFp();
    if (builtin.cpu.arch == .aarch64) {
        // A function that has not stored its frame record (a leaf, such as a system call stub)
        // still holds its return address in the link register.
        const lr = std.debug.stripInstructionPtrAuthCode(context.x[30]);
        const saved = if (fp != 0) readRecord(fp) else null;
        if (lr > 1 and (saved == null or std.debug.stripInstructionPtrAuthCode(saved.?[1]) != lr)) {
            buf[n] = lr;
            n += 1;
        }
    }
    while (n < max_frames) {
        if (fp == 0) return .{ .return_addresses = buf[0..n], .skipped = .none };
        if (fp % @alignOf(usize) != 0) break;
        const record = readRecord(fp) orelse break;
        const return_address = std.debug.stripInstructionPtrAuthCode(record[1]);
        if (return_address <= 1) return .{ .return_addresses = buf[0..n], .skipped = .none };
        buf[n] = return_address;
        n += 1;
        // The stack grows down, so each caller's record lies above its callee's.
        if (record[0] != 0 and record[0] <= fp) break;
        fp = record[0];
    }
    return .{ .return_addresses = buf[0..n], .skipped = .unknown };
}

/// The two words at `address`, copied by the kernel, or null where they cannot be read. It
/// never faults and takes no lock.
fn readRecord(address: usize) ?[2]usize {
    var words: [2]usize = undefined;
    if (is_linux) {
        const linux = std.os.linux;
        const local = [1]posix.iovec{.{ .base = @ptrCast(&words), .len = @sizeOf([2]usize) }};
        const remote = [1]posix.iovec_const{.{ .base = @ptrFromInt(address), .len = @sizeOf([2]usize) }};
        const rc = linux.process_vm_readv(linux.getpid(), &local, &remote, 0);
        if (linux.errno(rc) != .SUCCESS or rc != @sizeOf([2]usize)) return null;
        return words;
    }
    if (is_darwin) {
        var copied: std.c.mach_vm_size_t = 0;
        const kr = mach_vm_read_overwrite(std.c.mach_task_self(), address, @sizeOf([2]usize), @intFromPtr(&words), &copied);
        if (kr != 0 or copied != @sizeOf([2]usize)) return null;
        return words;
    }
    return null;
}

const Send = union(enum) { sent, exited, failed: i32 };

fn sendFreeze(t: *const Thread) Send {
    if (is_linux) {
        const linux = std.os.linux;
        const rc = linux.tgkill(linux.getpid(), @intCast(t.id), freeze_signal);
        return switch (linux.errno(rc)) {
            .SUCCESS => .sent,
            .SRCH => .exited,
            else => |e| .{ .failed = @intCast(@backingInt(e)) },
        };
    }
    if (is_darwin) {
        const pt = t.pthread orelse return .exited;
        const rc = std.c.pthread_kill(pt, freeze_signal);
        return switch (rc) {
            0 => .sent,
            @backingInt(posix.E.SRCH) => .exited,
            else => .{ .failed = rc },
        };
    }
    return .{ .failed = 0 };
}

/// Runs on the thread being frozen. It takes no lock, allocates nothing, does no I/O and does
/// not touch Io: it copies the interrupted registers and spins until the watchdog is done.
fn freezeHandler(sig: posix.SIG, info: *const posix.siginfo_t, ctx: ?*anyopaque) callconv(.c) void {
    _ = sig;
    _ = info;
    const saved_errno = if (builtin.link_libc) std.c._errno().* else 0;
    defer if (builtin.link_libc) {
        std.c._errno().* = saved_errno;
    };
    if (state.cmpxchgStrong(requestFor(currentThreadId()), capturing, .seq_cst, .seq_cst) != null) return;
    slot_context = cpu_context.fromPosixSignalContext(ctx);
    state.store(captured, .seq_cst);
    // No time limit: while this thread is frozen the watchdog only copies memory through the
    // kernel, which cannot wait on any thread.
    while (state.load(.seq_cst) == captured) std.Thread.yield() catch {};
}

/// One row of the report, formatted before stderr is locked: symbol lookup can wait on
/// std.debug's lock, and stderr must not be held meanwhile.
var row_buffer: [64 * 1024]u8 = undefined;

fn writeReport(list: []const Thread, total: usize) void {
    const written = writeRowsWithSymbols(list, total);
    for (list[written..], written..) |*t, i| {
        var row: Io.Writer = .fixed(&row_buffer);
        writeRow(&row, t, i, total, false);
        writeStderr(row.buffered());
    }
    if (total > list.len) print("{d} more threads not shown\n", .{total - list.len});
}

// The symbol lookup thread. One exists at a time: from `writeRowsWithSymbols` until it has
// written its last row or, abandoned, its lookup returns.

/// The row the helper is on; `list.len` once it has written them all.
var lookup_row: std.atomic.Value(usize) = .init(0);
/// Whether the helper may write the row it is formatting. The helper moves it from
/// `formatting` to `writing` before it writes a row, and the watchdog from `formatting` to
/// `abandoned` when it takes the rest over, so exactly one of them writes each row.
var lookup_phase: std.atomic.Value(enum(u32) { formatting, writing, abandoned }) = .init(.formatting);
/// Set from the helper's spawn until it returns.
var lookup_busy: std.atomic.Value(bool) = .init(false);
/// When the helper began the futex wait it is in (`nowNanoseconds`), or 0.
var lookup_wait_since: std.atomic.Value(i64) = .init(0);
/// Nanoseconds the helper spent in finished futex waits during the current report. `markWait`
/// adds a wait here before it clears `lookup_wait_since`.
var lookup_waited_ns: std.atomic.Value(i64) = .init(0);

/// The watchdog's view of how long the helper has waited on futexes during this report, the
/// wait in progress included. `lookup_wait_since` is loaded first: a wait that finishes between
/// the two loads is then counted twice at most (giving up a little early), never missed.
fn lookupWaitedNanoseconds() i64 {
    const since = lookup_wait_since.load(.acquire);
    const waited = lookup_waited_ns.load(.acquire);
    return waited + (if (since != 0) nowNanoseconds() - since else 0);
}
/// The helper's row, apart from the watchdog's `row_buffer`.
var lookup_row_buffer: [64 * 1024]u8 = undefined;
threadlocal var is_lookup_thread = false;

/// Writes rows with their symbols from a helper thread, and returns how many it wrote: all of
/// them, or those before the row during whose lookup its futex waits reached `lock_wait` in
/// all. The caller writes the rest with bare addresses.
fn writeRowsWithSymbols(list: []const Thread, total: usize) usize {
    if (lookup_busy.load(.acquire)) {
        print("an earlier report's symbol lookup still waits on std.debug's symbol lock; the stacks that follow are bare addresses\n", .{});
        return 0;
    }
    lookup_row.store(0, .release);
    lookup_phase.store(.formatting, .release);
    lookup_wait_since.store(0, .release);
    lookup_waited_ns.store(0, .release);
    lookup_busy.store(true, .release);
    const helper = std.Thread.spawn(.{}, lookupMain, .{ list, total }) catch |err| {
        lookup_busy.store(false, .release);
        print("cannot start the symbol lookup thread: {t}; the stacks that follow are bare addresses\n", .{err});
        return 0;
    };
    helper.detach();
    while (true) {
        const row = lookup_row.load(.acquire);
        if (row == list.len) return row;
        if (lookupWaitedNanoseconds() >= lock_wait.toNanoseconds()) {
            if (lookup_phase.cmpxchgStrong(.formatting, .abandoned, .acq_rel, .acquire) == null) {
                print("another thread holds std.debug's symbol lock; the stacks that follow are bare addresses\n", .{});
                return lookup_row.load(.acquire);
            }
        }
        io().sleep(.fromMilliseconds(5), .awake) catch {};
    }
}

fn lookupMain(list: []const Thread, total: usize) void {
    // Before any lookup, so every wait the helper makes is a named thread's. macOS names only
    // the calling thread, so the watchdog cannot do it after the spawn.
    setOwnName("hang-report-sym");
    is_lookup_thread = true;
    defer lookup_busy.store(false, .release);
    for (list, 0..) |*t, i| {
        var row: Io.Writer = .fixed(&lookup_row_buffer);
        writeRow(&row, t, i, total, true);
        if (lookup_phase.cmpxchgStrong(.formatting, .writing, .acq_rel, .acquire) != null) return;
        writeStderr(row.buffered());
        lookup_row.store(i + 1, .release);
        lookup_phase.store(.formatting, .release);
    }
}

/// The Io the helper's lookups get in place of `io_`: the same, except that each futex wait
/// is recorded in `lookup_wait_since` and `lookup_waited_ns`. On any other thread, `io_` itself.
fn lookupIo(io_: Io) Io {
    if (!is_lookup_thread or io_.vtable == &lookup_vtable) return io_;
    lookup_base = io_;
    lookup_vtable = io_.vtable.*;
    lookup_vtable.futexWait = lookupFutexWait;
    lookup_vtable.futexWaitUncancelable = lookupFutexWaitUncancelable;
    return .{ .userdata = io_.userdata, .vtable = &lookup_vtable };
}
var lookup_base: Io = undefined;
var lookup_vtable: Io.VTable = undefined;

fn markWait(waiting: bool) void {
    // std may keep the Io it was given; only the helper's waits count.
    if (!is_lookup_thread) return;
    if (waiting) {
        lookup_wait_since.store(@max(1, nowNanoseconds()), .release);
        return;
    }
    // Added before the start is cleared, so the watchdog's sum never misses a finished wait.
    const since = lookup_wait_since.load(.acquire);
    if (since != 0) _ = lookup_waited_ns.fetchAdd(nowNanoseconds() - since, .release);
    lookup_wait_since.store(0, .release);
}

fn lookupFutexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    markWait(true);
    defer markWait(false);
    return lookup_base.vtable.futexWait(userdata, ptr, expected, timeout);
}

fn lookupFutexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    markWait(true);
    defer markWait(false);
    lookup_base.vtable.futexWaitUncancelable(userdata, ptr, expected);
}

fn writeRow(w: *Io.Writer, t: *const Thread, i: usize, total: usize, with_symbols: bool) void {
    const terminal: Io.Terminal = .{ .writer = w, .mode = .no_color };
    {
        w.print(prefix ++ "thread {d} of {d}, id {d}", .{ i + 1, total, t.id }) catch {};
        if (t.name_len != 0) w.print(", \"{s}\"", .{t.nameSlice()}) catch {};
        if (i == 0 and t.outcome != .watchdog) w.writeAll(" (running the test)") catch {};
        switch (t.outcome) {
            .captured => {
                w.writeAll(":\n") catch {};
                if (with_symbols) {
                    std.debug.writeStackTrace(&traces[i], terminal) catch {};
                } else {
                    for (traces[i].return_addresses) |address| w.print("0x{x}\n", .{address}) catch {};
                }
            },
            .watchdog => w.writeAll(" (this report's watchdog; stack not shown)\n") catch {},
            .exited => w.writeAll(": exited before its stack was read\n") catch {},
            .no_answer => {
                w.print(": did not stop within {d}s", .{freeze_limit.toSeconds()}) catch {};
                if (t.kernel) |k| w.print(" (state {c}, SIGURG {s}pending, {s}blocked)", .{
                    k.state,
                    if (k.urg_pending) "" else "not ",
                    if (k.urg_blocked) "" else "not ",
                }) catch {};
                w.writeAll("; stack not shown\n") catch {};
            },
            .not_posix => w.writeAll(": not a POSIX thread; stack not shown\n") catch {},
            .signal_failed => w.print(": cannot be signaled (errno {d}); stack not shown\n", .{t.err}) catch {},
        }
    }
    // A row longer than the buffer ends where it was cut, on a line of its own.
    if (w.end == w.buffer.len) {
        w.end -= @min(w.end, 64);
        w.writeAll("\n" ++ prefix ++ "(the rest of this stack was cut)\n") catch {};
    }
}

/// std.debug's SelfInfo for this target: every call is forwarded, on the report's symbol lookup
/// thread with `lookupIo`'s Io, so that the watchdog sees that thread wait on std's lock. The
/// runner installs it as `root.debug.SelfInfo` (main.zig). std's own lock is reachable as
/// `std.debug.getSelfDebugInfo().target`, which the fixtures moved_on.zig and
/// symbol_lock_late.zig in tools/checks/fixtures/hang_report take directly to stall the
/// report's symbol lookup.
pub const SelfInfo = if (TargetSelfInfo == void) void else struct {
    target: TargetSelfInfo,

    const Error = std.debug.SelfInfoError;

    pub const init: SelfInfo = .{ .target = TargetSelfInfo.init };
    pub const can_unwind = TargetSelfInfo.can_unwind;
    pub const UnwindContext = TargetSelfInfo.UnwindContext;

    pub fn deinit(si: *SelfInfo, io_: Io) void {
        si.target.deinit(io_);
    }

    pub fn getSymbols(
        si: *SelfInfo,
        io_: Io,
        symbol_allocator: std.mem.Allocator,
        text_arena: std.mem.Allocator,
        address: usize,
        resolve_inline_callers: bool,
        symbols: *std.ArrayList(std.debug.Symbol),
    ) Error!void {
        return si.target.getSymbols(lookupIo(io_), symbol_allocator, text_arena, address, resolve_inline_callers, symbols);
    }

    pub fn getModuleName(si: *SelfInfo, io_: Io, address: usize) Error![]const u8 {
        return si.target.getModuleName(lookupIo(io_), address);
    }

    pub fn getModuleSlide(si: *SelfInfo, io_: Io, address: usize) Error!usize {
        return si.target.getModuleSlide(lookupIo(io_), address);
    }

    pub fn unwindFrame(si: *SelfInfo, io_: Io, context: *UnwindContext) Error!usize {
        return si.target.unwindFrame(lookupIo(io_), context);
    }
};
const TargetSelfInfo = std.debug.TargetInfo(builtin.os.tag, builtin.cpu.arch);
