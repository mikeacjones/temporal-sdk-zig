//! The call sites in workflow code where a blocked coroutine waits, for the `__stack_trace`
//! query: its stack is unwound from the registers it saved when it blocked
//! (`Scheduler.blockedTrace`), and each return address is resolved to a source location with
//! the executable's debug info. Frames in the SDK's own source files and in the standard
//! library are left out, so what remains is workflow code, innermost first.
//!
//! Where optimization inlined calls, ELF debug info lists the inlined functions at each
//! address and every workflow one among them is written. Mach-O debug info, as std reads it,
//! gives only the innermost: an address then resolves to the inlined SDK or standard library
//! line, and the workflow frames inlined with it are left out, so the entry starts at the
//! nearest workflow caller that wasn't inlined. When no frame remains, the answer says
//! `unknown_location` instead.
//!
//! Every string written that the SDK didn't write itself goes through `text`, so the answer
//! is valid UTF-8 and only the SDK starts its lines.

const std = @import("std");
const Scheduler = @import("Scheduler.zig");
const Allocator = std.mem.Allocator;

/// The line written for a coroutine whose call site in workflow code can't be read: the
/// build has no debug info (or the target can't unwind a stack), or optimization inlined the
/// workflow frames away.
pub const unknown_location = "at an unknown location: no debug information covers this call site in workflow code (a build without debug info, or optimization inlined it)";

/// Return addresses read from one coroutine's stack. Deeper frames are left out.
const max_frames = 64;
/// Workflow frames written for one coroutine, innermost first. Deeper ones are left out.
const max_lines = 16;

const enabled = std.options.allow_stack_tracing and std.debug.SelfInfo != void and
    std.debug.cpu_context.Native != noreturn;

/// Reads source locations. Create one per answer with `init`, from a thread's own stack:
/// reading debug info takes more stack than a coroutine has, and file I/O the first time.
pub const Reader = struct {
    di: if (enabled) *std.debug.SelfInfo else void,
    /// The directory holding this SDK's source files, and the standard library's, as debug
    /// info spells them.
    sdk_root: []const u8,
    std_root: []const u8,
    /// Scratch memory for one address's symbols, not std's shared debug-info allocator (see
    /// `failure.addressesText`).
    scratch: std.heap.ArenaAllocator,
    prev_protection: std.Io.CancelProtection,

    /// Null when the build or target gives no source locations. `out` holds the roots.
    pub fn init(out: Allocator) error{OutOfMemory}!?Reader {
        if (comptime !enabled) return null;
        const di = std.debug.getSelfDebugInfo() catch return null;
        const io = std.Options.debug_io;
        // As `failure.addressesText`: a cancel pending on this thread would fail the debug-info
        // file open, and std caches that failure.
        var r: Reader = .{
            .di = di,
            .sdk_root = "",
            .std_root = "",
            .scratch = .init(std.heap.page_allocator),
            .prev_protection = io.swapCancelProtection(.blocked),
        };
        errdefer r.deinit();
        r.sdk_root = try r.root(out, @intFromPtr(&anchor) + 1, @src().file) orelse {
            r.deinit();
            return null;
        };
        r.std_root = try r.root(out, @intFromPtr(&std.debug.captureCurrentStackTrace) + 1, "debug.zig") orelse {
            r.deinit();
            return null;
        };
        return r;
    }

    pub fn deinit(r: *Reader) void {
        r.scratch.deinit();
        _ = std.Options.debug_io.swapCancelProtection(r.prev_protection);
    }

    /// The directory that the file holding `address` is in, as debug info names it, given
    /// that file's path `suffix` below that directory; null when it doesn't resolve.
    fn root(r: *Reader, out: Allocator, address: usize, suffix: []const u8) error{OutOfMemory}!?[]const u8 {
        for (try r.symbols(address)) |sym| {
            const loc = sym.source_location orelse continue;
            if (!std.mem.endsWith(u8, loc.file_name, suffix)) return null;
            return try out.dupe(u8, loc.file_name[0 .. loc.file_name.len - suffix.len]);
        }
        return null;
    }

    /// The symbols of the instruction before return address `address`: the function it is
    /// in, and, where the debug info records them (ELF builds), the functions inlined at it,
    /// innermost first. Empty when nothing resolves. Memory is `scratch`, valid until the
    /// next call.
    fn symbols(r: *Reader, address: usize) error{OutOfMemory}![]const std.debug.Symbol {
        _ = r.scratch.reset(.retain_capacity);
        const a = r.scratch.allocator();
        var list: std.ArrayList(std.debug.Symbol) = .empty;
        r.di.getSymbols(std.Options.debug_io, a, a, address -| 1, true, &list) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return &.{},
        };
        return list.items;
    }

    /// Whether `file` is workflow code: not the standard library's, and not one of this
    /// SDK's own files other than its tests (`*_test.zig`), whose workflows count as
    /// workflow code.
    fn isWorkflowCode(r: *const Reader, file: []const u8) bool {
        if (std.mem.startsWith(u8, file, r.std_root)) return false;
        if (std.mem.startsWith(u8, file, r.sdk_root)) return std.mem.endsWith(u8, file, "_test.zig");
        return true;
    }
};

/// Gives `Reader.init` an address in this file.
noinline fn anchor() void {
    std.mem.doNotOptimizeAway(@as(u8, 0));
}

/// Writes the workflow frames of blocked `fiber`, one line each ("at path/to/file.zig:12:5 in
/// function", the path and name through `text`), each starting with `indent`, or one
/// `unknown_location` line. Past `max_lines` call sites, a last line says the deeper ones
/// were left out. Notes progress on `sched` (`Scheduler.noteProgress`) after each frame it
/// reads.
pub fn write(reader: ?*Reader, sched: *Scheduler, w: *std.Io.Writer, fiber: *const Scheduler.Fiber, indent: []const u8) (error{OutOfMemory} || std.Io.Writer.Error)!void {
    var lines: usize = 0;
    if (reader) |r| frames: {
        var buf: [max_frames]usize = undefined;
        for (Scheduler.blockedTrace(fiber, &buf)) |addr| {
            defer sched.noteProgress();
            for (try r.symbols(addr)) |sym| {
                const loc = sym.source_location orelse continue;
                if (loc.file_name.len == 0 or !r.isWorkflowCode(loc.file_name)) continue;
                if (lines == max_lines) {
                    try w.print("{s}... deeper call sites left out\n", .{indent});
                    break :frames;
                }
                try w.print("{s}at {f}:{d}:{d} in {f}\n", .{ indent, text(loc.file_name), loc.line, loc.column, text(sym.name orelse "???") });
                lines += 1;
            }
        }
    }
    if (lines == 0) try w.print("{s}{s}\n", .{ indent, unknown_location });
}

/// `bytes`, a string the SDK didn't write (a name from workflow code, the server, or debug
/// info), as `__stack_trace` answer text: valid UTF-8 kept as is; each byte that isn't part
/// of a valid UTF-8 sequence written as U+FFFD; and control characters, `"` and `\`
/// escaped as in Zig string literals (`\n`, `\"`, `\x01`). So the answer is always valid
/// UTF-8, which the JSON converter encodes as a string, and a name can't start a line or
/// end a quote of its own.
pub fn text(bytes: []const u8) Text {
    return .{ .bytes = bytes };
}

pub const Text = struct {
    bytes: []const u8,

    pub fn format(t: Text, w: *std.Io.Writer) std.Io.Writer.Error!void {
        const bytes = t.bytes;
        var i: usize = 0;
        while (i < bytes.len) {
            const b = bytes[i];
            if (b < 0x80) {
                switch (b) {
                    '\n' => try w.writeAll("\\n"),
                    '\r' => try w.writeAll("\\r"),
                    '\t' => try w.writeAll("\\t"),
                    '"' => try w.writeAll("\\\""),
                    '\\' => try w.writeAll("\\\\"),
                    0...8, 11, 12, 14...0x1f, 0x7f => try w.print("\\x{x:0>2}", .{b}),
                    else => try w.writeByte(b),
                }
                i += 1;
                continue;
            }
            const len = std.unicode.utf8ByteSequenceLength(b) catch 0;
            const valid = len > 0 and i + len <= bytes.len and
                if (std.unicode.utf8Decode(bytes[i..][0..len])) |_| true else |_| false;
            if (!valid) {
                try w.writeAll(&std.unicode.replacement_character_utf8);
                i += 1;
                continue;
            }
            try w.writeAll(bytes[i..][0..len]);
            i += len;
        }
    }
};

test "text keeps valid UTF-8, replaces invalid bytes, and escapes what could change the answer's lines" {
    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.print("{f}", .{text("订单 ok\n\"q\"\\\x01\x7f" ++ "\xe8\xae" ++ "x\xff\xc0\xaf\xed\xa0\x80")});
    const out = w.buffered();
    try std.testing.expectEqualStrings("订单 ok\\n\\\"q\\\"\\\\\\x01\\x7f\u{fffd}\u{fffd}x\u{fffd}\u{fffd}\u{fffd}\u{fffd}\u{fffd}\u{fffd}", out);
    try std.testing.expect(std.unicode.utf8ValidateSlice(out));
}
