//! A symbolizer for the Thread Sanitizer check cases, so that they do not depend on the
//! host's: on macOS the runtime otherwise starts `/usr/bin/atos -p <pid>`, which reads the
//! live process through task_for_pid and, when the host's atos is wedged, never answers.
//!
//! build.zig installs it under the file name `llvm-symbolizer` and passes that path in
//! `TSAN_OPTIONS=external_symbolizer_path=...`. The runtime picks the protocol from the
//! file name (`ChooseExternalSymbolizer` in sanitizer_symbolizer_posix_libcdep.cpp), and
//! the llvm-symbolizer protocol names a file, not a process. It reads requests from stdin,
//! one per line, and ignores its arguments (`--demangle --inlines --default-arch=...`):
//!
//!   CODE "<module>[:<arch>]" 0x<offset>
//!     The function at <offset>, a virtual address in the module's file as its load
//!     commands or program headers give it, answered as
//!     `<function>\n<file>:<line>:<column>\n\n`, with `??` for what is not known.
//!   DATA "<module>[:<arch>]" 0x<offset>
//!     The symbol at <offset>, answered as `<name>\n<offset> 0\n\n`: the size is not
//!     known, which the runtime takes as "not printed".
//!
//! Any other line is answered `??\n??:0:0\n\n`, which every request reads as unknown. Names
//! and locations come from the module's debug info through `std.debug` (Mach-O and its
//! object files or dSYM, or ELF), and from its symbol table where debug info is missing.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const native_endian = builtin.cpu.arch.endian();

pub fn main(init: std.process.Init) !void {
    var symbolizer: Symbolizer = .init(init.gpa, init.io);
    defer symbolizer.deinit();
    // A request carries a path of up to PATH_MAX (4096) bytes.
    var in_buffer: [16 * 1024]u8 = undefined;
    var stdin = Io.File.stdin().readerStreaming(init.io, &in_buffer);
    var out_buffer: [4096]u8 = undefined;
    var stdout = Io.File.stdout().writerStreaming(init.io, &out_buffer);
    while (try stdin.interface.takeDelimiter('\n')) |line| {
        try symbolizer.answer(line, &stdout.interface);
        try stdout.interface.flush();
    }
}

pub const Request = struct {
    kind: enum { code, data },
    module: []const u8,
    arch: ?std.Target.Cpu.Arch,
    offset: u64,

    /// Parses `CODE "<module>[:<arch>]" 0x<offset>` or the same with `DATA`, or returns
    /// null. The arch is null when the module has no `:<arch>` suffix, and also when the
    /// suffix names an arch that `std.debug` cannot read.
    pub fn parse(line: []const u8) ?Request {
        const text = std.mem.trimEnd(u8, line, "\r");
        const space = std.mem.indexOfScalar(u8, text, ' ') orelse return null;
        const kind: @FieldType(Request, "kind") = if (std.mem.eql(u8, text[0..space], "CODE"))
            .code
        else if (std.mem.eql(u8, text[0..space], "DATA"))
            .data
        else
            return null;
        const rest = text[space + 1 ..];
        if (rest.len < 2 or rest[0] != '"') return null;
        const close = std.mem.lastIndexOfScalar(u8, rest, '"') orelse return null;
        if (close == 0) return null;
        const quoted = rest[1..close];
        const number = std.mem.trim(u8, rest[close + 1 ..], " ");
        if (!std.mem.startsWith(u8, number, "0x")) return null;
        const offset = std.fmt.parseInt(u64, number[2..], 16) catch return null;
        var module = quoted;
        var arch: ?std.Target.Cpu.Arch = null;
        if (std.mem.lastIndexOfScalar(u8, quoted, ':')) |colon| {
            if (archNamed(quoted[colon + 1 ..])) |known| {
                module = quoted[0..colon];
                arch = known.arch;
            }
        }
        if (module.len == 0) return null;
        return .{ .kind = kind, .module = module, .arch = arch, .offset = offset };
    }
};

/// The arch names the runtime appends to a module (`ModuleArchToString`), with the arch
/// `std.debug` reads them as, or null for one it cannot read. Null for any other text.
fn archNamed(name: []const u8) ?struct { arch: ?std.Target.Cpu.Arch } {
    const names = [_]struct { []const u8, ?std.Target.Cpu.Arch }{
        .{ "x86_64", .x86_64 }, .{ "x86_64h", .x86_64 }, .{ "arm64", .aarch64 }, .{ "arm64e", .aarch64 },
        .{ "i386", null },      .{ "armv6", null },      .{ "armv7", null },     .{ "armv7s", null },
        .{ "armv7k", null },    .{ "riscv64", null },
    };
    for (names) |entry| if (std.mem.eql(u8, name, entry[0])) return .{ .arch = entry[1] };
    return null;
}

/// What is known of one address.
pub const Location = struct {
    name: ?[]const u8 = null,
    file: ?[]const u8 = null,
    line: u64 = 0,
    column: u64 = 0,
};

pub const Symbolizer = struct {
    gpa: Allocator,
    io: Io,
    /// Each module read so far, by path (owned), or null when it could not be read.
    modules: std.StringArrayHashMapUnmanaged(?*Module) = .empty,
    /// The text of one answer (source file names); reset before each request.
    scratch: std.heap.ArenaAllocator,

    const Module = switch (builtin.object_format) {
        .macho => std.debug.MachOFile,
        .elf => struct {
            file: std.debug.ElfFile,
            /// Whether `file.dwarf` was opened; false also when it has none or is invalid.
            dwarf_open: bool,
        },
        else => void,
    };

    pub fn init(gpa: Allocator, io: Io) Symbolizer {
        return .{ .gpa = gpa, .io = io, .scratch = .init(gpa) };
    }

    pub fn deinit(s: *Symbolizer) void {
        for (s.modules.keys(), s.modules.values()) |path, maybe| {
            s.gpa.free(path);
            const m = maybe orelse continue;
            switch (builtin.object_format) {
                .macho => m.deinit(s.gpa),
                .elf => m.file.deinit(s.gpa),
                else => {},
            }
            s.gpa.destroy(m);
        }
        s.modules.deinit(s.gpa);
        s.scratch.deinit();
    }

    /// Writes the answer to one request line.
    pub fn answer(s: *Symbolizer, line: []const u8, w: *Io.Writer) !void {
        _ = s.scratch.reset(.retain_capacity);
        const request = Request.parse(line) orelse return w.writeAll("??\n??:0:0\n\n");
        const found = try s.lookup(request.module, request.arch, request.offset);
        switch (request.kind) {
            .code => try w.print("{s}\n{s}:{d}:{d}\n\n", .{
                found.name orelse "??",
                found.file orelse "??",
                found.line,
                found.column,
            }),
            .data => if (found.name) |name|
                try w.print("{s}\n{d} 0\n\n", .{ name, request.offset })
            else
                try w.writeAll("??\n0 0\n\n"),
        }
    }

    /// What the module's debug info or symbol table says of `vaddr`. A module that cannot
    /// be read gives an empty `Location`, as does an arch `std.debug` cannot read.
    pub fn lookup(s: *Symbolizer, path: []const u8, arch: ?std.Target.Cpu.Arch, vaddr: u64) error{OutOfMemory}!Location {
        const m = (try s.moduleAt(path, arch orelse builtin.cpu.arch)) orelse return .{};
        const text = s.scratch.allocator();
        return switch (builtin.object_format) {
            .macho => lookupMachO(s.gpa, s.io, text, m, vaddr),
            .elf => lookupElf(s.gpa, text, m, vaddr),
            else => .{},
        };
    }

    fn moduleAt(s: *Symbolizer, path: []const u8, arch: std.Target.Cpu.Arch) error{OutOfMemory}!?*Module {
        if (s.modules.get(path)) |known| return known;
        const key = try s.gpa.dupe(u8, path);
        errdefer s.gpa.free(key);
        try s.modules.ensureUnusedCapacity(s.gpa, 1);
        const loaded = try s.load(path, arch);
        s.modules.putAssumeCapacityNoClobber(key, loaded);
        return loaded;
    }

    fn load(s: *Symbolizer, path: []const u8, arch: std.Target.Cpu.Arch) error{OutOfMemory}!?*Module {
        switch (builtin.object_format) {
            .macho => {
                // MachOFile.load accepts only these.
                if (arch != .x86_64 and arch != .aarch64) return null;
                const file = std.debug.MachOFile.load(s.gpa, s.io, path, arch) catch |err| switch (err) {
                    error.OutOfMemory => |e| return e,
                    else => return null,
                };
                const m = s.gpa.create(Module) catch |err| {
                    var f = file;
                    f.deinit(s.gpa);
                    return err;
                };
                m.* = file;
                return m;
            },
            .elf => {
                const handle = Io.Dir.cwd().openFile(s.io, path, .{}) catch return null;
                defer handle.close(s.io);
                var file = std.debug.ElfFile.load(s.gpa, s.io, handle, null, &.none) catch |err| switch (err) {
                    error.OutOfMemory => |e| return e,
                    else => return null,
                };
                const m = s.gpa.create(Module) catch |err| {
                    file.deinit(s.gpa);
                    return err;
                };
                m.* = .{ .file = file, .dwarf_open = false };
                if (m.file.dwarf) |*dwarf| {
                    if (dwarf.open(s.gpa, m.file.endian)) {
                        m.dwarf_open = true;
                    } else |err| switch (err) {
                        error.OutOfMemory => {
                            m.file.deinit(s.gpa);
                            s.gpa.destroy(m);
                            return error.OutOfMemory;
                        },
                        else => {},
                    }
                }
                return m;
            },
            else => return null,
        }
    }
};

fn lookupMachO(gpa: Allocator, io: Io, text: Allocator, file: *std.debug.MachOFile, vaddr: u64) error{OutOfMemory}!Location {
    // The symbol table's name, without the leading underscore Mach-O gives C symbols.
    const table_name: ?[]const u8 = if (file.lookupSymbolName(vaddr)) |name|
        (if (std.mem.startsWith(u8, name, "_")) name[1..] else name)
    else |_|
        null;
    const dwarf, const dwarf_vaddr = file.getDwarfForAddress(gpa, io, vaddr) catch |err| switch (err) {
        error.OutOfMemory => |e| return e,
        else => return .{ .name = table_name },
    };
    return fromDwarf(gpa, text, dwarf, .little, dwarf_vaddr, table_name);
}

fn lookupElf(gpa: Allocator, text: Allocator, module: *Symbolizer.Module, vaddr: u64) error{OutOfMemory}!Location {
    const table_name: ?[]const u8 = if (module.file.searchSymtab(gpa, vaddr)) |symbol|
        symbol.name
    else |err| switch (err) {
        error.OutOfMemory => |e| return e,
        else => null,
    };
    if (!module.dwarf_open) return .{ .name = table_name };
    return fromDwarf(gpa, text, &module.file.dwarf.?, module.file.endian, vaddr, table_name);
}

/// The symbol table's name wins: it is the linkage name, such as `race.write`, which
/// llvm-symbolizer prints by default and suppressions match, while the debug info's
/// `DW_AT_name` is the bare `write`.
fn fromDwarf(gpa: Allocator, text: Allocator, dwarf: *std.debug.Dwarf, endian: std.builtin.Endian, vaddr: u64, table_name: ?[]const u8) error{OutOfMemory}!Location {
    const name = table_name orelse dwarf.getSymbolName(vaddr);
    const unit = dwarf.findCompileUnit(endian, vaddr) catch return .{ .name = name };
    const source = dwarf.getLineNumberInfo(gpa, text, endian, unit, vaddr) catch |err| switch (err) {
        error.OutOfMemory => |e| return e,
        else => return .{ .name = name },
    };
    return .{ .name = name, .file = source.file_name, .line = source.line, .column = source.column };
}

const testing = std.testing;

test "a request names its kind, module, arch and offset" {
    const code = Request.parse("CODE \"/tmp/a b/race:arm64\" 0x100165be8").?;
    try testing.expectEqual(.code, code.kind);
    try testing.expectEqualStrings("/tmp/a b/race", code.module);
    try testing.expectEqual(@as(?std.Target.Cpu.Arch, .aarch64), code.arch);
    try testing.expectEqual(@as(u64, 0x100165be8), code.offset);

    const data = Request.parse("DATA \"/usr/lib/dyld:arm64e\" 0x31d78\r").?;
    try testing.expectEqual(.data, data.kind);
    try testing.expectEqualStrings("/usr/lib/dyld", data.module);
    try testing.expectEqual(@as(u64, 0x31d78), data.offset);

    // No arch, or a colon that is part of the path.
    const plain = Request.parse("CODE \"/opt/x:y/lib.so\" 0x10").?;
    try testing.expectEqualStrings("/opt/x:y/lib.so", plain.module);
    try testing.expectEqual(@as(?std.Target.Cpu.Arch, null), plain.arch);

    // An arch std.debug cannot read is dropped from the module, and leaves no arch.
    const other = Request.parse("CODE \"/a/b:i386\" 0x10").?;
    try testing.expectEqualStrings("/a/b", other.module);
    try testing.expectEqual(@as(?std.Target.Cpu.Arch, null), other.arch);

    for ([_][]const u8{
        "",
        "FRAME \"/a/b\" 0x10",
        "CODE /a/b 0x10",
        "CODE \"/a/b\" 10",
        "CODE \"/a/b\" 0xzz",
        "CODE \"\" 0x10",
        "CODE \"",
    }) |line| try testing.expectEqual(@as(?Request, null), Request.parse(line));
}

test "an address this test binary holds resolves to its function's name" {
    if (builtin.object_format != .macho and builtin.object_format != .elf) return error.SkipZigTest;
    const io = testing.io;
    const path = try std.process.executablePathAlloc(io, testing.allocator);
    defer testing.allocator.free(path);
    const address = @intFromPtr(&knownFunction);
    const self_info = try std.debug.getSelfDebugInfo();
    const vaddr = address - try self_info.getModuleSlide(io, address);

    var symbolizer: Symbolizer = .init(testing.allocator, io);
    defer symbolizer.deinit();
    var line_buffer: [4096 + 64]u8 = undefined;
    const line = try std.fmt.bufPrint(&line_buffer, "CODE \"{s}\" 0x{x}", .{ path, vaddr });
    var out: Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try symbolizer.answer(line, &out.writer);
    // Twice, so the second answer comes from the module already read.
    try symbolizer.answer(line, &out.writer);

    var answers = std.mem.splitSequence(u8, out.written(), "\n\n");
    for (0..2) |_| {
        const got = answers.next().?;
        if (!std.mem.startsWith(u8, got, "symbolizer.knownFunction\n")) {
            std.debug.print("answer: {s}\n", .{got});
            return error.TestUnexpectedResult;
        }
        // The source line is the function's, in this file.
        const file_line = got["symbolizer.knownFunction\n".len..];
        try testing.expect(std.mem.indexOf(u8, file_line, "symbolizer.zig:") != null);
        try testing.expect(!std.mem.endsWith(u8, file_line, ":0:0"));
    }
    try testing.expectEqualStrings("", answers.rest());
}

noinline fn knownFunction(x: u32) u32 {
    return x *% 2654435761 +% 1;
}

test "a module that cannot be read, or an unknown request, is answered as unknown" {
    var symbolizer: Symbolizer = .init(testing.allocator, testing.io);
    defer symbolizer.deinit();
    var out: Io.Writer.Allocating = .init(testing.allocator);
    defer out.deinit();
    try symbolizer.answer("CODE \"/nonexistent/symbolizer-test/module:arm64\" 0x1000", &out.writer);
    try symbolizer.answer("DATA \"/nonexistent/symbolizer-test/module\" 0x1000", &out.writer);
    try symbolizer.answer("FRAME \"/nonexistent/symbolizer-test/module\" 0x1000", &out.writer);
    try symbolizer.answer("garbage", &out.writer);
    try testing.expectEqualStrings("??\n??:0:0\n\n??\n0 0\n\n??\n??:0:0\n\n??\n??:0:0\n\n", out.written());
    _ = knownFunction(1);
}
