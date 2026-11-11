//! Runs the samples under samples/ and reports which pass. `zig build samples-test` builds the
//! samples and runs this with the build root and the sample names.
//!
//! A sample counts as ported once it has a samples/<name>/sample.zon manifest describing how to
//! validate it. Until the full runner exists (beads: "Samples runner: zig build samples-test
//! against a dev server"), this reports each sample's status and fails if a listed sample
//! directory is missing. Ported samples are not executed yet.
//! Usage: samples_runner <build-root> <sample-name>...

const std = @import("std");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const root_path = args.next() orelse return error.MissingBuildRoot;
    const root = try std.Io.Dir.cwd().openDir(io, root_path, .{});

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buffer);
    const out = &stdout.interface;

    var total: usize = 0;
    var ported: usize = 0;
    var missing: usize = 0;
    try out.print("{s:<40} status\n", .{"sample"});
    while (args.next()) |name| {
        total += 1;
        var path_buf: [256]u8 = undefined;
        const dir_path = try std.fmt.bufPrint(&path_buf, "samples/{s}", .{name});
        root.access(io, dir_path, .{}) catch {
            missing += 1;
            try out.print("{s:<40} MISSING (listed in build.zig, no directory)\n", .{name});
            continue;
        };
        var manifest_buf: [256]u8 = undefined;
        const manifest = try std.fmt.bufPrint(&manifest_buf, "samples/{s}/sample.zon", .{name});
        if (root.access(io, manifest, .{})) |_| {
            ported += 1;
            try out.print("{s:<40} ported (not executed: runner pending)\n", .{name});
        } else |_| {
            try out.print("{s:<40} placeholder\n", .{name});
        }
    }
    try out.print("\n{d} samples listed, {d} ported, {d} missing\n", .{ total, ported, missing });
    try out.flush();
    if (missing > 0) std.process.exit(1);
}
