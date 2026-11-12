//! A codec server: serves the encryption codecs over HTTP, so the Temporal CLI can show the
//! sample's payloads decrypted:
//!
//!     temporal workflow show --workflow-id encryption_workflowID --codec-endpoint 'http://localhost:8081/'
//!
//! Without `--codec-endpoint`, the CLI shows only the encrypted bytes. Browsers are not
//! allowed to call this server (it sends no CORS headers), so the Web UI can't use it.
//!
//! Usage: codec_server [--port N]. The port defaults to 8081.

const std = @import("std");
const temporal = @import("temporal");
const codec = @import("codec.zig");
const Io = std.Io;

const Server = struct {
    gpa: std.mem.Allocator,
    io: Io,
    handler: temporal.converter.CodecHandler,

    /// Serves HTTP on `listener` until canceled, handling each connection in its own task.
    fn serve(s: *const Server, listener: *Io.net.Server) Io.net.Server.AcceptError!void {
        var group: Io.Group = .init;
        defer group.cancel(s.io);
        while (true) {
            const stream = try listener.accept(s.io);
            // Without a task of its own, a kept-alive connection would hold up the others.
            group.concurrent(s.io, serveConnection, .{ s, stream }) catch try serveConnection(s, stream);
        }
    }

    fn serveConnection(s: *const Server, stream: Io.net.Stream) Io.Cancelable!void {
        defer stream.close(s.io);
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [4096]u8 = undefined;
        var conn_reader = stream.reader(s.io, &recv_buffer);
        var conn_writer = stream.writer(s.io, &send_buffer);
        var server = std.http.Server.init(&conn_reader.interface, &conn_writer.interface);
        while (server.reader.state == .ready) {
            var request = server.receiveHead() catch |err| switch (err) {
                error.HttpConnectionClosing => return,
                else => return std.log.warn("closing a codec server connection: {t}", .{err}),
            };
            var arena: std.heap.ArenaAllocator = .init(s.gpa);
            defer arena.deinit();
            s.handler.handle(arena.allocator(), s.io, &request) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return std.log.warn("unable to answer a codec server request: {t}", .{err}),
            };
        }
    }
};

pub fn main(init: std.process.Init) !void {
    var port: u16 = 8081;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    while (args.next()) |arg| {
        if (!std.mem.eql(u8, arg, "--port")) {
            std.log.err("Unknown argument {s}; usage: codec_server [--port N]", .{arg});
            std.process.exit(1);
        }
        const value = args.next() orelse "";
        port = std.fmt.parseInt(u16, value, 10) catch {
            std.log.err("Invalid --port value \"{s}\": expected a port number from 0 to 65535", .{value});
            std.process.exit(1);
        };
    }

    const server: Server = .{ .gpa = init.gpa, .io = init.io, .handler = temporal.converter.codecHandler(&codec.codecs, .{}) };
    const address = try Io.net.IpAddress.parse("0.0.0.0", port);
    var listener = address.listen(init.io, .{ .reuse_address = true }) catch |err| {
        std.log.err("Unable to listen on 0.0.0.0:{d}: {t}", .{ port, err });
        std.process.exit(1);
    };
    defer listener.deinit(init.io);
    std.log.info("Codec server listening on http://localhost:{d}/", .{port});
    server.serve(&listener) catch |err| {
        std.log.err("Codec server stopped serving: {t}", .{err});
        std.process.exit(1);
    };
}
