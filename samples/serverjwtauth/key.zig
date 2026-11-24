//! Makes and serves the sample's signing key, for running the sample by hand against a server
//! of your own.
//!
//!     serverjwtauth-key gen | serve | gen-and-serve | cli-system-token
//!
//! - `gen` writes a new ES256 private key, as a JWK, to SAMPLE_JWT_KEY, else `key.priv.json`.
//! - `serve` serves that key's public half at `GET /jwks.json` on every address, on a port the
//!   system picks, which it logs.
//! - `gen-and-serve` does both.
//! - `cli-system-token` prints `TEMPORAL_API_KEY=<token>`: a token with the
//!   "temporal-system:admin" permission, valid for one hour. The `temporal` CLI sends
//!   TEMPORAL_API_KEY as `authorization: Bearer <token>`.
//!
//! Run the server with JWT authorization that reads the logged endpoint, in its config file:
//!
//!     global:
//!       authorization:
//!         jwtKeyProvider:
//!           keySourceURIs:
//!             - http://<address>/jwks.json
//!           refreshInterval: 1m
//!         permissionsClaimName: permissions
//!         authorizer: default
//!         claimMapper: default
//!
//! A server reads the keys when it starts, so start `serve` first. The worker's and starter's
//! tokens may use only an existing namespace. Register it with the system token:
//!
//!     export $(serverjwtauth-key cli-system-token)
//!     temporal operator namespace create --namespace default
//!
//! then run the worker and the starter with TEMPORAL_ADDRESS and the server's TLS settings
//! (TEMPORAL_TLS_SERVER_CA_CERT_PATH and the others `client.loadOptions` reads).

const std = @import("std");
const serverjwtauth = @import("serverjwtauth.zig");
const Io = std.Io;
const http = std.http;

const usage = "usage: serverjwtauth-key gen | serve | gen-and-serve | cli-system-token";

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const command = args.next();
    if (command == null or args.next() != null) {
        std.log.err("expected one argument; {s}", .{usage});
        std.process.exit(1);
    }
    const path = serverjwtauth.keyPath(init.environ_map);
    run(init, command.?, path) catch |err| {
        std.log.err("serverjwtauth-key {s} failed: {t}", .{ command.?, err });
        std.process.exit(1);
    };
}

fn run(init: std.process.Init, command: []const u8, path: []const u8) !void {
    const io = init.io;
    const arena = init.arena.allocator();

    if (std.mem.eql(u8, command, "gen")) {
        try generate(arena, io, path);
    } else if (std.mem.eql(u8, command, "serve")) {
        try serve(init, path);
    } else if (std.mem.eql(u8, command, "gen-and-serve")) {
        try generate(arena, io, path);
        try serve(init, path);
    } else if (std.mem.eql(u8, command, "cli-system-token")) {
        const key = serverjwtauth.readKey(arena, io, path) catch std.process.exit(1);
        const permissions: []const []const u8 = &.{"temporal-system:admin"};
        const config: serverjwtauth.JwtConfig = .{ .key = key, .permissions = permissions, .expiration = .fromSeconds(3600) };
        const token = config.mint(arena, io) catch |err| {
            std.log.err("Unable to mint a JWT: {t}", .{err});
            std.process.exit(1);
        };
        var buf: [1024]u8 = undefined;
        var stdout = Io.File.stdout().writer(io, &buf);
        try stdout.interface.print("TEMPORAL_API_KEY={s}\n", .{token});
        try stdout.interface.flush();
    } else {
        std.log.err("unknown command \"{s}\"; {s}", .{ command, usage });
        std.process.exit(1);
    }
}

fn generate(arena: std.mem.Allocator, io: Io, path: []const u8) !void {
    const key = try serverjwtauth.generateKey(arena, io);
    serverjwtauth.writeKey(arena, io, path, key) catch |err| {
        std.log.err("Unable to write the key to {s}: {t}", .{ path, err });
        std.process.exit(1);
    };
    std.log.info("Wrote a new key to {s}, key ID {s}", .{ path, key.id });
}

fn serve(init: std.process.Init, path: []const u8) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    // readKey logs what is wrong with the key file.
    const key = serverjwtauth.readKey(arena, io, path) catch std.process.exit(1);
    const jwks = try serverjwtauth.publicJwks(arena, key);

    const address = try Io.net.IpAddress.parse("0.0.0.0", 0);
    var listener = address.listen(io, .{ .reuse_address = true }) catch |err| {
        std.log.err("Unable to listen for the JWKS server: {t}", .{err});
        std.process.exit(1);
    };
    defer listener.deinit(io);
    std.log.info("Started JWKS server. Endpoint: http://{f}/jwks.json. Ctrl+C to exit.", .{listener.socket.address});

    var group: Io.Group = .init;
    defer group.cancel(io);
    while (true) {
        const stream = listener.accept(io) catch |err| {
            std.log.err("JWKS server stopped serving: {t}", .{err});
            std.process.exit(1);
        };
        // Without a task of its own, a kept-alive connection would hold up the others.
        group.concurrent(io, serveConnection, .{ io, stream, jwks }) catch try serveConnection(io, stream, jwks);
    }
}

fn serveConnection(io: Io, stream: Io.net.Stream, jwks: []const u8) Io.Cancelable!void {
    defer stream.close(io);
    var recv_buffer: [8192]u8 = undefined;
    var send_buffer: [4096]u8 = undefined;
    var conn_reader = stream.reader(io, &recv_buffer);
    var conn_writer = stream.writer(io, &send_buffer);
    var server = http.Server.init(&conn_reader.interface, &conn_writer.interface);
    while (server.reader.state == .ready) {
        var request = server.receiveHead() catch |err| switch (err) {
            error.HttpConnectionClosing => return,
            else => return std.log.warn("closing a JWKS server connection: {t}", .{err}),
        };
        const found = request.head.method == .GET and std.mem.eql(u8, request.head.target, "/jwks.json");
        const result = if (found)
            request.respond(jwks, .{ .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }} })
        else
            request.respond("not found: the keys are at GET /jwks.json", .{ .status = .not_found, .extra_headers = &.{.{ .name = "content-type", .value = "text/plain" }} });
        result catch |err| return std.log.warn("unable to answer a JWKS server request for {s}: {t}", .{ request.head.target, err });
    }
}
