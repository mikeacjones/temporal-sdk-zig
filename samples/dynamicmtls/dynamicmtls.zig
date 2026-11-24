//! Mutual TLS with certificate rotation: the worker presents a client certificate to a server
//! that requires one and, while it runs, watches the certificate and key files. When either
//! file's contents change and both can be read, it connects again with them, without
//! restarting; running workers move to the new connection. The workflow and activity are
//! the helloworld sample's.
//! The connection settings come from flags or, for a flag that is absent, its environment
//! variable; `parseClientOptions` sets each option it reads, and only those.

const std = @import("std");
const builtin = @import("builtin");
const temporal = @import("temporal");
const client = temporal.client;

const Allocator = std.mem.Allocator;
const Io = std.Io;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const task_queue = "dynamic-mtls";
pub const workflow_id = "dynamic-mtls";

pub const ArgsError = error{ InvalidArguments, OutOfMemory };

const Setting = struct {
    flag: []const u8,
    /// The environment variable read when the flag is absent; empty for none.
    variable: []const u8,
    default: []const u8 = "",
};

const settings = [_]Setting{
    .{ .flag = "--target-host", .variable = "TEMPORAL_ADDRESS", .default = "localhost:7233" },
    .{ .flag = "--namespace", .variable = "TEMPORAL_NAMESPACE", .default = "default" },
    // The client's certificate and private key, which the server checks against its CA. The
    // worker reads them again whenever their contents change.
    .{ .flag = "--client-cert", .variable = "TEMPORAL_TLS_CLIENT_CERT_PATH" },
    .{ .flag = "--client-key", .variable = "TEMPORAL_TLS_CLIENT_KEY_PATH" },
    // The server's CA certificate, for a server whose certificate a private CA signed; empty
    // trusts the system roots.
    .{ .flag = "--server-root-ca-cert", .variable = "TEMPORAL_TLS_SERVER_CA_CERT_PATH" },
    // The name to check the server's certificate against, when it isn't the target host's.
    .{ .flag = "--server-name", .variable = "TEMPORAL_TLS_SERVER_NAME" },
    // How often, in seconds, the worker checks the certificate files for a change.
    .{ .flag = "--reload-interval", .variable = "", .default = "30" },
};

fn settingIndex(comptime flag: []const u8) usize {
    inline for (settings, 0..) |s, i| if (comptime std.mem.eql(u8, s.flag, flag)) return i;
    @compileError("no setting " ++ flag);
}

// Test hook, read only when builtin.is_test: when set, `parseClientOptions` and
// `reloadInterval` write their error line here, with a newline, instead of logging it (the
// test runner fails a test that logs an error).
pub var test_error_log: ?*std.Io.Writer = null;

fn logError(comptime format: []const u8, args: anytype) void {
    if (builtin.is_test) if (test_error_log) |w| {
        w.print(format ++ "\n", args) catch {};
        return;
    };
    std.log.err(format, args);
}

/// Each setting's value from `args`, else its environment variable, else its default.
fn readSettings(args: []const []const u8, env: ?*const std.process.Environ.Map) error{InvalidArguments}![settings.len][]const u8 {
    var values: [settings.len]?[]const u8 = @splat(null);
    var i: usize = 0;
    next: while (i < args.len) : (i += 2) {
        inline for (settings, 0..) |s, n| if (std.mem.eql(u8, args[i], s.flag)) {
            if (i + 1 >= args.len) {
                if (s.variable.len > 0)
                    logError("flag {s} needs a value; leave it out to use {s}", .{ s.flag, s.variable })
                else
                    logError("flag {s} needs a value; leave it out to use {s}", .{ s.flag, s.default });
                return error.InvalidArguments;
            }
            values[n] = args[i + 1];
            continue :next;
        };
        logError("unknown flag {s}; the flags are --target-host, --namespace, --client-cert, --client-key, --server-root-ca-cert, --server-name and --reload-interval", .{args[i]});
        return error.InvalidArguments;
    }
    var result: [settings.len][]const u8 = undefined;
    inline for (settings, values, &result) |s, v, *r| {
        r.* = v orelse blk: {
            if (env) |e| if (s.variable.len > 0) if (e.get(s.variable)) |value| if (value.len > 0) break :blk value;
            break :blk s.default;
        };
    }
    return result;
}

fn parseInterval(value: []const u8) error{InvalidArguments}!Io.Duration {
    const seconds = std.fmt.parseInt(u32, value, 10) catch 0;
    if (seconds == 0) {
        logError("--reload-interval must be a positive whole number of seconds, not \"{s}\"", .{value});
        return error.InvalidArguments;
    }
    return .fromSeconds(seconds);
}

/// `args` are the program's arguments after its name. A flag is `--name value`. A flag that
/// is absent takes its environment variable from `env`, then its default. The result always
/// has TLS on with a client certificate and key, and never an API key. The certificate files
/// are named, not read: `Client.connect` reads them. `--reload-interval` is checked here and
/// read by `reloadInterval`.
pub fn parseClientOptions(arena: Allocator, args: []const []const u8, env: *const std.process.Environ.Map) ArgsError!client.Options {
    _ = arena; // Every string is borrowed from `args` or `env`.
    const values = try readSettings(args, env);
    inline for (.{ "--client-cert", "--client-key" }) |flag| {
        const s = settings[settingIndex(flag)];
        if (values[settingIndex(flag)].len == 0) {
            logError("{s} is required: pass {s} or set {s}", .{ flag, flag, s.variable });
            return error.InvalidArguments;
        }
    }
    _ = try parseInterval(values[settingIndex("--reload-interval")]);
    return .{
        .host_port = values[settingIndex("--target-host")],
        .namespace = values[settingIndex("--namespace")],
        .tls = .{
            .server_root_ca_cert_path = values[settingIndex("--server-root-ca-cert")],
            .server_name = values[settingIndex("--server-name")],
            .client_cert_path = values[settingIndex("--client-cert")],
            .client_private_key_path = values[settingIndex("--client-key")],
        },
    };
}

/// The `--reload-interval` in `args` (30 s when absent). It has no environment variable.
pub fn reloadInterval(args: []const []const u8) error{InvalidArguments}!Io.Duration {
    const values = try readSettings(args, null);
    return parseInterval(values[settingIndex("--reload-interval")]);
}

/// The program's arguments after its name.
pub fn programArgs(arena: Allocator, init: std.process.Init) error{OutOfMemory}![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.skip();
    while (it.next()) |a| try list.append(arena, a);
    return list.items;
}

/// Watches a client certificate and key and connects the client again when either file's
/// contents change, once both files can be read. It compares contents, not modification
/// times, so a file rewritten with the same bytes is no change.
pub const CertificateWatcher = struct {
    gpa: Allocator,
    io: Io,
    cert_path: []const u8, // borrowed
    key_path: []const u8, // borrowed
    /// SHA-256 of each file as last accepted; null for a file that could not be read.
    seen: Digests,

    pub const Digests = [2]?[Sha256.digest_length]u8;

    /// Certificate and key files are a few kilobytes; a larger file is not read.
    const max_file_size = 1 << 20;

    /// Records the files' current contents as the ones the client connected with.
    pub fn init(gpa: Allocator, io: Io, cert_path: []const u8, key_path: []const u8) CertificateWatcher {
        var w: CertificateWatcher = .{ .gpa = gpa, .io = io, .cert_path = cert_path, .key_path = key_path, .seen = undefined };
        w.seen = w.read();
        return w;
    }

    /// The files' digests now, when they differ from `seen` and both files can be read; else
    /// null. A file that cannot be read, such as one being replaced, is not acted on.
    pub fn changed(w: *const CertificateWatcher) ?Digests {
        const now = w.read();
        for (now) |d| if (d == null) return null;
        if (std.meta.eql(now, w.seen)) return null;
        return now;
    }

    /// Until canceled: sleeps `interval`, and when `changed` returns digests, calls
    /// `c.reconnect(options)`; on success sets `seen` and logs info "Reloaded the client
    /// certificate"; on failure logs a warning and leaves `seen`, so the next round retries.
    /// The client keeps its current connection when the reconnect fails.
    pub fn run(w: *CertificateWatcher, c: *client.Client, options: client.ConnectionOptions, interval: Io.Duration) Io.Cancelable!void {
        while (true) {
            try w.io.sleep(interval, .awake);
            const now = w.changed() orelse continue;
            c.reconnect(options) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    std.log.warn("Unable to reload the client certificate from {s} and {s}, will retry in the next round: {t}", .{ w.cert_path, w.key_path, err });
                    continue;
                },
            };
            w.seen = now;
            std.log.info("Reloaded the client certificate", .{});
        }
    }

    fn read(w: *const CertificateWatcher) Digests {
        return .{ w.digest(w.cert_path), w.digest(w.key_path) };
    }

    fn digest(w: *const CertificateWatcher, path: []const u8) ?[Sha256.digest_length]u8 {
        const bytes = Io.Dir.cwd().readFileAlloc(w.io, path, w.gpa, .limited(max_file_size)) catch |err| switch (err) {
            error.Canceled => {
                // Unreadable for this round; the caller's next sleep sees the cancelation.
                w.io.recancel();
                return null;
            },
            else => return null,
        };
        defer w.gpa.free(bytes);
        var out: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(bytes, &out, .{});
        return out;
    }
};
