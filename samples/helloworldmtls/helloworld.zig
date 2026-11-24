//! Hello world over mutual TLS: the worker and the starter present a client certificate to a
//! server that requires one, such as a Temporal Cloud namespace with mTLS, and run a workflow
//! that calls one activity.
//! The connection settings come from flags or, for a flag that is absent, its environment
//! variable; `parseClientOptions` sets each option it reads, and only those.

const std = @import("std");
const builtin = @import("builtin");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const client = temporal.client;

const Allocator = std.mem.Allocator;

pub const task_queue = "hello-world-mtls";
pub const workflow_id = "hello-world-mtls";

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
    // The client's certificate and private key, which the server checks against its CA.
    .{ .flag = "--client-cert", .variable = "TEMPORAL_TLS_CLIENT_CERT_PATH" },
    .{ .flag = "--client-key", .variable = "TEMPORAL_TLS_CLIENT_KEY_PATH" },
    // The server's CA certificate, for a server whose certificate a private CA signed; empty
    // trusts the system roots.
    .{ .flag = "--server-root-ca-cert", .variable = "TEMPORAL_TLS_SERVER_CA_CERT_PATH" },
    // The name to check the server's certificate against, when it isn't the target host's.
    .{ .flag = "--server-name", .variable = "TEMPORAL_TLS_SERVER_NAME" },
};

fn settingIndex(comptime flag: []const u8) usize {
    inline for (settings, 0..) |s, i| if (comptime std.mem.eql(u8, s.flag, flag)) return i;
    @compileError("no setting " ++ flag);
}

// Test hook, read only when builtin.is_test: when set, `parseClientOptions` writes its error
// line here, with a newline, instead of logging it (the test runner fails a test that logs an
// error).
pub var test_error_log: ?*std.Io.Writer = null;

fn logError(comptime format: []const u8, args: anytype) void {
    if (builtin.is_test) if (test_error_log) |w| {
        w.print(format ++ "\n", args) catch {};
        return;
    };
    std.log.err(format, args);
}

/// `args` are the program's arguments after its name. A flag is `--name value`. A flag that
/// is absent takes its environment variable from `env`, then its default. The result always
/// has TLS on with a client certificate and key, and never an API key. The certificate files
/// are named, not read: `Client.connect` reads them.
pub fn parseClientOptions(arena: Allocator, args: []const []const u8, env: *const std.process.Environ.Map) ArgsError!client.Options {
    _ = arena; // Every string is borrowed from `args` or `env`.
    var values: [settings.len]?[]const u8 = @splat(null);
    var i: usize = 0;
    next: while (i < args.len) : (i += 2) {
        inline for (settings, 0..) |s, n| if (std.mem.eql(u8, args[i], s.flag)) {
            if (i + 1 >= args.len) {
                logError("flag {s} needs a value; leave it out to use {s}", .{ s.flag, s.variable });
                return error.InvalidArguments;
            }
            values[n] = args[i + 1];
            continue :next;
        };
        logError("unknown flag {s}; the flags are --target-host, --namespace, --client-cert, --client-key, --server-root-ca-cert and --server-name", .{args[i]});
        return error.InvalidArguments;
    }
    inline for (settings, &values) |s, *v| {
        if (v.* == null) if (env.get(s.variable)) |e| if (e.len > 0) {
            v.* = e;
        };
        if (v.* == null) v.* = s.default;
    }

    inline for (.{ "--client-cert", "--client-key" }) |flag| {
        const s = settings[settingIndex(flag)];
        if (values[settingIndex(flag)].?.len == 0) {
            logError("{s} is required: pass {s} or set {s}", .{ flag, flag, s.variable });
            return error.InvalidArguments;
        }
    }
    return .{
        .host_port = values[settingIndex("--target-host")].?,
        .namespace = values[settingIndex("--namespace")].?,
        .tls = .{
            .server_root_ca_cert_path = values[settingIndex("--server-root-ca-cert")].?,
            .server_name = values[settingIndex("--server-name")].?,
            .client_cert_path = values[settingIndex("--client-cert")].?,
            .client_private_key_path = values[settingIndex("--client-key")].?,
        },
    };
}

/// The program's arguments after its name.
pub fn programArgs(arena: Allocator, init: std.process.Init) error{OutOfMemory}![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    var it = std.process.Args.Iterator.init(init.minimal.args);
    _ = it.skip();
    while (it.next()) |a| try list.append(arena, a);
    return list.items;
}

fn helloWorld(ctx: *workflow.Context, name: []const u8) ![]const u8 {
    const logger = ctx.logger();
    logger.info("HelloWorld workflow started", .{ .name = name });

    const greeting = ctx.executeActivity(Activity, .{ .start_to_close_timeout = .fromSeconds(10) }, .{name});
    const result = greeting.get() catch |err| {
        logger.err("Activity failed.", .{ .err = err });
        return err;
    };

    logger.info("HelloWorld workflow completed.", .{ .result = result });
    return result;
}

pub const Workflow = workflow.Def("Workflow", helloWorld);

fn greet(ctx: *activity.Context, name: []const u8) ![]const u8 {
    ctx.logger().info("Activity", .{ .name = name });
    return std.fmt.allocPrint(ctx.allocator(), "Hello {s}!", .{name});
}

pub const Activity = activity.Def("Activity", greet);
