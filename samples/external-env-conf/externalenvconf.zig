//! External client configuration: the worker and the starter read their connection settings
//! from a TOML config file and TEMPORAL_* environment variables instead of hardcoding them,
//! and share `loadProfile` so both connect the same way. Environment variables override the
//! file's values. The workflow is a hello world that calls one activity.

const std = @import("std");
const temporal = @import("temporal");
const workflow = temporal.workflow;
const activity = temporal.activity;
const client = temporal.client;

const Allocator = std.mem.Allocator;
const Environ = std.process.Environ;

/// The config file, built into the program so it's found whatever the working directory.
/// `client.LoadOptions.config_file` reads one from disk instead.
pub const config_file = "config.toml";
const config_data = @embedFile(config_file);

/// The options the worker and starter connect with. The other loaders below show the other
/// ways to get them; switch to one of them here.
pub fn loadProfile(arena: Allocator, env: *const Environ.Map) client.LoadError!client.Options {
    return loadFromCustomFile(arena, env);
}

/// The default profile (or the one TEMPORAL_PROFILE names) of config.toml.
pub fn loadFromCustomFile(arena: Allocator, env: *const Environ.Map) client.LoadError!client.Options {
    const options = try client.loadOptions(arena, .{ .config_data = config_data, .env = env });
    std.log.info("✅ Connecting using custom config: {s}", .{config_file});
    return options;
}

/// The default profile of the default config file (TEMPORAL_CONFIG_FILE, or
/// temporal/temporal.toml in the user's config directory). Without either, only the
/// environment applies, and its defaults connect to localhost:7233 in namespace "default".
pub fn loadDefaultProfile(arena: Allocator, env: *const Environ.Map) client.LoadError!client.Options {
    std.log.info("✅ Connecting to Temporal with default client options", .{});
    return client.loadOptions(arena, .{ .env = env });
}

/// The "prod" profile of config.toml. Its certificate paths and API key are placeholders, so
/// connecting with these options fails, but loading them shows how to pick a profile.
pub fn loadSpecificProfile(arena: Allocator, env: *const Environ.Map) client.LoadError!client.Options {
    const profile = "prod";
    const options = try client.loadOptions(arena, .{ .profile = profile, .config_data = config_data, .env = env });
    std.log.info("✅ Connecting to Temporal using '{s}' profile", .{profile});
    return options;
}

/// Loaded options are a plain struct: change fields after loading to override them. The
/// namespace "test-namespace" must exist to connect with these.
pub fn overrideAfterLoading(arena: Allocator, env: *const Environ.Map) client.LoadError!client.Options {
    var options = try client.loadOptions(arena, .{ .env = env });
    options.host_port = "localhost:7233";
    options.namespace = "test-namespace";
    std.log.info("✅ Connecting with overridden config to: {s} in namespace: {s}", .{ options.host_port, options.namespace });
    return options;
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
